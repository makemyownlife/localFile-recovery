package com.fcar.store.recovery.journal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * 数据文件操作类
 * Created by zhangyong on 2017/1/29.
 */
public class LocalFileAppender {

    private final static Logger logger = LoggerFactory.getLogger(LocalFileAppender.class);

    private volatile boolean started = false;

    private LocalFileStore localFileStore;

    private Thread appendThread;

    private WriteCommandQueue writeCommandQueue;

    public LocalFileAppender(LocalFileStore localFileStore) {
        this.localFileStore = localFileStore;
        //写入命令队列
        this.writeCommandQueue = new WriteCommandQueue();
        //启动异步入盘线程
        this.startAppendThread();
    }

    private void startAppendThread() {
        try {
            this.appendThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        try {
                            LocalFileAppender.this.flushQueueData();
                        } catch (Exception e) {
                            logger.error("run error:", e);
                        }
                    }
                }
            });
            logger.warn("start appendThread!");
            this.appendThread.setName("appendThread");
            this.appendThread.setPriority(Thread.MAX_PRIORITY);
            this.appendThread.setDaemon(true);
            this.appendThread.start();
            this.started = true;
        } catch (Throwable e) {
            logger.error("startAppendThread error: ", e);
        }
    }

    /**
     * 刷新队列数据
     * 1  将数据拆成 多个批次数据
     * 2  先写入数据文件，然后入日志文件
     * 3  清理batch -- queue的内容
     * 4  处理完成后，设置最后刷新时间
     *
     * @throws InterruptedException
     */
    private void flushQueueData() throws InterruptedException, IOException {
        Map<Integer, WriteBatch> batchMap = asembleWriteBatch();
        Iterator it = batchMap.keySet().iterator();
        while (it.hasNext()) {
            Integer number = (Integer) it.next();
            WriteBatch writeBatch = batchMap.get(number);
            writeDataAndLogFile(writeBatch);
            processFileAndIndexMap(writeBatch);
        }
    }

    private Map<Integer, WriteBatch> asembleWriteBatch() throws InterruptedException, IOException {
        Map<Integer, WriteBatch> batchMap = new TreeMap<Integer, WriteBatch>();
        List<WriteCommand> writeCommands = writeCommandQueue.getQueueCommands();
        for (int i = 0; i < writeCommands.size(); i++) {
            //是否需要创建新的文件
            boolean createNextFile = false;
            //当前写文件的编号
            int currentNumber = this.localFileStore.getNumber().get();
            WriteBatch writeBatch = batchMap.get(currentNumber);
            WriteCommand writeCommand = writeCommands.get(i);
            LocalFile currentDataFile = this.localFileStore.getCurrentDataFile();
            if (writeCommand.getOperateItem().getOperate() == OperateItem.OP_ADD) {
                //文件太大 或者 批次数据太大
                if (writeCommand.getData().length + currentDataFile.position() >= this.localFileStore.DEFAULT_MAX_FILE_SIZE ||
                        (writeBatch != null && writeBatch.getBatchDataSize() + writeCommand.getData().length >= this.localFileStore.DEFAULT_MAX_BATCH_SIZE)) {
                    this.localFileStore.createNewLocalFile();
                    createNextFile = true;
                }
            }
            //初始化writeBatch
            if (writeBatch == null || createNextFile) {
                writeBatch = new WriteBatch(
                        this.localFileStore.getNumber().get(),
                        this.localFileStore.getCurrentDataFile(),
                        this.localFileStore.getCurrentLogFile()
                );
                batchMap.put(this.localFileStore.getNumber().get(), writeBatch);
            }
            //添加到批次中,并且设置 操作的 offset 和 number
            writeBatch.addWriteCommandAndSetOperateItem(writeCommand);
        }
        return batchMap;
    }

    private void writeDataAndLogFile(WriteBatch writeBatch) {
        LocalFile dataFile = writeBatch.getDataFile();
        LogLocalFile logLocalFile = writeBatch.getLogLocalFile();
        //分配内存资源
        final ByteBuffer dataBuf = ByteBuffer.allocate(writeBatch.getBatchDataSize());
        final ByteBuffer logBuf = ByteBuffer.allocate(writeBatch.getWriteCommandList().size() * OperateItem.LENGTH);
        for (final WriteCommand writeCommand : writeBatch.getWriteCommandList()) {
            logBuf.put(writeCommand.getOperateItem().toByte());
            if (writeCommand.getOperateItem().getOperate() == OperateItem.OP_ADD) {
                dataBuf.put(writeCommand.getData());
            }
        }
        if (dataBuf != null) {
            dataBuf.flip();
        }
        logBuf.flip();
        //写文件的内容
        try {
            if (dataBuf != null) {
                dataFile.write(dataBuf);
                dataFile.force();
            }
            logLocalFile.write(logBuf);
            logLocalFile.force();
        } catch (Exception e) {
            logger.error("write file error:", e);
        }
    }

    private void processFileAndIndexMap(WriteBatch writeBatch) {
        for (final WriteCommand writeCommand : writeBatch.getWriteCommandList()) {
            OperateItem operateItem = writeCommand.getOperateItem();
            if (operateItem.getOperate() == OperateItem.OP_ADD) {
                this.localFileStore.getIndexMap().put(new BytesKey(operateItem.getKey()), operateItem);
            }
            if (operateItem.getOperate() == OperateItem.OP_DEL) {
                this.localFileStore.getIndexMap().remove(new BytesKey(operateItem.getKey()));
            }
        }
        writeCommandQueue.setLastFlushTime();
        try {
            LocalFile dataFile = writeBatch.getDataFile();
            LogLocalFile logLocalFile = writeBatch.getLogLocalFile();

            int currentNumber = this.localFileStore.getNumber().get();
            if (currentNumber > writeBatch.getNumber() && dataFile.isUnUsed()) {
                this.localFileStore.getDataLocalFiles().remove(Integer.valueOf(dataFile.getNumber()));
                this.localFileStore.getLogLocalFiles().remove(Integer.valueOf(dataFile.getNumber()));
                dataFile.delete();
                logLocalFile.delete();
            }
        } catch (Exception e) {
            logger.error("removeProcess error:", e);
        }
    }

    public void store(byte operate, BytesKey bytesKey, final byte[] data, final boolean force) throws IOException, InterruptedException {
        if (!this.started) {
            throw new RuntimeException("DataFileAppender已经关闭");
        }
        OperateItem operateItem = new OperateItem();
        operateItem.setOperate(operate);
        operateItem.setKey(bytesKey.getData());
        operateItem.setLength(data == null ? 0 : data.length);

        enqueueTryWait(operateItem, data, force);
    }

    private void enqueueTryWait(final OperateItem operateItem, byte[] data, final boolean force) throws IOException, InterruptedException {
        WriteCommand writeCommand = new WriteCommand(operateItem, data, force);
        writeCommandQueue.insert(writeCommand);
    }

    public void close() throws IOException {
        //先同步数据
        for (final LocalFile dataFile : this.localFileStore.getDataLocalFiles().values()) {
            try {
                dataFile.close();
            } catch (final Exception e) {
                logger.warn("close error:" + dataFile, e);
            }
        }
        this.localFileStore.getDataLocalFiles().clear();
        for (final LocalFile lf : this.localFileStore.getLogLocalFiles().values()) {
            try {
                lf.close();
            } catch (final Exception e) {
                logger.warn("close error:" + lf, e);
            }
        }
        this.localFileStore.getLogLocalFiles().clear();
        this.localFileStore.getIndexMap().close();
    }

}
