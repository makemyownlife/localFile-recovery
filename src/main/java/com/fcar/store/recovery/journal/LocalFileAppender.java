package com.fcar.store.recovery.journal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

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
     * 2  多个批次一次 先写入数据文件，然后入日志文件
     * 3  清理batch -- queue的内容
     * 4  处理完成后，设置最后刷新时间
     *
     * @throws InterruptedException
     */
    private void flushQueueData() throws InterruptedException {
        List<WriteBatch> writeBatchList = asembleWriteBatch();
    }

    private List<WriteBatch> asembleWriteBatch() throws InterruptedException {
        Map<Integer ,WriteBatch> batchMap = new TreeMap<Integer, WriteBatch>();

        int number = this.localFileStore.getNumber().get();
        LinkedList<WriteCommand> writeCommands = writeCommandQueue.getQueueCommands();
        Iterator<WriteCommand> iterator = writeCommands.listIterator();
        while (iterator.hasNext()) {
            WriteCommand writeCommand = iterator.next();
            
        }
        return null;
    }

    public void store(byte operate, BytesKey bytesKey, final byte[] data, final boolean force) throws IOException, InterruptedException {
        if (!this.started) {
            throw new RuntimeException("DataFileAppender已经关闭");
        }
        OperateItem operateItem = new OperateItem();
        operateItem.setOperate(operate);
        operateItem.setKey(bytesKey.getData());
        operateItem.setLength(data.length);

        enqueueTryWait(operateItem, data, force);
    }

    private void enqueueTryWait(final OperateItem operateItem, byte[] data, final boolean force) throws IOException, InterruptedException {
        WriteCommand writeCommand = new WriteCommand(operateItem, data, force);
        writeCommandQueue.insert(writeCommand);
    }

    public void close() {

    }

}
