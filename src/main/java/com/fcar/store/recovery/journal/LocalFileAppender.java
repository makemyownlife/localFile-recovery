package com.fcar.store.recovery.journal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;

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

    private void flushQueueData() throws InterruptedException {
        LinkedList<WriteCommand> writeCommands = writeCommandQueue.takeCommands();
    }

    public OperateItem store(byte operate, BytesKey bytesKey, final byte[] data, final boolean force) throws IOException {
        if (!this.started) {
            throw new RuntimeException("DataFileAppender已经关闭");
        }
        OperateItem operateItem = new OperateItem();
        operateItem.setOperate(operate);
        operateItem.setKey(bytesKey.getData());
        operateItem.setLength(data.length);

        operateItem = this.enqueueTryWait(operateItem, force);
        return operateItem;
    }

    private OperateItem enqueueTryWait(final OperateItem operateItem, final boolean force) throws IOException {
        WriteCommand writeCommand = new WriteCommand(operateItem, force);
        writeCommandQueue.insert(writeCommand);
        return operateItem;
    }

    public void close() {

    }

}
