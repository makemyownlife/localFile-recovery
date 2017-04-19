package com.fcar.store.recovery.journal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 数据文件操作类
 * Created by zhangyong on 2017/1/29.
 */
public class LocalFileAppender {

    private final static Logger logger = LoggerFactory.getLogger(LocalFileAppender.class);

    private volatile boolean started = false;

    private final Lock enqueLock = new ReentrantLock();

    private LocalFileStore localFileStore;

    private Thread appendThread;

    public LocalFileAppender(LocalFileStore localFileStore) {
        this.localFileStore = localFileStore;
        //启动异步入盘线程
        this.startAppendThread();
    }

    private void startAppendThread() {
        try {
            this.appendThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    LocalFileAppender.this.processQueue();
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

    private void processQueue() {
        while (true) {
            System.out.println("processQueue");
            try {
                Thread.currentThread().sleep(50000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public OperateItem store(byte operate, BytesKey bytesKey, final byte[] data, final boolean sync) throws IOException {
        if (!this.started) {
            throw new RuntimeException("DataFileAppender已经关闭");
        }
        OperateItem operateItem = new OperateItem();
        operateItem.setOperate(operate);
        operateItem.setKey(bytesKey.getData());
        operateItem.setLength(data.length);

        operateItem = this.enqueueTryWait(operateItem, sync);
        return operateItem;
    }

    private OperateItem enqueueTryWait(final OperateItem operateItem, final boolean sync) throws IOException {
        return null;
    }

    public void close() {

    }

}
