package com.fcar.store.recovery.journal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 数据文件操作类
 * Created by zhangyong on 2017/1/29.
 */
public class LocalFileAppender {

    private final static Logger logger = LoggerFactory.getLogger(LocalFileAppender.class);

    private volatile boolean started = false;

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

        }
    }

    public OperateItem store(byte operate, BytesKey bytesKey, final byte[] data, final boolean sync) throws IOException {
        if (!this.started) {
            throw new RuntimeException("DataFileAppender已经关闭");
        }
        OperateItem operateItem = new OperateItem();
        operateItem.setKey(bytesKey.getData());
        return operateItem;
    }

    public void close() {

    }

}
