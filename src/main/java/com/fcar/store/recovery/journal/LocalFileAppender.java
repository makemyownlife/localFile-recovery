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

    private boolean started = false;

    private LocalFileStore localFileStore;

    public LocalFileAppender(LocalFileStore localFileStore) {
        this.localFileStore = localFileStore;
    }

    public OperateItem store(byte operate, BytesKey bytesKey, final byte[] data, final boolean sync) throws IOException {
        if (!this.started) {
            throw new RuntimeException("DataFileAppender已经关闭");
        }
        OperateItem operateItem = new OperateItem();
        return operateItem;
    }

    public void close() {

    }

}
