package com.fcar.store.recovery.journal;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 写入命令队列
 * Created by zhangyong on 2017/4/20.
 */
public class WriteCommandQueue {

    //默认自大的刷盘数据大小 5M
    public static int DEFAULT_MAX_FLUSH_DATA_SIZE = 1024 * 1024 * 5;

    //默认刷新频率 5s
    public static int DEFAULT_FLUSH_INTERVAL = 5;

    //最大的刷盘数据
    private int maxFlushDataSize;

    //隔多长时间刷盘
    private int flushInterval;

    private final Lock enqueLock = new ReentrantLock();

    public WriteCommandQueue() {
        this(DEFAULT_MAX_FLUSH_DATA_SIZE, DEFAULT_FLUSH_INTERVAL);
    }

    public WriteCommandQueue(int maxFlushDataSize, int flushInterval) {
        this.maxFlushDataSize = maxFlushDataSize;
        this.flushInterval = flushInterval;
    }



}
