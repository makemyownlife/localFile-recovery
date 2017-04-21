package com.fcar.store.recovery.journal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 写入命令队列
 * Created by zhangyong on 2017/4/20.
 */
public class WriteCommandQueue {

    private final static Logger logger = LoggerFactory.getLogger(WriteCommandQueue.class);

    private final Lock enqueLock = new ReentrantLock();

    private final Condition available = enqueLock.newCondition();

    //默认自大的刷盘数据大小1M
    private static int DEFAULT_MAX_FLUSH_DATA_SIZE = 10;

    //当前队列中存储的数据大小
    private AtomicInteger currentTotalDataSize = new AtomicInteger(0);

    //上次刷新时间
    private volatile long lastFlushTime = System.currentTimeMillis();

    private LinkedList<WriteCommand> linkedList = new LinkedList<WriteCommand>();

    //最大的刷盘数据
    private int maxFlushDataSize;

    public WriteCommandQueue() {
        this(DEFAULT_MAX_FLUSH_DATA_SIZE);
    }

    public WriteCommandQueue(int maxFlushDataSize) {
        this.maxFlushDataSize = maxFlushDataSize;
    }

    public void insert(WriteCommand writeCommand) throws InterruptedException {
        final Lock lock = this.enqueLock;
        lock.lockInterruptibly();
        try {
            linkedList.addFirst(writeCommand);
            currentTotalDataSize.addAndGet(writeCommand.getOperateItem().getLength());
            if (needFlush() || writeCommand.isForce()) {
                available.signal();
            }
        } finally {
            lock.unlock();
        }
    }

    private boolean needFlush() {
        if (currentTotalDataSize.get() >= maxFlushDataSize) {
            return true;
        }
        return false;
    }

    public LinkedList<WriteCommand> takeCommands() throws InterruptedException {
        enqueLock.lockInterruptibly();
        try {
            available.await();
            return this.linkedList;
        } finally {
            enqueLock.unlock();
        }
    }

}