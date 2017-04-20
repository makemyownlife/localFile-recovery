package com.fcar.store.recovery.journal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
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

    private final Condition notFull = enqueLock.newCondition();

    private final Lock flushLock = new ReentrantLock();

    private final Condition flushCondition = flushLock.newCondition();

    //默认自大的刷盘数据大小1M
    private static int DEFAULT_MAX_FLUSH_DATA_SIZE = 1024 * 1024;

    //默认刷新频率5s
    private static int DEFAULT_FLUSH_INTERVAL = 5;

    //当前队列中存储的数据大小
    private int currentTotalDataSize = 0;

    private LinkedList<WriteCommand> linkedList = new LinkedList<WriteCommand>();

    //最大的刷盘数据
    private int maxFlushDataSize;

    //隔多长时间刷盘
    private int flushInterval;

    public WriteCommandQueue() {
        this(DEFAULT_MAX_FLUSH_DATA_SIZE, DEFAULT_FLUSH_INTERVAL);
    }

    public WriteCommandQueue(int maxFlushDataSize, int flushInterval) {
        this.maxFlushDataSize = maxFlushDataSize;
        this.flushInterval = flushInterval;
    }

    public boolean insert(WriteCommand writeCommand) {
        try {
            enqueLock.lock();
            if (currentTotalDataSize + writeCommand.getOperateItem().getLength() >= maxFlushDataSize && currentTotalDataSize > 0) {
                notFull.await();
            }
            linkedList.addFirst(writeCommand);
            currentTotalDataSize += (writeCommand.getOperateItem().getLength());
        } catch (Exception e) {
            logger.error("offer error:", e);
        } finally {
            enqueLock.unlock();
        }
        return false;
    }

    public LinkedList<WriteCommand> takeCommands() throws InterruptedException {
        try {
            enqueLock.lock();
            flushCondition.await();
        } finally {
            enqueLock.unlock();
        }
        return null;
    }

}
