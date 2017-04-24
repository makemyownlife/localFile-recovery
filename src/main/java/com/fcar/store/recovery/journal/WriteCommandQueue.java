package com.fcar.store.recovery.journal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
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

    //最大数据刷新条数
    private static int MAX_FLUSH_COMMANDS_COUNT = 10;

    //上次刷新时间
    private volatile long lastFlushTime = System.currentTimeMillis();

    private LinkedList<WriteCommand> linkedList = new LinkedList<WriteCommand>();

    //最大的刷盘数据条数 到达MAX_FLUSH_COMMANDS_COUNT 则刷新到appender线程处理
    private int maxFlushCommandsCount = MAX_FLUSH_COMMANDS_COUNT;

    public WriteCommandQueue() {
        this(MAX_FLUSH_COMMANDS_COUNT);
    }

    public WriteCommandQueue(int maxFlushCommandsCount) {
        this.maxFlushCommandsCount = maxFlushCommandsCount;
    }

    public void insert(WriteCommand writeCommand) throws InterruptedException {
        final Lock lock = this.enqueLock;
        lock.lockInterruptibly();
        try {
            linkedList.addLast(writeCommand);
            if (needFlush() || writeCommand.isForce()) {
                available.signal();
            }
        } finally {
            lock.unlock();
        }
    }

    private boolean needFlush() {
        if (this.linkedList.size() >= this.maxFlushCommandsCount || (System.currentTimeMillis() - lastFlushTime > 2000L)) {
            return true;
        }
        return false;
    }

    public List<WriteCommand> getQueueCommands() throws InterruptedException {
        final Lock lock = this.enqueLock;
        lock.lockInterruptibly();
        try {
            List<WriteCommand> writeCommands = new ArrayList<WriteCommand>(this.linkedList.size());
            while (true) {
                WriteCommand writeCommand = this.linkedList.peek();
                if (writeCommand == null && writeCommands.size() == 0) {
                    available.await();
                }
                if (writeCommand != null) {
                    writeCommand = this.linkedList.pop();
                    writeCommands.add(writeCommand);
                    continue;
                }
                break;
            }
            return writeCommands;
        } finally {
            lock.unlock();
        }
    }

    public void setLastFlushTime() {
        this.lastFlushTime = System.currentTimeMillis();
    }

}
