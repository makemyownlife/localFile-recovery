package com.fcar.store.recovery.journal;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 本地文件
 * Created by zhangyong on 2017/4/10.
 */
public class LocalFile {

    protected final File file;

    protected FileChannel fileChannel;

    //文件号?
    private final int number;

    //当前指针
    private volatile long currentPos;

    private final AtomicInteger referenceCount = new AtomicInteger(0);

    public LocalFile(final File file, final int number, final boolean force) throws IOException {
        this.file = file;
        // 在rw下是使用cache的，
        this.fileChannel = new RandomAccessFile(file, force ? "rws" : "rw").getChannel();
        // 指针移到最后
        this.fileChannel.position(this.fileChannel.size());
        this.currentPos = this.fileChannel.position();
        this.number = number;
    }

    public long position() throws IOException {
        return this.currentPos;
    }

    public long length() throws IOException {
        return this.fileChannel.size();
    }

    public int getNumber() {
        return number;
    }

    /**
     * 对文件增加一个引用计数
     * s
     */
    public int increment() {
        return this.referenceCount.incrementAndGet();
    }

    public int increment(final int n) {
        return this.referenceCount.addAndGet(n);
    }

    /**
     * 对文件减少一个引用计数
     */
    public int decrement() {
        return this.referenceCount.decrementAndGet();
    }

    public int decrementUntilZero() {
        int current = this.referenceCount.get();
        if(current == 0) {
            return current;
        }else {
            return this.referenceCount.decrementAndGet();
        }
    }

    public int decrement(final int n) {
        return this.referenceCount.addAndGet(-n);
    }

    /**
     * 文件是否还在使用（引用计数是否是0了）
     */
    public boolean isUnUsed() {
        return this.getReferenceCount() <= 0;
    }


    /**
     * 获得引用计数的值
     */
    public int getReferenceCount() {
        return this.referenceCount.get();
    }

    //指针添加
    public void forward(final long offset) {
        this.currentPos += offset;
    }

    //获取文件最后修改时间
    public long lastModified() throws IOException {
        return this.file.lastModified();
    }

    /**
     * 删除文件
     */
    public boolean delete() throws IOException {
        this.close();
        return this.file.delete();
    }

    /**
     * 关闭文件
     */
    public void close() throws IOException {
        this.fileChannel.close();
    }

    /**
     * 强制将数据写回硬盘
     */
    public void force() throws IOException {
        this.fileChannel.force(true);
    }

    /**
     * 从文件的制定位置读取数据到bf，直到读满或者读到文件结尾
     * 文件指针不会移动
     */
    public void read(final ByteBuffer bf, final long offset) throws IOException {
        int size = 0;
        int l = 0;
        while (bf.hasRemaining()) {
            l = this.fileChannel.read(bf, offset + size);
            if (l < 0) {
                // 数据还未写入，忙等待
                if (offset < this.currentPos) {
                    continue;
                } else {
                    break;
                }
            }
            size += l;
        }
    }

    /**
     * 写入bf长度的数据到文件，文件指针会向后移动
     *
     * @param bf
     * @return 写入后的文件position
     * @throws IOException
     */
    public long write(final ByteBuffer bf) throws IOException {
        while (bf.hasRemaining()) {
            final int l = this.fileChannel.write(bf);
            if (l < 0) {
                break;
            }
        }
        return this.fileChannel.position();
    }


    /**
     * 从指定位置写入bf长度的数据到文件，文件指针<b>不会</b>向后移动
     *
     * @param offset
     * @param bf
     * @throws IOException
     */
    public void write(final long offset, final ByteBuffer bf) throws IOException {
        int size = 0;
        while (bf.hasRemaining()) {
            final int l = this.fileChannel.write(bf, offset + size);
            size += l;
            if (l < 0) {
                break;
            }
        }
    }

    @Override
    public String toString() {
        String result = null;
        try {
            result =
                    this.file.getName() + " , length = " + this.length() + " refCount = " + this.referenceCount
                            + " position:" + this.fileChannel.position();
        } catch (final IOException e) {
            result = e.getMessage();
        }
        return result;
    }

}
