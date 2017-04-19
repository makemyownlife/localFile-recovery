package com.fcar.store.recovery.journal;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * 一个操作日志  操作+数据key+数据文件编号+偏移量+长度
 * <p>
 * Created by zhangyong on 2017/1/26.
 */
public class OperateItem {

    public static final byte OP_ADD = 1;

    public static final byte OP_DEL = 2;

    public static final int KEY_LENGTH = 16;

    public static final int LENGTH = KEY_LENGTH + 1 + 4 + 8 + 4;

    //一个字节 -- 操作类型(添加或者删除)
    private byte operate;

    //key转成字节
    private byte[] key;

    //文件号
    private int number;

    //偏移量
    private volatile long offset;

    //数据大小
    private int length;

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(this.key);
        result = prime * result + this.length;
        result = prime * result + this.number;
        result = prime * result + (int) (this.offset ^ this.offset >>> 32);
        result = prime * result + this.operate;
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        final OperateItem other = (OperateItem) obj;
        if (!Arrays.equals(this.key, other.getKey())) {
            return false;
        }
        if (this.length != other.getLength()) {
            return false;
        }
        if (this.number != other.getNumber()) {
            return false;
        }
        if (this.offset != other.getOffset()) {
            return false;
        }
        if (this.operate != other.getOperate()) {
            return false;
        }
        return true;
    }

    /**
     * 将一个操作转换成字节数组
     *
     * @return 字节数组
     */
    public byte[] toByte() {
        final byte[] data = new byte[LENGTH];
        final ByteBuffer bf = ByteBuffer.wrap(data);
        bf.put(this.key);
        bf.put(this.operate);
        bf.putInt(this.number);
        bf.putLong(this.offset);
        bf.putInt(this.length);
        bf.flip();
        return bf.array();
    }

    public byte getOperate() {
        return operate;
    }

    public byte[] getKey() {
        return this.key;
    }

    public void setKey(final byte[] key) {
        this.key = key;
    }

    public int getNumber() {
        return this.number;
    }

    public void setNumber(final int number) {
        this.number = number;
    }

    public long getOffset() {
        return this.offset;
    }

    public void setOffset(final long offset) {
        this.offset = offset;
    }

    public int getLength() {
        return this.length;
    }

    public void setLength(final int length) {
        this.length = length;
    }

    public void setOperate(byte operate) {
        this.operate = operate;
    }

    /**
     * 通过字节数组构造成一个操作日志
     *
     * @param data
     */
    public void parse(final byte[] data) {
        this.parse(data, 0, data.length);
    }

    public void parse(final byte[] data, final int offset, final int length) {
        final ByteBuffer bf = ByteBuffer.wrap(data, offset, length);
        this.key = new byte[16];
        bf.get(this.key);
        this.operate = bf.get();
        this.number = bf.getInt();
        this.offset = bf.getLong();
        this.length = bf.getInt();
    }

    public void parse(final ByteBuffer bf) {
        this.key = new byte[16];
        bf.get(this.key);
        this.operate = bf.get();
        this.number = bf.getInt();
        this.offset = bf.getLong();
        this.length = bf.getInt();
    }

    @Override
    public String toString() {
        return "OperateItem number:" + this.number + ", op:" + (int) this.operate + ", offset:" + this.offset + ", length:"
                + this.length;
    }

}
