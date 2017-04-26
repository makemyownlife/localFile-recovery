package com.fcar.store.recovery;

import java.io.IOException;
import java.util.Iterator;

/**
 * 本地文件存储
 * Created by zhangyong on 2017/1/25.
 */
public interface AbstractStore {

    void add(byte[] key, byte[] data) throws IOException, InterruptedException;

    void add(byte[] key, byte[] data, boolean force) throws IOException, InterruptedException;

    boolean remove(byte[] key) throws IOException, InterruptedException;

    boolean remove(byte[] key, boolean force) throws IOException, InterruptedException;

    byte[] get(byte[] key) throws IOException;

    Iterator<byte[]> iterator() throws IOException;

    //所有的消息数量
    int size();

    //关闭存储
    void close() throws IOException, InterruptedException;

}
