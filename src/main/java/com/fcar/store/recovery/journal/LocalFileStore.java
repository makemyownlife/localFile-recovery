package com.fcar.store.recovery.journal;

import com.fcar.store.recovery.AbstractStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 * 本地文件日志存储
 * Created by zhangyong on 2017/4/10.
 */
public class LocalFileStore implements AbstractStore {

    private final static Logger logger = LoggerFactory.getLogger(LocalFileStore.class);

    //文件最大20M
    public static final int FILE_SIZE = 1024 * 1024 * 20;

    private String path;

    private String name;

    private final boolean force;

    private IndexMap indexMap;

    public LocalFileStore(final String path,
                          final String name,
                          final boolean force,
                          final IndexMap indexMap) throws IOException {
        this.path = path;
        this.name = name;
        this.force = force;
        if (indexMap == null) {
            this.indexMap = new ConIndexMap();
        } else {
            this.indexMap = indexMap;
        }
        this.initLoad();
    }

    /*
     * 类初始化的时候，需要遍历所有的日志文件，恢复内存的索引
     */
    private void initLoad() throws IOException {
        logger.warn("开始恢复数据");


    }

    @Override
    public void add(byte[] key, byte[] data) throws IOException, InterruptedException {

    }

    @Override
    public void add(byte[] key, byte[] data, boolean force) throws IOException, InterruptedException {

    }

    @Override
    public Iterator<byte[]> iterator() throws IOException {
        return null;
    }

    @Override
    public byte[] get(byte[] key) throws IOException {
        return new byte[0];
    }

    @Override
    public int size() {
        return this.indexMap.size();
    }

    @Override
    public void close() throws IOException {

    }

}
