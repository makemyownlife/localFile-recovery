package com.fcar.store.recovery.journal;

import com.fcar.store.recovery.AbstractStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 本地文件日志存储
 * Created by zhangyong on 2017/4/10.
 */
public class LocalFileStore implements AbstractStore {

    private final static Logger logger = LoggerFactory.getLogger(LocalFileStore.class);

    //文件加载类
    private LocalFileLoader localFileLoader;

    //本地文件操作器
    private LocalFileAppender localFileAppender;

    private String path;

    private String name;

    private final boolean force;

    public LocalFileStore(final String path,
                          final String name,
                          final boolean force) throws IOException {
        this.path = path;
        this.name = name;
        this.force = force;
        this.localFileLoader = new LocalFileLoader(path, name, force);
        this.localFileAppender = new LocalFileAppender(localFileLoader);
        //当应用被关闭的时候,如果没有关闭文件,关闭之.对某些操作系统有用
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    LocalFileStore.this.close();
                } catch (final Exception e) {
                    logger.error("close error", e);
                }
            }
        });
    }

    //=========================================================================================basic method start ================================================================================================
    private void checkParam(final byte[] key, final byte[] data) {
        if (null == key || null == data) {
            throw new NullPointerException("key/data can't be null");
        }
        if (key.length != 16) {
            throw new IllegalArgumentException("key.length must be 16");
        }
    }

    private void innerAdd(final byte[] key, final byte[] data, final long oldLastTime, final boolean force) throws IOException, InterruptedException {
        BytesKey bytesKey = new BytesKey(key);
        this.localFileAppender.store(OperateItem.OP_ADD, bytesKey, data, force);
    }

    //=========================================================================================basic method end ================================================================================================
    @Override
    public void add(byte[] key, byte[] data) throws IOException, InterruptedException {
        this.add(key, data, false);
    }

    @Override
    public void add(byte[] key, byte[] data, boolean force) throws IOException, InterruptedException {
        // 先检查是否已经存在，如果已经存在抛出异常 判断文件是否满了，添加name.1，获得offset，记录日志，增加引用计数，加入或更新内存索引
        this.checkParam(key, data);
        this.innerAdd(key, data, -1, force);
    }

    @Override
    public boolean remove(byte[] key) throws IOException, InterruptedException {
        return this.remove(key, false);
    }

    @Override
    public boolean remove(byte[] key, boolean force) throws IOException, InterruptedException {
        BytesKey bytesKey = new BytesKey(key);
        this.localFileAppender.store(OperateItem.OP_DEL, bytesKey, null, force);
        return true;
    }

    @Override
    public Iterator<byte[]> iterator() throws IOException {
        final Iterator<BytesKey> it = this.localFileLoader.getIndexMap().keyIterator();
        return new Iterator<byte[]>() {
            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public byte[] next() {
                final BytesKey bk = it.next();
                if (null != bk) {
                    return bk.getData();
                }
                return null;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("不支持删除，请直接调用store.remove方法");
            }
        };
    }

    @Override
    public byte[] get(byte[] key) throws IOException {
        byte[] data = null;
        BytesKey bytesKey = new BytesKey(key);
        OperateItem operateItem = this.localFileLoader.getIndexMap().get(bytesKey);
        if (operateItem == null) {
            return data;
        }
        final LocalFile dataFile = this.localFileLoader.getDataLocalFiles().get(Integer.valueOf(operateItem.getNumber()));
        if (null != dataFile) {
            final ByteBuffer bf = ByteBuffer.wrap(new byte[operateItem.getLength()]);
            dataFile.read(bf, operateItem.getOffset());
            data = bf.array();
        } else {
            logger.warn("数据文件丢失：" + operateItem);
            this.localFileLoader.getIndexMap().remove(bytesKey);
        }
        return data;
    }

    @Override
    public int size() {
        return this.localFileLoader.getIndexMap().size();
    }

    @Override
    public void close() throws IOException, InterruptedException {
        //同步数据并且关闭文件
        this.localFileAppender.close();
        //关闭loader里的内容
        this.localFileLoader.clear();
    }


}
