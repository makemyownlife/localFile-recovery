package com.fcar.store.recovery.journal;

import com.fcar.store.recovery.AbstractStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 本地文件日志存储
 * Created by zhangyong on 2017/4/10.
 */
public class LocalFileStore implements AbstractStore {

    private final static Logger logger = LoggerFactory.getLogger(LocalFileStore.class);

    //文件最大20M
    public static final int DEFAULT_MAX_FILE_SIZE = 1024 * 1024 * 20;

    //最大的批次大小
    public static final int DEFAULT_MAX_BATCH_SIZE = 1024 * 1024 * 4;

    //当前的文件编号
    private final AtomicInteger number = new AtomicInteger(0);

    public Map<Integer, LocalFile> dataLocalFiles = new ConcurrentHashMap<Integer, LocalFile>();

    protected Map<Integer, LogLocalFile> logLocalFiles = new ConcurrentHashMap<Integer, LogLocalFile>();

    private final Map<BytesKey, Long> lastModifiedMap = new ConcurrentHashMap<BytesKey, Long>();

    private LocalFileAppender localFileAppender;

    //当前的数据文件
    private LocalFile currentDataFile;

    //当前的日志文件
    private LogLocalFile currentLogFile;

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
        if (this.currentDataFile == null || this.currentLogFile == null) {
            this.createNewLocalFile();
        }
        this.localFileAppender = new LocalFileAppender(this);
        //当应用被关闭的时候,如果没有关闭文件,关闭之.对某些操作系统有用
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    LocalFileStore.this.close();
                } catch (final IOException e) {
                    logger.error("close error", e);
                }
            }
        });
    }

    public LocalFileStore(final String path,
                          final String name,
                          final boolean force) throws IOException {
        this(path, name, force, null);
    }

    /*
     * 类初始化的时候，需要遍历所有的日志文件，恢复内存的索引
     */
    private synchronized void initLoad() throws IOException {
        logger.warn("开始恢复数据");
        final String nm = this.name + ".";
        final File parentDir = new File(this.path);
        this.checkParentDir(parentDir);
        //所有的数据文件 不包含操作日志文件
        final File[] dataFiles = parentDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(final File dir, final String n) {
                return n.startsWith(nm) && !n.endsWith(".log");
            }
        });
        if (dataFiles == null || dataFiles.length == 0) {
            logger.warn("父目录:{} ，没有查找到名为：{}的数据文件!", this.path, this.name);
            return;
        }
        logger.warn("遍历每个数据文件");
        //提取出每个数据文件的文件号,然后排序分开处理
        final List<Integer> indexList = new LinkedList<Integer>();
        for (final File f : dataFiles) {
            try {
                final String fn = f.getName();
                final int n = Integer.parseInt(fn.substring(nm.length()));
                indexList.add(Integer.valueOf(n));
            } catch (final Exception e) {
                logger.error("parse file index error" + f, e);
            }
        }
        Integer[] indices = indexList.toArray(new Integer[indexList.size()]);
        //对文件顺序进行排序(从小到大排序)
        Arrays.sort(indices);
        for (final Integer n : indices) {
            logger.warn("处理index为" + n + "的文件");
            Map<BytesKey, OperateItem> tempIndexMap = new HashMap<BytesKey, OperateItem>();
            File file = new File(parentDir, this.name + "." + n);
            //数据文件
            LocalFile dataLocalFile = new LocalFile(file, n, this.force);
            //操作记录文件
            LogLocalFile logLocalFile = new LogLocalFile(new File(file.getAbsolutePath() + ".log"), n, this.force);
            //总的操作数
            long itemsCount = logLocalFile.length() / OperateItem.LENGTH;
            for (int i = 0; i < itemsCount; ++i) {
                ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[OperateItem.LENGTH]);
                //实际上是读取一个操作记录
                logLocalFile.read(byteBuffer, i * OperateItem.LENGTH);
                if (byteBuffer.hasRemaining()) {
                    logger.warn("log file error:" + logLocalFile + ", index:" + i);
                    continue;
                }
                OperateItem operateItem = new OperateItem();
                operateItem.parse(byteBuffer.array());
                BytesKey bytesKey = new BytesKey(operateItem.getKey());
                switch (operateItem.getOperate()) {
                    case OperateItem.OP_ADD:
                        OperateItem cachedItem = indexMap.get(bytesKey);
                        if (null != cachedItem) {
                            this.indexMap.remove(bytesKey);
                            this.lastModifiedMap.remove(bytesKey);
                        }
                        //引用计数,为了便于后续的删除以及处理
                        boolean addRefCount = true;
                        if (tempIndexMap.get(bytesKey) != null) {
                            //在同一个文件中add或者update过，那么只是更新内容，而不增加引用计数。
                            addRefCount = false;
                        }
                        tempIndexMap.put(bytesKey, operateItem);
                        if (addRefCount) {
                            dataLocalFile.increment();
                        }
                        break;
                    case OperateItem.OP_DEL:
                        tempIndexMap.remove(bytesKey);
                        dataLocalFile.decrement();
                        break;
                    default:
                        logger.warn("unknow operateItem:" + (int) operateItem.getOperate());
                        break;
                }
            }
            // 如果这个数据文件已经达到指定大小，并且不再使用，删除
            if (dataLocalFile.length() >= DEFAULT_MAX_FILE_SIZE && dataLocalFile.isUnUsed()) {
                dataLocalFile.delete();
                logLocalFile.delete();
                logger.warn(dataLocalFile + "不用了，也超过了大小，删除");
            } else {
                this.dataLocalFiles.put(n, dataLocalFile);
                this.logLocalFiles.put(n, logLocalFile);
                //如果有索引，加入总索引
                if (!dataLocalFile.isUnUsed()) {
                    this.indexMap.putAll(tempIndexMap);
                    //从新启动后，用日志文件的最后修改时间,这里没有必要非常精确.
                    final long lastModified = dataLocalFile.lastModified();
                    for (final BytesKey key : tempIndexMap.keySet()) {
                        this.lastModifiedMap.put(key, lastModified);
                    }
                    logger.warn("还在使用，放入索引，referenceCount:" + dataLocalFile.getReferenceCount() + ", index:" + tempIndexMap.size());
                }
            }
        }
        //校验加载的文件,并设置当前文件
        if (this.dataLocalFiles.size() > 0) {
            indices = this.dataLocalFiles.keySet().toArray(new Integer[this.dataLocalFiles.keySet().size()]);
            Arrays.sort(indices);
            for (int i = 0; i < indices.length - 1; i++) {
                final LocalFile dataLocalFile = this.dataLocalFiles.get(indices[i]);
                if (dataLocalFile.isUnUsed() || dataLocalFile.length() < DEFAULT_MAX_FILE_SIZE) {
                    throw new IllegalStateException("非当前文件的状态是大于等于文件块长度，并且是used状态");
                }
            }
            final Integer n = indices[indices.length - 1];
            this.number.set(n.intValue());
            this.currentDataFile = this.dataLocalFiles.get(n);
            this.currentLogFile = this.logLocalFiles.get(n);
        }
        logger.warn("恢复数据：" + this.size());
    }

    /**
     * Create the parent directory if it doesn't exist.
     */
    private void checkParentDir(final File parent) {
        if (!parent.exists() && !parent.mkdirs()) {
            throw new IllegalStateException("Can't make dir " + this.path);
        }
    }

    /**
     * 创建新的本地文件
     */
    public synchronized void createNewLocalFile() throws IOException {
        final int n = this.number.incrementAndGet();
        this.currentDataFile = new LocalFile(new File(this.path + File.separator + this.name + "." + n), n, this.force);
        this.currentLogFile = new LogLocalFile(new File(this.path + File.separator + this.name + "." + n + ".log"), n, this.force);
        this.dataLocalFiles.put(Integer.valueOf(n), this.currentDataFile);
        this.logLocalFiles.put(Integer.valueOf(n), this.currentLogFile);
        logger.info("生成新文件：" + this.currentDataFile);
    }

    //=========================================================================================basic method start ================================================================================================
    private void checkParam(final byte[] key, final byte[] data) {
        if (null == key || null == data) {
            throw new NullPointerException("key/data can't be null");
        }
        //写入的文件最大值是1M
        if (data.length > 1024 * 1024) {
            throw new IllegalArgumentException("data must less than 1 M !");
        }
        if (key.length != 16) {
            throw new IllegalArgumentException("key.length must be 16");
        }
    }

    private void innerAdd(final byte[] key, final byte[] data, final long oldLastTime, final boolean force) throws IOException, InterruptedException {
        BytesKey bytesKey = new BytesKey(key);
        this.localFileAppender.store(OperateItem.OP_ADD, bytesKey, data, force);
        if (oldLastTime == -1) {
            this.lastModifiedMap.put(bytesKey, System.currentTimeMillis());
        }
        else {
            this.lastModifiedMap.put(bytesKey, oldLastTime);
        }
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
        this.innerAdd(key, data, -1, false);
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

    //==========================================================================================get method start ===========================================================
    public AtomicInteger getNumber() {
        return number;
    }

    public Map<Integer, LocalFile> getDataLocalFiles() {
        return dataLocalFiles;
    }

    public Map<Integer, LogLocalFile> getLogLocalFiles() {
        return logLocalFiles;
    }

    public LocalFile getCurrentDataFile() {
        return currentDataFile;
    }

    public LogLocalFile getCurrentLogFile() {
        return currentLogFile;
    }

    public String getName() {
        return name;
    }

    public IndexMap getIndexMap() {
        return indexMap;
    }
    //==========================================================================================get method end ===========================================================

}
