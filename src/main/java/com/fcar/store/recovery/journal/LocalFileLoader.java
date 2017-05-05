package com.fcar.store.recovery.journal;

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
 * Created by zhangyong on 2017/5/2.
 */
public class LocalFileLoader {

    private final static Logger logger = LoggerFactory.getLogger(LocalFileLoader.class);

    //文件最大20M
    public static final int DEFAULT_MAX_FILE_SIZE = 1024 * 1024 * 20;

    //最大的批次大小
    public static final int DEFAULT_MAX_BATCH_SIZE = 1024 * 1024 * 4;

    //当前的文件编号
    private final AtomicInteger number = new AtomicInteger(0);

    public Map<Integer, LocalFile> dataLocalFiles = new ConcurrentHashMap<Integer, LocalFile>();

    protected Map<Integer, LogLocalFile> logLocalFiles = new ConcurrentHashMap<Integer, LogLocalFile>();

    private final Map<BytesKey, Long> lastModifiedMap = new ConcurrentHashMap<BytesKey, Long>();

    private IndexMap indexMap;

    //当前的数据文件
    private LocalFile currentDataFile;

    //当前的日志文件
    private LogLocalFile currentLogFile;

    private String name;

    private String path;

    private boolean force;

    public LocalFileLoader(String path, String name, boolean force) throws IOException {
        this.path = path;
        this.name = name;
        this.force = force;
        this.indexMap = new ConIndexMap();
        this.initLoad();
    }

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
                        dataLocalFile.decrementUntilZero();
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


    private void checkParentDir(final File parent) {
        if (!parent.exists() && !parent.mkdirs()) {
            throw new IllegalStateException("Can't make dir " + this.path);
        }
    }

    /**
     * 创建新的本地文件
     */
    public void createNewLocalFile() throws IOException {
        final int n = this.number.incrementAndGet();
        this.currentDataFile = new LocalFile(new File(this.path + File.separator + this.name + "." + n), n, this.force);
        this.currentLogFile = new LogLocalFile(new File(this.path + File.separator + this.name + "." + n + ".log"), n, this.force);
        this.dataLocalFiles.put(Integer.valueOf(n), this.currentDataFile);
        this.logLocalFiles.put(Integer.valueOf(n), this.currentLogFile);
        logger.info("生成新文件：" + this.currentDataFile);
    }

    public int size() {
        return this.indexMap.size();
    }

    public void clear() throws IOException {
        this.dataLocalFiles.clear();
        this.logLocalFiles.clear();
        this.indexMap.close();
        this.lastModifiedMap.clear();
        this.currentLogFile = null;
        this.currentDataFile = null;
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
