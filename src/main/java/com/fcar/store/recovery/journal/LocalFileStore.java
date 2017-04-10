package com.fcar.store.recovery.journal;

import com.fcar.store.recovery.AbstractStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.*;

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
            //数据文件
            File file = new File(parentDir, this.name + "." + n);
            //数据文件
            LocalFile dataLocalFile = new LocalFile(file, n, this.force);
            //操作记录文件
            LogLocalFile logLocalFile = new LogLocalFile(new File(file.getAbsolutePath() + ".log"), n, this.force);
            //总的操作数
            long itemsCount = dataLocalFile.length() / OperateItem.LENGTH;

        }
    }

    /**
     * Create the parent directory if it doesn't exist.
     */
    private void checkParentDir(final File parent) {
        if (!parent.exists() && !parent.mkdirs()) {
            throw new IllegalStateException("Can't make dir " + this.path);
        }
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
