package com.fcar.store.recovery.journal;

import java.io.File;
import java.io.IOException;

/**
 * 日志本地文件
 * Created by zhangyong on 2017/4/10.
 */
public class LogLocalFile extends LocalFile {

    public LogLocalFile(final File file, final int n) throws IOException {
        this(file, n, false);
    }

    public LogLocalFile(final File file, final int n, final boolean force) throws IOException {
        super(file, n, force);
        // 这个地方是为了防止操作日志文件的不完整。如果不完整，则丢弃最后不完整的数据。
        final long count = fileChannel.size() / OperateItem.LENGTH;
        if (count * OperateItem.LENGTH < fileChannel.size()) {
            //可能丢弃调多余的数据
            fileChannel.truncate(count * OperateItem.LENGTH);
            fileChannel.position(count * OperateItem.LENGTH);
        }
    }

}
