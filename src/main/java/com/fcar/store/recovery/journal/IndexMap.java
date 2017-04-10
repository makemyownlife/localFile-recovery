package com.fcar.store.recovery.journal;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by zhangyong on 2017/1/26.
 */
public interface IndexMap {

    void put(BytesKey key, OperateItem operateItem);

    void remove(BytesKey key);

    OperateItem get(BytesKey key);

    int size();

    boolean containsKey(BytesKey key);

    Iterator<BytesKey> keyIterator();

    void putAll(Map<BytesKey, OperateItem> map);

    void close() throws IOException;

}
