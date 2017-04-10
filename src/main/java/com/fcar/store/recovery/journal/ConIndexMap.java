package com.fcar.store.recovery.journal;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 同步本地索引map
 *
 * Created by zhangyong on 2017/1/28.
 */
public class ConIndexMap implements IndexMap {

    private final ConcurrentHashMap<BytesKey, OperateItem> map;

    public ConIndexMap() {
        this.map = new ConcurrentHashMap<BytesKey, OperateItem>();
    }

    @Override
    public boolean containsKey(final BytesKey key) {
        return this.map.containsKey(key);
    }

    @Override
    public OperateItem get(final BytesKey key) {
        return this.map.get(key);
    }

    @Override
    public Iterator<BytesKey> keyIterator() {
        return this.map.keySet().iterator();
    }

    @Override
    public void put(final BytesKey key, final OperateItem OperateItem) {
        this.map.put(key, OperateItem);
    }

    @Override
    public void putAll(final Map<BytesKey, OperateItem> map) {
        this.map.putAll(map);
    }

    @Override
    public void remove(final BytesKey key) {
        this.map.remove(key);
    }

    @Override
    public int size() {
        return this.map.size();
    }

    @Override
    public void close() throws IOException {
        this.map.clear();
    }

}
