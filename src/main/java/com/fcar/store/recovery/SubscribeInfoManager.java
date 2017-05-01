package com.fcar.store.recovery;

import java.io.UnsupportedEncodingException;

/**
 * Created by zhangyong on 2017/5/1.
 */
public interface SubscribeInfoManager {

    public void handle(byte[] key, byte[] data) throws UnsupportedEncodingException;

}
