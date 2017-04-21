package com.fcar.store.recovery;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by zhangyong on 2017/4/13.
 */
public class OperateItemUnitTest {

    private final static Logger logger = LoggerFactory.getLogger(OperateItemUnitTest.class);

    @Test
    public void testOperateItemKey() {
        Long orderId = 87665L;
        final ByteBuffer buf = ByteBuffer.allocate(16);
        buf.putLong(orderId);
        byte[] arr = buf.array();
        //字节转long型
        long num = 0;
        for (int ix = 0; ix < 8; ++ix) {
            num <<= 8;
            num |= (arr[ix] & 0xff);
        }
        System.out.println(num);
    }

    @Test
    public void testIncreaceAndGet() {
        AtomicInteger count = new AtomicInteger(0);
        count.addAndGet(4);
        Assert.assertEquals(count.get(), 4);
    }

    @Test
    public void testQueue() throws InterruptedException {

    }

}
