package com.fcar.store.recovery;

import com.fcar.store.recovery.journal.LocalFileStore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by zhangyong on 2017/4/11.
 */
public class LocalFileStoreUnitTest {

    private final static Logger logger = LoggerFactory.getLogger(LocalFileStoreUnitTest.class);

    @Test
    public void testAddData() throws IOException, InterruptedException {
        String path = "D://localstore";
        String name = "order-store";
        AbstractStore store = new LocalFileStore(
                path,
                name,
                true,
                null
        );
        for (int i = 0; i < 2; i++) {
            Long orderId = 87665L;
            byte[] data = "张勇".getBytes("UTF-8");
            final ByteBuffer buf = ByteBuffer.allocate(16);
            buf.putLong(orderId);
            byte[] arr = buf.array();
            store.add(arr, data);
        }
        Thread.currentThread().sleep(3000);
        for (int i = 0; i < 1; i++) {
            Long orderId = 7876L;
            byte[] data = "李林".getBytes("UTF-8");
            final ByteBuffer buf = ByteBuffer.allocate(16);
            buf.putLong(orderId);
            byte[] arr = buf.array();
            store.add(arr, data);
        }
        Thread.currentThread().sleep(2000);
        for (int i = 0; i < 1; i++) {
            Long orderId = 1876L;
            byte[] data = "美好".getBytes("UTF-8");
            final ByteBuffer buf = ByteBuffer.allocate(16);
            buf.putLong(orderId);
            byte[] arr = buf.array();
            store.add(arr, data);
        }
        Thread.currentThread().sleep(5000);
    }

}
