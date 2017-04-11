package com.fcar.store.recovery;

import com.fcar.store.recovery.journal.LocalFileStore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by zhangyong on 2017/4/11.
 */
public class LocalFileStoreUnitTest {

    private final static Logger logger = LoggerFactory.getLogger(LocalFileStoreUnitTest.class);

    @Test
    public void testAddData() throws IOException {
        String path = "D://localstore";
        String name = "order-store";
        AbstractStore store = new LocalFileStore(
                path,
                name,
                true,
                null
        );
    }

}
