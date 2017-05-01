package com.fcar.store.recovery;

import com.fcar.store.recovery.journal.LocalFileStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhangyong on 2017/4/10.
 */
public class RecoveryManager {

    private final static Logger logger = LoggerFactory.getLogger(RecoveryManager.class);

    private RecoveryConfig recoveryConfig;

    private SubscribeInfoManager subscribeInfoManager;

    private LocalFileStore localFileStore;

    private ScheduledExecutorService scheduledExecutorService;

    public RecoveryManager(RecoveryConfig recoveryConfig, SubscribeInfoManager subscribeInfoManager) {
        this.recoveryConfig = recoveryConfig;
        this.subscribeInfoManager = subscribeInfoManager;
        this.makeStoreAndStartSchedule();
    }

    private void makeStoreAndStartSchedule() {
        try {
            this.localFileStore = new LocalFileStore(this.recoveryConfig.getPath(), this.recoveryConfig.getStoreName(), true);
            this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        Iterator<byte[]> iterator = getLocalFileStore().iterator();
                        while (iterator.hasNext()) {
                            byte[] key = iterator.next();
                            byte[] data = getLocalFileStore().get(key);
                            if (data != null) {
                                subscribeInfoManager.handle(key, data);
                            }
                        }
                    } catch (Exception e) {
                        logger.error("schedule run error: ", e);
                    }
                }
            }, this.recoveryConfig.getRecoverMessageIntervalInmills(), this.recoveryConfig.getRecoverMessageIntervalInmills(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            logger.error("makeStore error: ", e);
        }
    }

    public void appendMessage(Long key, byte[] data) throws IOException, InterruptedException {
        final ByteBuffer buf = ByteBuffer.allocate(16);
        buf.putLong(key);
        byte[] arr = buf.array();
        this.localFileStore.add(arr, data);
    }

    //======================================================================get method ==============================================================
    public LocalFileStore getLocalFileStore() {
        return localFileStore;
    }

}
