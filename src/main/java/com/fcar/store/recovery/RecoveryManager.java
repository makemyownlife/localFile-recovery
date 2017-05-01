package com.fcar.store.recovery;

import com.fcar.store.recovery.journal.LocalFileStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zhangyong on 2017/4/10.
 */
public class RecoveryManager {

    private final static Logger logger = LoggerFactory.getLogger(RecoveryManager.class);

    private RecoveryConfig recoveryConfig;

    private String name;

    private String path;

    private LocalFileStore localFileStore;

    public RecoveryManager(RecoveryConfig recoveryConfig, String name, String path) {
        this.recoveryConfig = recoveryConfig;
        this.name = name;
        this.path = path;
        this.makeStore();
    }

    private void makeStore() {
        try {
            this.localFileStore = new LocalFileStore(this.path, this.name, true);
        } catch (Exception e) {
            logger.error("makeStore error: ", e);
        }
    }

    public void appendMessage(Long key, byte[] data) {

    }

}
