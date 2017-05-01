package com.fcar.store.recovery;

import java.io.File;

/**
 * Created by zhangyong on 2017/4/28.
 */
public class RecoveryConfig {

    private String path = System.getProperty("localFile.recover.path", System.getProperty("user.home") + File.separator + ".localFile_recover");

    private String storeName = "my-default";

    /**
     * recover本地消息的时间间隔
     */
    private long recoverMessageIntervalInmills = 5 * 60 * 1000L;

    private int recoverThreadCount = Runtime.getRuntime().availableProcessors();

    public long getRecoverMessageIntervalInmills() {
        return recoverMessageIntervalInmills;
    }

    public void setRecoverMessageIntervalInmills(long recoverMessageIntervalInmills) {
        this.recoverMessageIntervalInmills = recoverMessageIntervalInmills;
    }

    public String getPath() {
        return path;
    }

    public void setStoreName(String storeName) {
        this.storeName = storeName;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getStoreName() {
        return storeName;
    }

    public int getRecoverThreadCount() {
        return recoverThreadCount;
    }

    public void setRecoverThreadCount(int recoverThreadCount) {
        this.recoverThreadCount = recoverThreadCount;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (this.recoverMessageIntervalInmills ^ (this.recoverMessageIntervalInmills >>> 32));
        result = prime * result + this.recoverThreadCount;
        result = prime * result + this.path.hashCode();
        result = prime * result + this.storeName.hashCode();
        return result;
    }


}
