package com.fcar.store.recovery;

/**
 * Created by zhangyong on 2017/4/28.
 */
public class RecoveryConfig {

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

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (this.recoverMessageIntervalInmills ^ this.recoverMessageIntervalInmills >>> 32);
        result = prime * result + this.recoverThreadCount;
        return result;
    }


}
