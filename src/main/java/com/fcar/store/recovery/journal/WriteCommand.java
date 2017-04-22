package com.fcar.store.recovery.journal;

/**
 * 写入的命令
 * Created by zhangyong on 2017/4/20.
 */
public class WriteCommand {

    //操作明细
    private OperateItem operateItem;

    //是否同步
    private boolean force;

    //数据
    private byte[] data;

    public WriteCommand(OperateItem operateItem, byte[] data, boolean force) {
        this.operateItem = operateItem;
        this.data = data;
        this.force = force;
    }

    public OperateItem getOperateItem() {
        return operateItem;
    }

    public boolean isForce() {
        return force;
    }

    public byte[] getData() {
        return data;
    }

    @Override
    public String toString() {
        return this.operateItem.toString();
    }

}
