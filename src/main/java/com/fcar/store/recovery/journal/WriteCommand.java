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

    public WriteCommand(OperateItem operateItem, boolean force) {
        this.operateItem = operateItem;
        this.force = force;
    }

    public OperateItem getOperateItem() {
        return operateItem;
    }

    public boolean isForce() {
        return force;
    }
}
