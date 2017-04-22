package com.fcar.store.recovery.journal;

import java.util.LinkedList;
import java.util.List;

/**
 * 批次写对象
 * Created by zhangyong on 2017/4/22.
 */
public class WriteBatch {

    private Integer number;

    private List<WriteCommand> writeCommandList;

    public WriteBatch(Integer number) {
        this.number = number;
        this.writeCommandList = new LinkedList<WriteCommand>();
    }

    public Integer getNumber() {
        return number;
    }

    public List<WriteCommand> getWriteCommandList() {
        return writeCommandList;
    }

}
