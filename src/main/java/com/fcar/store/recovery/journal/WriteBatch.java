package com.fcar.store.recovery.journal;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * 批次写对象
 * Created by zhangyong on 2017/4/22.
 */
public class WriteBatch {

    private Integer number;

    private List<WriteCommand> writeCommandList;

    private LocalFile dataFile;

    private LogLocalFile logLocalFile;

    private int batchDataSize = 0;

    public WriteBatch(Integer number, LocalFile dataFile, LogLocalFile logLocalFile) {
        this.number = number;
        this.writeCommandList = new LinkedList<WriteCommand>();
        this.dataFile = dataFile;
        this.logLocalFile = logLocalFile;
    }

    public Integer getNumber() {
        return number;
    }

    public List<WriteCommand> getWriteCommandList() {
        return writeCommandList;
    }

    public LocalFile getDataFile() {
        return dataFile;
    }

    public LogLocalFile getLogLocalFile() {
        return logLocalFile;
    }

    public int getBatchDataSize() {
        return this.batchDataSize;
    }

    //添加写入的命令 并且修改operateItem --》 offset 和 number
    public void addWriteCommandAndSetOperateItem(WriteCommand writeCommand) throws IOException {
        writeCommand.getOperateItem().setNumber(this.number);
        if (writeCommand.getOperateItem().getOperate() == OperateItem.OP_ADD) {
            this.batchDataSize += writeCommand.getData().length;
            writeCommand.getOperateItem().setOffset(this.dataFile.position());
            // 移动dataFile指针
            this.dataFile.forward(writeCommand.getData().length);
            this.dataFile.increment();
        }
        if (writeCommand.getOperateItem().getOperate() == OperateItem.OP_DEL) {
            this.dataFile.decrement();
        }
        this.writeCommandList.add(writeCommand);
    }

}
