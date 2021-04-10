package com.hxm.core.cluster;

import com.hxm.core.server.LogOffsetMetadata;
import com.hxm.core.server.LogReadResult;
import com.hxm.core.log.Log;
import com.hxm.client.common.utils.Time;

public class Replica {
    private final int brokerId;
    private final Partition partition;
    private final Log log;
    private final Time time;
    private LogOffsetMetadata logEndOffsetMetadata;
    private LogOffsetMetadata logEndOffset;

    public Replica(int brokerId, Partition partition, Time time, Log log) {
        this.brokerId = brokerId;
        this.partition = partition;
        this.time=time;
        this.log=log;
        this.logEndOffsetMetadata=LogOffsetMetadata.UnknownOffsetMetadata;
        this.logEndOffset=getLogEndOffset();

    }

    public int getBrokerId() {
        return brokerId;
    }

    public Partition getPartition() {
        return partition;
    }

    public Log getLog() {
        return log;
    }

    public LogOffsetMetadata logEndOffset(){
        return this.logEndOffset;
    }

    public LogOffsetMetadata getLogEndOffset(){
        if(log!=null){
            return log.logEndOffsetMetadata();
        }else {
            return logEndOffsetMetadata;
        }
    }

    public void updateLogReadResult(LogReadResult logReadResult){
        this.logEndOffset=logReadResult.getInfo().getFetchOffsetMetadata();
    }
}
