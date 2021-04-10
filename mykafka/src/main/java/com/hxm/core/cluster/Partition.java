package com.hxm.core.cluster;

import com.hxm.core.server.LogReadResult;
import com.hxm.core.server.ReplicaManager;
import com.hxm.core.log.Log;
import com.hxm.core.log.LogManager;
import com.hxm.core.message.ByteBufferMessageSet;
import com.hxm.client.common.utils.Time;
import com.hxm.client.common.TopicPartition;

import java.util.HashMap;
import java.util.Map;

public class Partition {
    private final String topic;
    private final int partitionId;
    private final Time time;
    private final ReplicaManager replicaManager;
    private final LogManager logManager;
    private final Map<Integer, Replica> assignedReplicaMap=new HashMap<>();
    private final int localBrokerId=0;

    public Partition(String topic, int partitionId, Time time, ReplicaManager replicaManager) {
        this.topic = topic;
        this.partitionId = partitionId;
        this.time = time;
        this.replicaManager = replicaManager;
        this.logManager=replicaManager.getLogManager();
    }

    public void appendMessagesToLeader(ByteBufferMessageSet messageSet){
        Replica leaderReplica=getReplica(localBrokerId);
        Log log=leaderReplica.getLog();
        log.append(messageSet);
    }

    public void makeLeader(){
        getOrCreateReplica();
    }

    private void getOrCreateReplica(){
        Log log=logManager.createLog(new TopicPartition(topic,partitionId));
        Replica localReplica=new Replica(0,this,time,log);
        addReplicaIfNotExists(localReplica);
    }

    private void addReplicaIfNotExists(Replica replica){
        assignedReplicaMap.putIfAbsent(replica.getBrokerId(), replica);
    }

    private Replica getReplica(int replicaId){
        return assignedReplicaMap.get(replicaId);
    }

    public Replica leaderReplicaIfLocal(){
        return getReplica(localBrokerId);
    }

    public void updateReplicaLogReadResult(int replicaId, LogReadResult logReadResult){
        Replica replica=getReplica(replicaId);
        if(replica!=null){
            replica.updateLogReadResult(logReadResult);
        }else {
            throw new RuntimeException(String.format("Leader %d failed to record follower %d's position %d since the replica" +
                    " is not recognized to be one of the assigned replicas for partition %s.",
                    localBrokerId,
                    replicaId,
                    logReadResult.getInfo().getFetchOffsetMetadata().getMessageOffset(),
                    new TopicPartition(topic, partitionId)));
        }
    }


}
