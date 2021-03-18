package com.hxm.broker;

import com.hxm.cluster.Partition;
import com.hxm.cluster.Replica;
import com.hxm.consumer.PartitionFetchInfo;
import com.hxm.log.Log;
import com.hxm.log.LogManager;
import com.hxm.message.ByteBufferMessageSet;
import com.hxm.message.FetchDataInfo;
import com.hxm.message.MessageSet;
import com.hxm.producer.Time;
import com.hxm.producer.TopicPartition;
import lombok.val;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReplicaManager {
    private final LogManager logManager;
    private final Map<TopicPartition, Partition> allPartitions=new HashMap<>();
    private final Time time;
    private boolean hardMaxBytesLimit;
    public ReplicaManager(LogManager logManager,Time time){
        this.logManager=logManager;
        this.time=time;
    }

    public void appendMessages(Map<TopicPartition, ByteBufferMessageSet> messagesPerPartition){
        for(Map.Entry<TopicPartition, ByteBufferMessageSet> entry:messagesPerPartition.entrySet()){
            Partition partition=getPartition(entry.getKey().topic(),entry.getKey().partition());
            partition.appendMessagesToLeader(entry.getValue());
        }
    }

    public void becomeLeaderOrFollower(List<TopicPartition> topicPartitionList){
        List<Partition> partitionsTobeLeader=new ArrayList<>();
        topicPartitionList.forEach(topicPartition -> {
            Partition partition=getOrCreatePartition(topicPartition.topic(),topicPartition.partition());
            partitionsTobeLeader.add(partition);
        });
        makeLeaders(partitionsTobeLeader);

    }

    private void makeLeaders(List<Partition> partitionsTobeLeader){
        partitionsTobeLeader.forEach(Partition::makeLeader);
    }

    private Partition getOrCreatePartition(String topic, int partitionId){
        TopicPartition key=new TopicPartition(topic,partitionId);
        Partition partition=allPartitions.get(key);
        if(partition==null){
            allPartitions.put(new TopicPartition(topic,partitionId),new Partition(topic,partitionId,time,this));
            return allPartitions.get(key);
        }else {
            return partition;
        }
    }

    private Partition getPartition(String topic, int partitionId){
        return allPartitions.get(new TopicPartition(topic,partitionId));
    }

    public LogManager getLogManager() {
        return logManager;
    }

    public void fetchMessages(long timeout, int replicaId, int fetchMinBytes, int fetchMaxBytes, boolean hardMaxBytesLimit,TopicPartition tp, PartitionFetchInfo fetchInfo){
        LogReadResult logReadResult=readFromLocalLog(fetchMaxBytes,hardMaxBytesLimit,tp,fetchInfo);
        // update its corresponding log end offset
        updateFollowerLogReadResults(replicaId,tp,logReadResult);
        int bytesReadable=logReadResult.getInfo().getMessageSet().sizeInBytes();
        // respond immediately if 1) fetch request does not want to wait
        //                        2) fetch request does not require any data
        //                        3) has enough data to respond
        //                        4) some error happens while reading data
        if (timeout <= 0 || bytesReadable >= fetchMinBytes) {

        }

    }

    private void updateFollowerLogReadResults(int replicaId, TopicPartition topicPartition, LogReadResult logReadResult){
        Partition partition=getPartition(topicPartition.topic(),topicPartition.partition());
        if(partition!=null){
            partition.updateReplicaLogReadResult(replicaId,logReadResult);
        }else{
            throw new RuntimeException(String.format("While recording the replica LEO, the partition %s hasn't been created.",topicPartition));
        }
    }

    public LogReadResult readFromLocalLog(int fetchMaxBytes,boolean hardMaxBytesLimit, TopicPartition tp, PartitionFetchInfo fetchInfo){
        this.hardMaxBytesLimit=hardMaxBytesLimit;
        return read(tp,fetchInfo,fetchMaxBytes,!hardMaxBytesLimit);
    }

    public LogReadResult read(TopicPartition tp, PartitionFetchInfo fetchInfo, int limitBytes, boolean minOneMessage){
        Replica localReplica=getPartition(tp.topic(),tp.partition()).leaderReplicaIfLocal();
        LogOffsetMetadata initialLogEndOffset = localReplica.logEndOffset();
        FetchDataInfo logReadInfo=null;
        Log log=localReplica.getLog();
        if(log==null){
            System.out.println("Leader for partition "+tp+" does not have a local log");
            logReadInfo=new FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MessageSet.Empty,false);
        }else {
            int adjustedFetchSize = Math.min(fetchInfo.getFetchSize(), limitBytes);
            FetchDataInfo fetch = log.read(fetchInfo.getOffset(), adjustedFetchSize, null, minOneMessage);
            if (!hardMaxBytesLimit && fetch.isFirstMessageSetIncomplete()) {
                logReadInfo = new FetchDataInfo(fetch.getFetchOffsetMetadata(), MessageSet.Empty, false);
            } else {
                logReadInfo = fetch;
            }
        }
        boolean readToEndOfLog = initialLogEndOffset.getMessageOffset() - logReadInfo.getFetchOffsetMetadata().getMessageOffset() <= 0;
        return new LogReadResult(logReadInfo,0L,fetchInfo.getFetchSize(),readToEndOfLog);

    }


}
