package com.hxm.core.server;

import com.hxm.core.cluster.Partition;
import com.hxm.core.cluster.Replica;
import com.hxm.core.api.FetchResponsePartitionData;
import com.hxm.core.api.PartitionFetchInfo;
import com.hxm.core.log.Log;
import com.hxm.core.log.LogManager;
import com.hxm.core.message.ByteBufferMessageSet;
import com.hxm.core.message.MessageSet;
import com.hxm.client.common.utils.Time;
import com.hxm.client.common.TopicPartition;
import javafx.util.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

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

    public void fetchMessages(long timeout, int replicaId, int fetchMinBytes, int fetchMaxBytes, boolean hardMaxBytesLimit, List<Pair<TopicPartition, PartitionFetchInfo>> fetchInfos, Consumer<List<Pair<TopicPartition, FetchResponsePartitionData>>> responseCallback){
        List<Pair<TopicPartition, LogReadResult>> logReadResults=readFromLocalLog(fetchMaxBytes,hardMaxBytesLimit,fetchInfos);
        // update its corresponding log end offset
        updateFollowerLogReadResults(replicaId,logReadResults);
        int bytesReadable=logReadResults.stream().mapToInt(pair->pair.getValue().getInfo().getMessageSet().sizeInBytes()).sum();
        // respond immediately if 1) fetch request does not want to wait
        //                        2) fetch request does not require any data
        //                        3) has enough data to respond
        //                        4) some error happens while reading data
        if (timeout <= 0 || bytesReadable >= fetchMinBytes) {
            List<Pair<TopicPartition, FetchResponsePartitionData>> list=new ArrayList<>();
            logReadResults.forEach(pair->{
                TopicPartition tp=pair.getKey();
                LogReadResult result=pair.getValue();
                list.add(new Pair<>(tp,new FetchResponsePartitionData(result.getHw(),result.getInfo().getMessageSet())));
            });
            responseCallback.accept(list);
        }else {
            System.out.println("wrong");
        }
    }

    private void updateFollowerLogReadResults(int replicaId, List<Pair<TopicPartition,LogReadResult>> logReadResults){
        logReadResults.forEach(pair->{
            TopicPartition topicPartition=pair.getKey();
            LogReadResult logReadResult=pair.getValue();
            Partition partition=getPartition(topicPartition.topic(),topicPartition.partition());
            if(partition!=null){
                partition.updateReplicaLogReadResult(replicaId,logReadResult);
            }else{
                throw new RuntimeException(String.format("While recording the replica LEO, the partition %s hasn't been created.",topicPartition));
            }
        });

    }

    public List<Pair<TopicPartition,LogReadResult>> readFromLocalLog(int fetchMaxBytes,boolean hardMaxBytesLimit, List<Pair<TopicPartition, PartitionFetchInfo>> readPartitionInfo){
        this.hardMaxBytesLimit=hardMaxBytesLimit;
        List<Pair<TopicPartition,LogReadResult>> result=new ArrayList<>();
        readPartitionInfo.forEach(pair->{
            LogReadResult readResult=read(pair.getKey(),pair.getValue(),fetchMaxBytes,!hardMaxBytesLimit);
            result.add(new Pair<>(pair.getKey(),readResult));
        });
        return result;
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
//            FetchDataInfo fetch = log.read(0, adjustedFetchSize, null, minOneMessage);
            FetchDataInfo fetch = log.read(fetchInfo.getOffset(), adjustedFetchSize, null, minOneMessage);
            if (!hardMaxBytesLimit && fetch.isFirstMessageSetIncomplete()) {
                logReadInfo = new FetchDataInfo(fetch.getFetchOffsetMetadata(), MessageSet.Empty, false);
            } else {
                logReadInfo = fetch;
            }
        }
        boolean readToEndOfLog = initialLogEndOffset.getMessageOffset() - logReadInfo.getFetchOffsetMetadata().getMessageOffset() <= 0;
        //暂时将hw定为LogEndOffset
        return new LogReadResult(logReadInfo,initialLogEndOffset.getMessageOffset(),fetchInfo.getFetchSize(),readToEndOfLog);

    }

    public interface Callback<A,B>{
        void apply(A a,B b);
    }


}
