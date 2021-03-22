package com.hxm.consumer;

import com.hxm.producer.TopicPartition;
import javafx.util.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FetchRequest {
    private static short CurrentVersion=3;
    private static int DefaultCorrelationId=0;
    private final int maxWait;
    private final int replicaId;
    private final int minBytes;
    private final int maxBytes;
    private short versionId=CurrentVersion;
    private int correlationId=DefaultCorrelationId;
    private List<Pair<TopicPartition,PartitionFetchInfo>> requestInfo;

    public FetchRequest(int maxWait, int replicaId, int minBytes, int maxBytes) {
        this.maxWait = maxWait;
        this.replicaId = replicaId;
        this.minBytes = minBytes;
        this.maxBytes = maxBytes;
    }

    public static Map<String, List<Pair<Integer,FetchResponsePartitionData>>> batchByTopic(List<Pair<TopicPartition,FetchResponsePartitionData>> datas){
        Map<String,List<Pair<Integer,FetchResponsePartitionData>>> result=new HashMap<>();
        for(Pair<TopicPartition,FetchResponsePartitionData> data:datas){
            TopicPartition tp=data.getKey();
            String topic=tp.topic();
            int partition=tp.partition();
            List<Pair<Integer,FetchResponsePartitionData>> value=result.get(tp.topic());
            if(result.isEmpty() || value==null){
                result.put(topic,new ArrayList<>());
            }
            result.get(topic).add(new Pair<>(partition,data.getValue()));
        }

        return result;
    }

    public int getCorrelationId() {
        return correlationId;
    }

    public int getMaxWait() {
        return maxWait;
    }

    public int getReplicaId() {
        return replicaId;
    }

    public int getMinBytes() {
        return minBytes;
    }

    public int getMaxBytes() {
        return maxBytes;
    }

    public short getVersionId() {
        return versionId;
    }

    public List<Pair<TopicPartition, PartitionFetchInfo>> getRequestInfo() {
        return requestInfo;
    }
}
