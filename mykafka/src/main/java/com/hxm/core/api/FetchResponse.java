package com.hxm.core.api;

import com.hxm.client.common.requests.FetchRequest;
import com.hxm.client.common.TopicPartition;
import javafx.util.Pair;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * server端的FetchResponse(由scala翻译而来)
 */
public class FetchResponse {
    private final int correlationId;
    private final List<Pair<TopicPartition, FetchResponsePartitionData>> data;
    private final int requestVersion;
    private final int throttleTimeMs;
    private final Map<String, List<Pair<Integer,FetchResponsePartitionData>>> dataGroupedByTopic;
    private final int sizeInBytes;

    public FetchResponse(int correlationId, List<Pair<TopicPartition,FetchResponsePartitionData>> data, int requestVersion, int throttleTimeMs) {
        this.correlationId = correlationId;
        this.data = data;
        this.requestVersion = requestVersion;
        this.throttleTimeMs = throttleTimeMs;
        this.dataGroupedByTopic=batchByTopic(this.data);
        this.sizeInBytes=sizeInBytes();
    }

    public FetchResponse readFrom(ByteBuffer buffer, int requestVersion) {
        int correlationId = buffer.getInt();
        int throttleTime = requestVersion > 0?buffer.getInt() : 0;
        int topicCount = buffer.getInt();
        List<Pair<TopicPartition,FetchResponsePartitionData>> list =new ArrayList<>();
        while (topicCount-- >0){
            TopicData topicData=TopicData.readFrom(buffer);
            topicData.getPartitionData().forEach(pair->{
                list.add(new Pair<>(new TopicPartition(topicData.getTopic(),pair.getKey()),pair.getValue()));
            });
        }
        return new FetchResponse(correlationId, list, requestVersion, throttleTime);
    }

    public int headerSizeInBytes(){
        return headerSize(requestVersion);
    }

    private Map<String, List<Pair<Integer,FetchResponsePartitionData>>> batchByTopic(List<Pair<TopicPartition,FetchResponsePartitionData>> data){
        return FetchRequest.batchByTopic(data);
    }

    private static int headerSize(int requestVersion){
        // correlationId + topic count + throttleTimeSize
        int throttleTimeSize=requestVersion>0?4:0;
        return 4+4+throttleTimeSize;
    }

    private static int responseSize(Map<String, List<Pair<Integer,FetchResponsePartitionData>>> dataGroupedByTopic, int requestVersion){
        int temp=0;
        for(Map.Entry<String, List<Pair<Integer,FetchResponsePartitionData>>>entry:dataGroupedByTopic.entrySet()){
            TopicData topicData=new TopicData(entry.getKey(),entry.getValue());
            temp+=topicData.sizeInBytes();
        }
        return headerSize(requestVersion)+temp;
    }

    public int sizeInBytes(){
        return FetchResponse.responseSize(dataGroupedByTopic,requestVersion);
    }

    public ByteBuffer writeHeaderTo(ByteBuffer buffer){
        buffer.putInt(sizeInBytes);
        buffer.putInt(correlationId);
        // Include the throttleTime only if the client can read it
        if (requestVersion > 0) {
            buffer.putInt(throttleTimeMs);
        }
        // topic count
        return buffer.putInt(dataGroupedByTopic.size());
    }

    public Map<String, List<Pair<Integer, FetchResponsePartitionData>>> getDataGroupedByTopic() {
        return dataGroupedByTopic;
    }
}
