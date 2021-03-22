package com.hxm.consumer;

import com.hxm.producer.TopicPartition;
import javafx.util.Pair;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class FetchResponse {
    private final int correlationId;
    private final List<Pair<TopicPartition,FetchResponsePartitionData>> data;
    private final int requestVersion;
    private final int throttleTimeMs;
    private final Map<String, List<Pair<Integer,FetchResponsePartitionData>>> dataGroupedByTopic;
    private final int sizeInBytes;

    public FetchResponse(int correlationId, List<Pair<TopicPartition,FetchResponsePartitionData>> data, int requestVersion, int throttleTimeMs) {
        this.correlationId = correlationId;
        this.data = data;
        this.requestVersion = requestVersion;
        this.throttleTimeMs = throttleTimeMs;
        this.dataGroupedByTopic=batchByTopic(data);
        this.sizeInBytes=sizeInBytes();
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


}
