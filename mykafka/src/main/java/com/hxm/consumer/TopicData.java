package com.hxm.consumer;

import com.hxm.broker.Utils;
import javafx.util.Pair;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class TopicData {
    private final String topic;
    private final List<Pair<Integer,FetchResponsePartitionData>> partitionData;
    private final int headerSize;

    public TopicData(String topic, List<Pair<Integer,FetchResponsePartitionData>> partitionData) {
        this.topic = topic;
        this.partitionData = partitionData;
        this.headerSize=TopicData.headerSize(topic);
    }

    public int sizeInBytes(){
        int temp=0;
        for(Pair<Integer,FetchResponsePartitionData> pair : partitionData){
            temp+=pair.getValue().sizeInBytes()+4;
        }
        return headerSize(topic)+temp;
    }

    public static int headerSize(String topic) {
        int size=0;
        //4 : partition count
        try {
            size=Utils.shortStringLength(topic) +4;
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return size;
    }


    public TopicData readFrom(ByteBuffer buffer){
        List<Pair<Integer,FetchResponsePartitionData>> topicPartitionDataPairs= new ArrayList<>();
        String topic = null;
        try {
            topic=Utils.readShortString(buffer);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        int partitionCount=buffer.getInt();
        while(partitionCount-- >0){
            int partitionId=buffer.getInt();
            FetchResponsePartitionData partitionData = FetchResponsePartitionData.readFrom(buffer);
            topicPartitionDataPairs.add(new Pair<>(partitionId,partitionData));
        }
        return new TopicData(topic,topicPartitionDataPairs);
    }

    public int getHeaderSize() {
        return headerSize;
    }
}
