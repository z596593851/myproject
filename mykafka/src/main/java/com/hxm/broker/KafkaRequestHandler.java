package com.hxm.broker;

import com.hxm.message.ByteBufferMessageSet;
import com.hxm.message.MessageSet;
import com.hxm.producer.TopicPartition;
import com.hxm.protocol.ApiKeys;
import com.hxm.requests.ProduceRequest;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class KafkaRequestHandler implements Runnable{

    private final RequestChannel requestChannel;
    private final ReplicaManager replicaManager;

    public KafkaRequestHandler(RequestChannel requestChannel,ReplicaManager replicaManager){
        this.requestChannel=requestChannel;
        this.replicaManager=replicaManager;
    }

    @Override
    public void run() {
        while (true){
            RequestChannel.Request request=null;
            while (request==null){
                request=this.requestChannel.receiveRequest(300L);
            }
            handle(request);
        }
    }

    public void handle(RequestChannel.Request request){
        if(ApiKeys.PRODUCE.id==request.getRequestId()){
            handleProducerRequest(request);
        }
    }

    public void handleProducerRequest(RequestChannel.Request request){
        System.out.println("收到消息");
        ProduceRequest produceRequest=(ProduceRequest)request.getBody();
        Map<TopicPartition, ByteBufferMessageSet> authorizedMessagesPerPartition=new HashMap<>();
        for(Map.Entry<TopicPartition,ByteBuffer> entry:produceRequest.partitionRecords().entrySet()){
            authorizedMessagesPerPartition.put(entry.getKey(),new ByteBufferMessageSet(entry.getValue()));
        }
        replicaManager.appendMessages(authorizedMessagesPerPartition);
    }

}
