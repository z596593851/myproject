package com.hxm.broker;

import com.hxm.consumer.FetchRequest;
import com.hxm.consumer.FetchResponse;
import com.hxm.consumer.FetchResponsePartitionData;
import com.hxm.consumer.PartitionFetchInfo;
import com.hxm.message.ByteBufferMessageSet;
import com.hxm.message.MessageSet;
import com.hxm.network.FetchResponseSend;
import com.hxm.producer.TopicPartition;
import com.hxm.protocol.ApiKeys;
import com.hxm.requests.ProduceRequest;
import javafx.util.Pair;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
        switch (ApiKeys.forId(request.getRequestId())){
            case PRODUCE:handleProducerRequest(request);break;
            case FETCH:handleFetchRequest(request);break;
            default:throw new RuntimeException("Unknown api code " + request.getRequestId());
        }

    }

    private void handleProducerRequest(RequestChannel.Request request){
        System.out.println("收到Producer消息");
        ProduceRequest produceRequest=(ProduceRequest)request.getBody();
        Map<TopicPartition, ByteBufferMessageSet> authorizedMessagesPerPartition=new HashMap<>();
        for(Map.Entry<TopicPartition,ByteBuffer> entry:produceRequest.partitionRecords().entrySet()){
            authorizedMessagesPerPartition.put(entry.getKey(),new ByteBufferMessageSet(entry.getValue()));
        }
        replicaManager.appendMessages(authorizedMessagesPerPartition);
    }

    private void handleFetchRequest(RequestChannel.Request request){
        System.out.println("收到Producer消息");
        FetchRequest fetchRequest=(FetchRequest)request;
        List<Pair<TopicPartition, PartitionFetchInfo>> fetchInfos=fetchRequest.getRequestInfo();
        replicaManager.fetchMessages(
                fetchRequest.getMaxWait(),
                fetchRequest.getReplicaId(),
                fetchRequest.getMinBytes(),
                fetchRequest.getMaxBytes(),
                fetchRequest.getVersionId()<2,
                fetchInfos,
                (responsePartitionData)->{
                    FetchResponse response=new FetchResponse(fetchRequest.getCorrelationId(), responsePartitionData, fetchRequest.getVersionId(),0);
                    requestChannel.sendResponse(new RequestChannel.Response(request.processor(), request, new FetchResponseSend(request.connectionId, response)));
                }
        );

    }
}
