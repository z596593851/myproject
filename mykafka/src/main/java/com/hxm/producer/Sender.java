package com.hxm.producer;

import com.hxm.clients.NetworkClient;
import com.hxm.protocol.ApiKeys;
import com.hxm.protocol.Struct;
import com.hxm.requests.ProduceRequest;
import com.hxm.requests.RequestHeader;
import com.hxm.test.ClientRequest;
import com.hxm.test.ClientResponse;
import com.hxm.test.RequestCompletionHandler;
import com.hxm.network.RequestSend;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Sender implements Runnable{

    private final RecordAccumulator accumulator;
    private volatile boolean running;
    private final Time time;
    private final short acks;
    private final int requestTimeout;
    private final NetworkClient client;

    public Sender(NetworkClient client,RecordAccumulator accumulator, Time time, short acks, int requestTimeout){
        this.running=true;
        this.accumulator=accumulator;
        this.time=time;
        this.acks=acks;
        this.requestTimeout=requestTimeout;
        this.client=client;

    }

    @Override
    public void run() {
        while (running){
            run(time.milliseconds());
        }
    }

    void run(long now){
        client.initiateConnect();
        //将在同一个broker上的leader partition分为一组
        Map<Integer, List<RecordBatch>> batches = accumulator.drain();
        if(!batches.isEmpty()){
            //将发往同一个broker上面partition的数据组合成一个请求，而不是一个partition发一个请求，从而减少网络IO
            List<ClientRequest> requests = createProduceRequests(batches, now);
            //发送请求（其实只是给channel绑定OP_WRITE时间，并往inFlightRequests存5个请求）
            for (ClientRequest request : requests) {
                client.send(request);
            }
        }
        //真正执行网络操作
        //第一次执行到这里时建立连接
        this.client.poll(now);
    }

    public List<ClientRequest> createProduceRequests(Map<Integer, List<RecordBatch>> collated, long now) {
        List<ClientRequest> requests = new ArrayList<ClientRequest>(collated.size());
        for (Map.Entry<Integer, List<RecordBatch>> entry : collated.entrySet()) {
            requests.add(produceRequest(now, entry.getKey(), acks, requestTimeout, entry.getValue()));
        }
        return requests;
    }

    private ClientRequest produceRequest(long now, int destination, short acks, int timeout, List<RecordBatch> batches) {
        Map<TopicPartition, ByteBuffer> produceRecordsByPartition = new HashMap<TopicPartition, ByteBuffer>(batches.size());
        final Map<TopicPartition, RecordBatch> recordsByPartition = new HashMap<TopicPartition, RecordBatch>(batches.size());
        for (RecordBatch batch : batches) {
            TopicPartition tp = batch.topicPartition;
            produceRecordsByPartition.put(tp, batch.records.buffer());
            recordsByPartition.put(tp, batch);
        }
        //构造请求包
        ProduceRequest request = new ProduceRequest(acks, timeout, produceRecordsByPartition);
        Struct body=request.toStruct();
        RequestHeader header=this.client.nextRequestHeader(ApiKeys.PRODUCE);
        RequestSend send = new RequestSend(Integer.toString(destination),header,body);
        RequestCompletionHandler callback = new RequestCompletionHandler() {
            @Override
            public void onComplete(ClientResponse response) {
               //handleProduceResponse(response, recordsByPartition, time.milliseconds());
            }
        };
        //封装请求，带了一个回调函数
        return new ClientRequest(now, acks != 0, send, callback);
    }

    public void wakeup() {
        this.client.wakeup();
    }

    public void initiateClose() {
        // Ensure accumulator is closed first to guarantee that no more appends are accepted after
        // breaking from the sender loop. Otherwise, we may miss some callbacks when shutting down.
//        this.accumulator.close();
        //this.running = false;
        this.wakeup();
    }
}
