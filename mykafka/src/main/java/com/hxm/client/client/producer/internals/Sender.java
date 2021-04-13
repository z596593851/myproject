package com.hxm.client.client.producer.internals;

import com.hxm.client.client.ClientRequest;
import com.hxm.client.client.NetworkClient;
import com.hxm.client.client.RequestCompletionHandler;
import com.hxm.client.common.Cluster;
import com.hxm.client.common.Node;
import com.hxm.client.common.PartitionInfo;
import com.hxm.client.common.TopicPartition;
import com.hxm.client.common.protocol.ApiKeys;
import com.hxm.client.common.protocol.Struct;
import com.hxm.client.common.requests.ProduceRequest;
import com.hxm.client.common.requests.RequestHeader;
import com.hxm.client.common.requests.RequestSend;
import com.hxm.client.common.utils.Time;
import java.nio.ByteBuffer;
import java.util.*;

public class Sender implements Runnable{

    private final RecordAccumulator accumulator;
    private volatile boolean running;
    private final Time time;
    private final short acks;
    private final int requestTimeout;
    private final NetworkClient client;
    /* the maximum request size to attempt to send to the server */
    private final int maxRequestSize;

    public Sender(NetworkClient client,RecordAccumulator accumulator, Time time, short acks, int requestTimeout, int maxRequestSize){
        this.running=true;
        this.accumulator=accumulator;
        this.time=time;
        this.acks=acks;
        this.requestTimeout=requestTimeout;
        this.client=client;
        this.maxRequestSize=maxRequestSize;
    }

    @Override
    public void run() {
        while (running){
            run(time.milliseconds());
        }
    }

    void run(long now){
        //自定义一个服务器节点
        Cluster cluster=defultCluster();
        RecordAccumulator.ReadyCheckResult result = this.accumulator.ready(cluster, now);
        // remove any nodes we aren't ready to send to
        Iterator<Node> iter = result.readyNodes.iterator();
        long notReadyTimeout = Long.MAX_VALUE;
        while (iter.hasNext()) {
            Node node = iter.next();
            //3、检查与要发送数据的主机的网络是否已经建立好，并尝试连接
            //第一次执行时，是没有建立好的，直接走最后一行建立连接
            if (!this.client.ready(node, now)) {
                //移除result里要发送消息的主机
                iter.remove();
                notReadyTimeout = Math.min(notReadyTimeout, this.client.connectionDelay(node, now));
            }
        }
        //将在同一个broker上的leader partition分为一组
        Map<Integer, List<RecordBatch>> batches = accumulator.drain(cluster,result.readyNodes,this.maxRequestSize,now);
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

    private Cluster defultCluster(){
        Node node=new Node(1,"127.0.0.1",6666);
        List<Node> nodes=new ArrayList<>();
        PartitionInfo partitionInfo=new PartitionInfo("xiaoming",0,node);
        List<PartitionInfo> partitions=new ArrayList<>();
        nodes.add(node);
        partitions.add(partitionInfo);
        return new Cluster(false,nodes,partitions);
    }

    public List<ClientRequest> createProduceRequests(Map<Integer, List<RecordBatch>> collated, long now) {
        List<ClientRequest> requests = new ArrayList<>(collated.size());
        for (Map.Entry<Integer, List<RecordBatch>> entry : collated.entrySet()) {
            requests.add(produceRequest(now, entry.getKey(), acks, requestTimeout, entry.getValue()));
        }
        return requests;
    }

    private ClientRequest produceRequest(long now, int destination, short acks, int timeout, List<RecordBatch> batches) {
        Map<TopicPartition, ByteBuffer> produceRecordsByPartition = new HashMap<>(batches.size());
        final Map<TopicPartition, RecordBatch> recordsByPartition = new HashMap<>(batches.size());
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

        RequestCompletionHandler callback = response -> {
//               handleProduceResponse(response, recordsByPartition, time.milliseconds());
        };
        //封装请求，带了一个回调函数
        return new ClientRequest(now, acks != 0, send, callback);
    }

    public void wakeup() {
        this.client.wakeup();
    }

    public void initiateClose() {
        this.accumulator.close();
        this.running = false;
        this.wakeup();
    }
}
