package com.hxm.client.client;

import com.hxm.client.common.Node;
import com.hxm.client.common.network.KSelector;
import com.hxm.client.common.network.NetworkReceive;
import com.hxm.client.common.network.Send;
import com.hxm.client.common.requests.MetadataRequest;
import com.hxm.client.common.requests.RequestSend;
import com.hxm.client.common.utils.Time;
import com.hxm.client.common.protocol.ApiKeys;
import com.hxm.client.common.protocol.ProtoUtils;
import com.hxm.client.common.protocol.Struct;
import com.hxm.client.common.requests.RequestHeader;
import com.hxm.client.common.requests.ResponseHeader;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class NetworkClient {

    private final String clientId;
    private KSelector selector;
    private final ClusterConnectionStates connectionStates;
    private int correlation;
    private final InFlightRequests inFlightRequests;
    private final Time time;

    public NetworkClient(KSelector selector, String clientId, long reconnectBackoffMs, Time time){
        this.clientId=clientId;
        this.selector=selector;
        this.correlation=0;
        this.inFlightRequests = new InFlightRequests(5);
        this.time=time;
        this.connectionStates=new ClusterConnectionStates(reconnectBackoffMs);
    }

    public void send(ClientRequest request){
        //暂存还没有收到响应的请求（默认5个），如果成功收到响应则会移除
        this.inFlightRequests.add(request);
        selector.send(request.request());
    }

    private void maybeUpdate(long now){
        Node node=new Node(1,"127.0.0.1",6666);
        String nodeConnectionId = node.idString();
        //判断网络是否建立好
        if (canSendRequest(nodeConnectionId)) {
//            RequestSend send=new RequestSend(nodeConnectionId,nextRequestHeader(ApiKeys.METADATA), MetadataRequest.allTopics().toStruct());
//            ClientRequest clientRequest = new ClientRequest(now,true,send,null);
//            doSend(clientRequest, now);
        } else if (connectionStates.canConnect(nodeConnectionId,now)) {
            initiateConnect(node, now);
        }
    }

    private void doSend(ClientRequest request, long now) {
        //暂存还没有收到响应的请求（默认5个），如果成功收到响应则会移除
        this.inFlightRequests.add(request);
        selector.send(request.request());
    }

    public void poll(long timeout){
        maybeUpdate(timeout);
        try {
            selector.poll(timeout);
        } catch (IOException e) {
            e.printStackTrace();
        }
        long updatedNow = this.time.milliseconds();
        List<ClientResponse> responses = new ArrayList<>();
        handleCompletedSends(responses,updatedNow);
        handleCompletedReceives(responses,updatedNow);
        handleConnections();
        //处理长时间没有响应的请求（那5个）
//        handleTimedOutRequests(responses, updatedNow);
        // invoke callbacks
        for (ClientResponse response : responses) {
            if (response.request().hasCallback()) {
                try {
                    //调用发送的请求的回调函数
                    response.request().callback().onComplete(response);
                } catch (Exception e) {
                    log.error("Uncaught error in request completion:", e);
                }
            }
        }
    }

    private void handleCompletedSends(List<ClientResponse> responses, long now) {
        // if no response is expected then when the send is completed, return it
        for (Send send : this.selector.completedSends()) {
            ClientRequest request = this.inFlightRequests.lastSent(send.destination());
            if (!request.expectResponse()) {
                this.inFlightRequests.completeLastSent(send.destination());
                responses.add(new ClientResponse(request, now, false, null));
            }
        }
    }

    public long connectionDelay(Node node, long now) {
        return connectionStates.connectionDelay(node.idString(), now);
    }

    private void handleCompletedReceives(List<ClientResponse> responses, long now) {
        //completedReceives就是存接收到的响应
        for (NetworkReceive receive : this.selector.completedReceives()) {
            //broker id
            String source = receive.source();
            //inFlightRequests是之前发送请求时暂存的未收到响应的5条请求
            ClientRequest req = inFlightRequests.completeNext(source);
            //解析服务端发送回来的响应
            Struct body = parseResponse(receive.payload(), req.request().header());
            responses.add(new ClientResponse(req, now, false, body));
        }
    }

    public boolean ready(Node node, long now) {
        if (node.isEmpty()) {
            throw new IllegalArgumentException("Cannot connect to empty node " + node);
        }
        //判断要发送消息的主机是否具备发送消息的条件
        if (isReady(node, now)) {
            return true;
        }
        //第一次进来是没有建立好的，尝试建立网络
        if (connectionStates.canConnect(node.idString(), now)) {
            initiateConnect(node, now);
        }
        return false;
    }

    public boolean isReady(Node node, long now) {
        return canSendRequest(node.idString());
    }

    private boolean canSendRequest(String node) {
        return connectionStates.isConnected(node) &&
                selector.isChannelReady(node) &&
                //每个往broker发送消息的连接，最多容忍5个消息发送出去了但是没有收到响应
                inFlightRequests.canSendMore(node);
    }
    public static Struct parseResponse(ByteBuffer responseBuffer, RequestHeader requestHeader) {
        ResponseHeader responseHeader = ResponseHeader.parse(responseBuffer);
        // Always expect the response version id to be the same as the request version id
        short apiKey = requestHeader.apiKey();
        short apiVer = requestHeader.apiVersion();
//        System.out.println(String.format("apiKey:%d , apiVer:%d",apiKey,apiVer));
        Struct responseBody = ProtoUtils.responseSchema(apiKey, apiVer).read(responseBuffer);
//        Struct responseBody = ProtoUtils.responseSchema(1, 2).read(responseBuffer);
        correlate(requestHeader, responseHeader);
        return responseBody;
    }

    private static void correlate(RequestHeader requestHeader, ResponseHeader responseHeader) {
        if (requestHeader.correlationId() != responseHeader.correlationId()) {
            throw new IllegalStateException("Correlation id for response (" + responseHeader.correlationId()
                    + ") does not match request (" + requestHeader.correlationId() + "), request header: " + requestHeader);
        }
    }

    public void initiateConnect(Node node, long now){
        String nodeConnectionId = node.idString();
        try {
            this.connectionStates.connecting(nodeConnectionId, now);
            selector.connect(nodeConnectionId,new InetSocketAddress(node.host(),node.port()),102400,102400);
        } catch (IOException e) {
            connectionStates.disconnected(nodeConnectionId, now);
        }
    }

    public RequestHeader nextRequestHeader(ApiKeys key) {
        return new RequestHeader(key.id, clientId, correlation++);
    }

    public RequestHeader nextRequestHeader(ApiKeys key, short version) {
        return new RequestHeader(key.id,version, clientId, correlation++);
    }

    public void wakeup() {
        this.selector.wakeup();
    }

    private void handleConnections() {
        for (String node : this.selector.connected()) {
            log.debug("Completed connection to node {}", node);
            this.connectionStates.connected(node);
        }
    }

}
