package com.hxm.clients;

import com.hxm.broker.KSelector;
import com.hxm.broker.NetworkReceive;
import com.hxm.consumer.ClientResponse;
import com.hxm.network.Send;
import com.hxm.producer.Time;
import com.hxm.protocol.ApiKeys;
import com.hxm.protocol.ProtoUtils;
import com.hxm.protocol.Struct;
import com.hxm.requests.RequestHeader;
import com.hxm.consumer.ClientRequest;
import com.hxm.requests.ResponseHeader;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class NetworkClient {

    private KSelector selector;
    private boolean isConnect=false;
    private String id;
    private String host;
    private int port;
    private int correlation;
    private final InFlightRequests inFlightRequests;
    private final Time time;

    public NetworkClient(KSelector selector, String id, String host, int port, Time time){
        this.selector=selector;
        this.id=id;
        this.host=host;
        this.port=port;
        this.correlation=0;
        this.inFlightRequests = new InFlightRequests(5);
        this.time=time;
    }

    public void send(ClientRequest request){
        //暂存还没有收到响应的请求（默认5个），如果成功收到响应则会移除
        this.inFlightRequests.add(request);
        selector.send(request.request());
    }

    public void poll(long timeout){
        initiateConnect();
        try {
            selector.poll(timeout);
        } catch (IOException e) {
            e.printStackTrace();
        }
        long updatedNow = this.time.milliseconds();
        List<ClientResponse> responses = new ArrayList<>();
        handleCompletedSends(responses,updatedNow);
        handleCompletedReceives(responses,updatedNow);
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

    private void handleCompletedReceives(List<ClientResponse> responses, long now) {
        //completedReceives就是存接收到的响应
        for (NetworkReceive receive : this.selector.completedReceives()) {
            //broker id
            String source = receive.source();
            //inFlightRequests是之前发送请求时暂存的未收到响应的5条请求
            ClientRequest req = inFlightRequests.completeNext(source);
            //解析服务端发送回来的响应
            Struct body = parseResponse(receive.payload(), req.request().header());
            if (!maybeHandleCompletedReceive(req, now, body))
                //解析完后封装成ClientResponse，解析完后存放在list中
            {
                responses.add(new ClientResponse(req, now, false, body));
            }
        }
    }

    public boolean maybeHandleCompletedReceive(ClientRequest req, long now, Struct body) {
        short apiKey = req.request().header().apiKey();
        if (apiKey == ApiKeys.METADATA.id && req.isInitiatedByNetworkClient()) {
//            handleResponse(req.request().header(), body, now);
            return true;
        }
        return false;
    }

    public static Struct parseResponse(ByteBuffer responseBuffer, RequestHeader requestHeader) {
        ResponseHeader responseHeader = ResponseHeader.parse(responseBuffer);
        // Always expect the response version id to be the same as the request version id
        short apiKey = requestHeader.apiKey();
        short apiVer = requestHeader.apiVersion();
        Struct responseBody = ProtoUtils.responseSchema(apiKey, apiVer).read(responseBuffer);
//        Struct responseBody = ProtoUtils.responseSchema(0, 2).read(responseBuffer);
        correlate(requestHeader, responseHeader);
        return responseBody;
    }

    private static void correlate(RequestHeader requestHeader, ResponseHeader responseHeader) {
        if (requestHeader.correlationId() != responseHeader.correlationId()) {
            throw new IllegalStateException("Correlation id for response (" + responseHeader.correlationId()
                    + ") does not match request (" + requestHeader.correlationId() + "), request header: " + requestHeader);
        }
    }

    public void initiateConnect(){
        if(!isConnect){
            try {
                isConnect=true;
                selector.connect(id,new InetSocketAddress(host,port),102400,102400);
            } catch (IOException e) {
                isConnect=false;
                e.printStackTrace();
            }
        }
    }

    public RequestHeader nextRequestHeader(ApiKeys key) {
        return new RequestHeader(key.id, id, correlation++);
    }

    public RequestHeader nextRequestHeader(ApiKeys key, short version) {
        return new RequestHeader(key.id,version, id, correlation++);
    }

    public void wakeup() {
        this.selector.wakeup();
    }

}
