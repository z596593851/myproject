package com.hxm.client.client.consumer.internals;


import com.hxm.client.client.ClientRequest;
import com.hxm.client.client.ClientResponse;
import com.hxm.client.client.NetworkClient;
import com.hxm.client.client.RequestCompletionHandler;
import com.hxm.client.common.Node;
import com.hxm.client.common.protocol.ApiKeys;
import com.hxm.client.common.protocol.ProtoUtils;
import com.hxm.client.common.requests.AbstractRequest;
import com.hxm.client.common.requests.RequestHeader;
import com.hxm.client.common.requests.RequestSend;
import com.hxm.client.common.utils.Time;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ConsumerNetworkClient {
    private static final long MAX_POLL_TIMEOUT_MS = 5000L;
    private final NetworkClient client;
    private final Time time;

    public ConsumerNetworkClient(NetworkClient client) {
        this.client = client;
        this.time=new Time();
    }

    private final Map<Node, List<ClientRequest>> unsent = new HashMap<>();
    private final List<RequestSend> completedSends=new ArrayList<>();
    private final Map<String, Deque<ClientRequest>> inFlightRequests=new HashMap<>();
    private final List<RequestSend> channelList=new ArrayList<>();
    private final ConcurrentLinkedQueue<RequestFutureCompletionHandler> pendingCompletion = new ConcurrentLinkedQueue<>();

    public class RequestFutureCompletionHandler implements RequestCompletionHandler {
        private final RequestFuture<ClientResponse> future;
        private ClientResponse response;
        private RuntimeException e;

        public RequestFutureCompletionHandler() {
            this.future = new RequestFuture<>();
        }

        public void fireCompletion() {
            if (e != null) {
                future.raise(e);
            }else {
                future.complete(response);
            }
        }

        @Override
        public void onComplete(ClientResponse response){
            this.response=response;
            pendingCompletion.add(this);
        }
    }

    public RequestFuture<ClientResponse> send(Node node,
                                              ApiKeys api,
                                              AbstractRequest request) {
        return send(node, api, ProtoUtils.latestVersion(api.id), request);
    }

    private RequestFuture<ClientResponse> send(Node node,
                                               ApiKeys api,
                                               short version,
                                               AbstractRequest request) {

        long now = time.milliseconds();
        RequestFutureCompletionHandler completionHandler = new RequestFutureCompletionHandler();
        RequestHeader header = client.nextRequestHeader(api,version);
        RequestSend send = new RequestSend(node.idString(), header, request.toStruct());
        //将待发送的请求封装成ClientRequest并保存到unsent中等待发送
        put(node, new ClientRequest(now, true, send, completionHandler));

        // wakeup the client in case it is blocking in poll so that we can send the queued request
        client.wakeup();
        return completionHandler.future;

    }

    private void put(Node node,ClientRequest request){
        List<ClientRequest> nodeUnsent = unsent.get(node);
        if (nodeUnsent == null) {
            nodeUnsent = new ArrayList<>();
            unsent.put(node, nodeUnsent);
        }
        nodeUnsent.add(request);
    }

    /**
     * 阻塞式调用直到成功
     * @param future
     */
    public void poll(RequestFuture<?> future){
        while (!future.isDone()) {
            poll(MAX_POLL_TIMEOUT_MS, time.milliseconds(), future);
        }
    }

    //暂时让selecotr取消调connect事件
    public void poll(){
        client.poll(0);
    }

    public void pollNoWakeup() {
        poll(0, time.milliseconds(), null);
    }

    public void poll(long timeout, long now, PollCondition pollCondition) {
        firePendingCompletedRequests();
        trySend(now);
        if (pollCondition == null || pollCondition.shouldBlock()) {
            client.poll(timeout);
        }
        firePendingCompletedRequests();
    }


    private boolean trySend(long now) {
        // send any requests that can be sent now
        boolean requestsSent = false;
        //遍历unsent中缓存的请求
        for (Map.Entry<Node, List<ClientRequest>> requestEntry: unsent.entrySet()) {
            Node node = requestEntry.getKey();
            Iterator<ClientRequest> iterator = requestEntry.getValue().iterator();
            while (iterator.hasNext()) {
                ClientRequest request = iterator.next();
                //检查网络连接
                if(client.ready(node, now)){
                    //同producer一样，调用send将请求放入inFlightRequests中
                    //同时往selector绑定write事件准备发送
                    client.send(request);
                    iterator.remove();
                    requestsSent = true;
                }
            }
        }
        return requestsSent;
    }

    private void firePendingCompletedRequests() {
        boolean completedRequestsFired = false;
        for (;;) {
            RequestFutureCompletionHandler completionHandler = pendingCompletion.poll();
            if (completionHandler == null) {
                break;
            }
            completionHandler.fireCompletion();
            completedRequestsFired = true;
        }
        // wakeup the client in case it is blocking in poll for this future's completion
        if (completedRequestsFired) {
            client.wakeup();
        }
    }

    public interface PollCondition {
        /**
         * Return whether the caller is still awaiting an IO event.
         * @return true if so, false otherwise.
         */
        boolean shouldBlock();
    }
}
