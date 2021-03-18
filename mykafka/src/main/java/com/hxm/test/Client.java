package com.hxm.test;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Client {
    //private Client client=new Client();

    private final Map<String,List<ClientRequest>> unsent = new HashMap<>();
    private final List<RequestSend> completedSends=new ArrayList<>();
    private final Map<String,Deque<ClientRequest>> inFlightRequests=new HashMap<>();
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

    public RequestFuture<ClientResponse> send(String destination,String request){
        RequestFutureCompletionHandler completionHandler=new RequestFutureCompletionHandler();
        put(new ClientRequest(new RequestSend(destination, ByteBuffer.wrap(request.getBytes())),completionHandler));
        return completionHandler.future;
    }

    private void put(ClientRequest request){
        if(unsent.get(request.request().destination())!=null){
            unsent.get(request.request().destination()).add(request);
        }else {
            List<ClientRequest> list=new ArrayList<>(Collections.singletonList(request));
            unsent.put(request.request().destination(),list);
        }
    }

    public void poll(RequestFuture<?> future){
        while (!future.isDone()){
            firePendingCompletedRequests();
            //模拟处理unsent中的请求：往inFlightRequests里添加，且往channle上绑定
            for(Map.Entry<String, List<ClientRequest>> requestEntry: unsent.entrySet()){
                Iterator<ClientRequest> iterator = requestEntry.getValue().iterator();
                while (iterator.hasNext()) {
                    ClientRequest request = iterator.next();
                    Deque<ClientRequest> reqs = inFlightRequests.get(request.request().destination());
                    if (reqs == null) {
                        reqs = new ArrayDeque<>();
                        inFlightRequests.put(request.request().destination(), reqs);
                    }
                    reqs.addFirst(request);
                    channelList.add(request.request());
                    iterator.remove();
                }
            }
            completedSends.clear();
            //模拟selecotr发消息
            for(RequestSend send:channelList){
//                System.out.println(send.request());
                this.completedSends.add(send);
            }
            channelList.clear();
            //模拟接到响应
            List<ClientResponse> responses = new ArrayList<>();
            for (RequestSend send : completedSends) {
                ClientRequest request = this.inFlightRequests.get(send.destination()).peekFirst();
                if (request!=null) {
                    this.inFlightRequests.get(send.destination()).pollFirst();
                    responses.add(new ClientResponse(request));
                }
            }
            //处理回调
            for (ClientResponse response : responses) {
                response.request().callback().onComplete(response);
            }
            firePendingCompletedRequests();
        }
    }

    private void firePendingCompletedRequests() {
        for (;;) {
            RequestFutureCompletionHandler completionHandler = pendingCompletion.poll();
            if (completionHandler == null) {
                break;
            }
            completionHandler.fireCompletion();
        }
    }



//    public void test(){
//        String request1="request-test1";
//        //String request2="request-test2";
//        RequestFuture<Void> requestFuture1 = client.send(request1).compose(new Test2ResponseHandler());
//        client.poll(requestFuture1);
//    }

    public static void main(String[] args) {
//        String request1="request-test1";
//        String request2="request-test2";
        //Client client=new Client();
//        //使用适配器添加回调函数
//        RequestFuture<Void> requestFuture1 = client.send(request1).compose(new Test1ResponseHandler());
//        RequestFuture<Void> requestFuture2 = client.send(request2).compose(new Test1ResponseHandler());
//        requestFuture1.chain(requestFuture2);
        //直接添加回调函数
//        client.send(request2).addListener(new RequestFutureListener<ClientResponse>() {
//            @Override
//            public void onSuccess(ClientResponse response) {
//                System.out.println("回应"+response.request().request().request());
//            }
//
//            @Override
//            public void onFailure(RuntimeException e) {
//
//            }
//        });
//        client.poll(requestFuture1);
    }

}
