package com.hxm.broker;

import com.hxm.requests.AbstractRequest;
import com.hxm.requests.RequestHeader;
import com.hxm.test.RequestSend;

import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class RequestChannel {

    private ArrayBlockingQueue<Request> requestQueue;
    private BlockingQueue<Response>[]  responseQueues;

    enum ResponseAction{
        NoOpAction,
        SendAction,
        CloseConnectionAction
    }

    static class Request{
        private RequestHeader header;
        private AbstractRequest body;
        private int processor;
        public String connectionId;
        private short requestId;

//        private ByteBuffer buffer;
        public Request(int processor,String connectionId,ByteBuffer buffer){
            this.processor=processor;
            this.connectionId=connectionId;
            this.requestId=buffer.getShort();
            parse(buffer);

        }

        public void parse(ByteBuffer buffer){
            buffer.rewind();
            this.header=RequestHeader.parse(buffer);
            this.body=AbstractRequest.getRequest(header.apiKey(), header.apiVersion(), buffer);
        }

        public int processor(){
            return this.processor;
        }

        public short getRequestId(){
            return this.requestId;
        }

        public AbstractRequest getBody(){
            return this.body;
        }

//        public ByteBuffer getBuffer() {
//            return buffer;
//        }
    }

    static class Response{
        public int processor;
        public Request request;
        public RequestSend send;
        public ResponseAction responseAction;
        public Response(int processor, Request request, RequestSend send,ResponseAction responseAction){
            this.processor=processor;
            this.request=request;
            this.send=send;
            this.responseAction=responseAction;
        }

    }


    public RequestChannel(int numProcessors, int queueSize){
        this.requestQueue = new ArrayBlockingQueue<>(queueSize);
        this.responseQueues=new BlockingQueue[numProcessors];
        for(int i=0; i<numProcessors; i++){
            responseQueues[i]=new LinkedBlockingQueue<>();
        }
    }

    public void sendRequest(Request request){
        try {
            requestQueue.put(request);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void sendResponse(Response response){
        try {
            responseQueues[response.processor].put(response);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public Request receiveRequest(Long timeout){
        try {
            return requestQueue.poll(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    public Response receiveResponse(int processor){
        return this.responseQueues[processor].poll();
    }
}
