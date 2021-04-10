package com.hxm.client.client;

import com.hxm.client.common.requests.RequestSend;

public class ClientRequest {
    private RequestSend request;
    private RequestCompletionHandler callback;
    private long createdTimeMs;
    private boolean expectResponse;
    private boolean isInitiatedByNetworkClient;
    private long sendTimeMs;

    public ClientRequest(long createdTimeMs, boolean expectResponse, RequestSend request,
                         RequestCompletionHandler callback, boolean isInitiatedByNetworkClient) {
        this.createdTimeMs = createdTimeMs;
        this.callback = callback;
        this.request = request;
        this.expectResponse = expectResponse;
        this.isInitiatedByNetworkClient = isInitiatedByNetworkClient;
    }
    public ClientRequest(long createdTimeMs, boolean expectResponse, RequestSend request,
                         RequestCompletionHandler callback){
        this(createdTimeMs,expectResponse,request,callback,false);
    }

    public RequestSend request(){
        return this.request;
    }

    public RequestCompletionHandler callback() {
        return callback;
    }

    public long createdTimeMs() {
        return createdTimeMs;
    }

    public boolean hasCallback() {
        return callback != null;
    }

    public boolean expectResponse() {
        return expectResponse;
    }

    public long sendTimeMs() {
        return sendTimeMs;
    }

}
