package com.hxm.consumer;

import com.hxm.network.RequestSend;

public class ClientRequest {
    private RequestSend request;
    private RequestCompletionHandler callback;
    private long createdTimeMs;
    private boolean expectResponse;
    private boolean isInitiatedByNetworkClient;
    private long sendTimeMs;

    public ClientRequest( RequestSend request, RequestCompletionHandler callback) {
        this.request = request;
        this.callback = callback;
    }

    public ClientRequest(long createdTimeMs, boolean expectResponse, RequestSend request,
                         RequestCompletionHandler callback, boolean isInitiatedByNetworkClient) {
        this.createdTimeMs = createdTimeMs;
        this.callback = callback;
        this.request = request;
        this.expectResponse = expectResponse;
        this.isInitiatedByNetworkClient = isInitiatedByNetworkClient;
    }

    public ClientRequest(long createdTimeMs, boolean expectResponse, RequestSend request,
                         RequestCompletionHandler callback) {
        this(createdTimeMs, expectResponse, request, callback, false);
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

    public long sendTimeMs() {
        return sendTimeMs;
    }

    public void setSendTimeMs(long sendTimeMs) {
        this.sendTimeMs = sendTimeMs;
    }

    public boolean expectResponse() {
        return expectResponse;
    }

    public boolean isInitiatedByNetworkClient() {
        return isInitiatedByNetworkClient;
    }

    public boolean hasCallback() {
        return callback != null;
    }
}
