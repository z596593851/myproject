package com.hxm.test;

import com.hxm.network.RequestSend;

public class ClientRequest {
    private RequestSend request;
    private RequestCompletionHandler callback;
    private long createdTimeMs;
    private boolean expectResponse;
    private boolean isInitiatedByNetworkClient;

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
}
