package com.hxm.client.client;

import com.hxm.client.common.protocol.Struct;

public class ClientResponse {
    private final long receivedTimeMs;
    private final boolean disconnected;
    private final ClientRequest request;
    private final Struct responseBody;

    public ClientResponse(ClientRequest request, long receivedTimeMs, boolean disconnected, Struct responseBody) {
        super();
        this.receivedTimeMs = receivedTimeMs;
        this.disconnected = disconnected;
        this.request = request;
        this.responseBody = responseBody;
    }

    public ClientRequest request() {
        return request;
    }

    @Override
    public String toString() {
        return "ClientResponse(receivedTimeMs=" + receivedTimeMs +
                ", disconnected=" +
                disconnected +
                ", request=" +
                request +
                ", responseBody=" +
                responseBody +
                ")";
    }

    public Struct responseBody() {
        return responseBody;
    }
}
