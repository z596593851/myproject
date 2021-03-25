package com.hxm.consumer;

import com.hxm.protocol.Struct;

public class ClientResponse {
    private final long receivedTimeMs;
    private final boolean disconnected;
    private final ClientRequest request;
    private final Struct responseBody;

    /**
     * @param request The original request
     * @param receivedTimeMs The unix timestamp when this response was received
     * @param disconnected Whether the client disconnected before fully reading a response
     * @param responseBody The response contents (or null) if we disconnected or no response was expected
     */
    public ClientResponse(ClientRequest request, long receivedTimeMs, boolean disconnected, Struct responseBody) {
        super();
        this.receivedTimeMs = receivedTimeMs;
        this.disconnected = disconnected;
        this.request = request;
        this.responseBody = responseBody;
    }

    public long receivedTimeMs() {
        return receivedTimeMs;
    }

    public boolean wasDisconnected() {
        return disconnected;
    }

    public ClientRequest request() {
        return request;
    }

    public Struct responseBody() {
        return responseBody;
    }

    public boolean hasResponse() {
        return responseBody != null;
    }

    public long requestLatencyMs() {
        return receivedTimeMs() - this.request.createdTimeMs();
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
}
