package com.hxm.test;

public class ClientResponse {
    private final ClientRequest request;

    public ClientResponse(ClientRequest request) {
        this.request = request;
    }
    public ClientRequest request() {
        return request;
    }
}
