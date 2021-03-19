package com.hxm.network;

import com.hxm.consumer.FetchResponse;

import java.io.IOException;
import java.nio.channels.GatheringByteChannel;

public class FetchResponseSend implements Send{

    private final String dest;
    private final FetchResponse fetchResponse;

    public FetchResponseSend(String dest, FetchResponse fetchResponse) {
        this.dest = dest;
        this.fetchResponse = fetchResponse;
    }

    @Override
    public String destination() {
        return null;
    }

    @Override
    public boolean completed() {
        return false;
    }

    @Override
    public long writeTo(GatheringByteChannel channel) throws IOException {
        return 0;
    }

    @Override
    public long size() {
        return 0;
    }
}
