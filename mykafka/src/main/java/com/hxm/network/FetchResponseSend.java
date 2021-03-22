package com.hxm.network;

import com.hxm.consumer.FetchResponse;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

public class FetchResponseSend implements Send {

    private final String dest;
    private final FetchResponse fetchResponse;
    private ByteBuffer emptyBuffer;
    private int payloadSize;
    private long sent=0L;
    private boolean pending=false;
    private ByteBuffer buffer;



    public FetchResponseSend(String dest, FetchResponse fetchResponse) {
        this.dest = dest;
        this.fetchResponse = fetchResponse;
        this.emptyBuffer=ByteBuffer.allocate(0);
        this.payloadSize = fetchResponse.sizeInBytes();
        this.buffer = ByteBuffer.allocate(4 /* for size */ + fetchResponse.headerSizeInBytes());
        fetchResponse.writeHeaderTo(buffer);
        this.buffer.rewind();
    }

    @Override
    public String destination() {
        return dest;
    }

    @Override
    public boolean completed() {
        return sent >= size() && !pending;
    }

    @Override
    public long writeTo(GatheringByteChannel channel) throws IOException {
        return 0;
    }

    @Override
    public long size() {
        return 4 /* for size byte */ + payloadSize;
    }
}
