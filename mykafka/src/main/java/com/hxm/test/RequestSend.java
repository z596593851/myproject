package com.hxm.test;

import com.hxm.protocol.Struct;
import com.hxm.requests.RequestHeader;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

public class RequestSend {
//    private String request;
    private ByteBuffer[] buffers;
    private String destination;
    private int size;
    private int remaining;
    private boolean pending = false;
    private RequestHeader header;
    private Struct body;

    public RequestSend(String destination, ByteBuffer... buffers) {
        this.buffers = buffers;
        this.destination = destination;
        for (int i = 0; i < buffers.length; i++) {
            remaining += buffers[i].remaining();
        }
        this.size = remaining;
    }

    public RequestSend(String destination, RequestHeader header, Struct body) {
        this.destination=destination;
        this.buffers=sizeDelimit(serialize(header, body));
        this.header = header;
        this.body = body;
    }

    public static ByteBuffer serialize(RequestHeader header, Struct body) {
        ByteBuffer buffer = ByteBuffer.allocate(header.sizeOf() + body.sizeOf());
        header.writeTo(buffer);
        body.writeTo(buffer);
        buffer.rewind();
        return buffer;
    }

    private static ByteBuffer[] sizeDelimit(ByteBuffer... buffers) {
        int size = 0;
        for (int i = 0; i < buffers.length; i++) {
            size += buffers[i].remaining();
        }
        ByteBuffer[] delimited = new ByteBuffer[buffers.length + 1];
        delimited[0] = ByteBuffer.allocate(4);
        delimited[0].putInt(size);
        delimited[0].rewind();
        System.arraycopy(buffers, 0, delimited, 1, buffers.length);
        return delimited;
    }

    public String destination() {
        return this.destination;
    }

//    public ByteBuffer request(){
//        return buffers;
//    }

    public long writeTo(GatheringByteChannel channel) throws IOException {
        long written = channel.write(buffers);
        if (written < 0) {
            throw new EOFException("Wrote negative bytes to channel. This shouldn't happen.");
        }
        remaining -= written;
        return written;
    }

    public boolean completed() {
        return remaining <= 0 && !pending;
    }

    public long size() {
        return this.size;
    }
}
