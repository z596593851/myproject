package com.hxm.network;

import com.hxm.protocol.Struct;
import com.hxm.requests.RequestHeader;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

public class RequestSend extends NetworkSend {
    private RequestHeader header;
    private Struct body;

    public RequestSend(String destination, RequestHeader header, Struct body) {
        super(destination, serialize(header, body));
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

    public RequestHeader header() {
        return this.header;
    }

    public Struct body() {
        return body;
    }

    @Override
    public String toString() {
        return "RequestSend(header=" + header.toString() + ", body=" + body.toString() + ")";
    }
}
