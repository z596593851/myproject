package com.hxm.network;

import com.hxm.protocol.Struct;
import com.hxm.requests.AbstractRequestResponse;
import com.hxm.requests.ResponseHeader;

import java.nio.ByteBuffer;

public class ResponseSend extends NetworkSend {
    public ResponseSend(String destination, ResponseHeader header, Struct body) {
        super(destination, serialize(header, body));
    }

    public ResponseSend(String destination, ResponseHeader header, AbstractRequestResponse response) {
        this(destination, header, response.toStruct());
    }

    public static ByteBuffer serialize(ResponseHeader header, Struct body) {
        ByteBuffer buffer = ByteBuffer.allocate(header.sizeOf() + body.sizeOf());
        header.writeTo(buffer);
        body.writeTo(buffer);
        buffer.rewind();
        return buffer;
    }
}
