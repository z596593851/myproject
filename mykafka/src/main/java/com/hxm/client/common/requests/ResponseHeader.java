package com.hxm.client.common.requests;

import com.hxm.client.common.protocol.Field;
import com.hxm.client.common.protocol.Protocol;
import com.hxm.client.common.protocol.Struct;
import static com.hxm.client.common.protocol.Protocol.RESPONSE_HEADER;

import java.nio.ByteBuffer;

public class ResponseHeader extends AbstractRequestResponse {
    private static final Field CORRELATION_KEY_FIELD = RESPONSE_HEADER.get("correlation_id");

    private final int correlationId;

    public ResponseHeader(Struct header) {
        super(header);
        correlationId = struct.getInt(CORRELATION_KEY_FIELD);
    }

    public ResponseHeader(int correlationId) {
        super(new Struct(Protocol.RESPONSE_HEADER));
        struct.set(CORRELATION_KEY_FIELD, correlationId);
        this.correlationId = correlationId;
    }

    public int correlationId() {
        return correlationId;
    }

    public static ResponseHeader parse(ByteBuffer buffer) {
        return new ResponseHeader(Protocol.RESPONSE_HEADER.read(buffer));
    }
}
