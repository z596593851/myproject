package com.hxm.client.common.requests;

import static com.hxm.client.common.protocol.Protocol.REQUEST_HEADER;

import com.hxm.client.common.protocol.Field;
import com.hxm.client.common.protocol.ProtoUtils;
import com.hxm.client.common.protocol.Protocol;
import com.hxm.client.common.protocol.Struct;
import java.nio.ByteBuffer;


/**
 * The header for a request in the Kafka protocol
 */
public class RequestHeader extends AbstractRequestResponse {

    private static final Field API_KEY_FIELD = REQUEST_HEADER.get("api_key");
    private static final Field API_VERSION_FIELD = REQUEST_HEADER.get("api_version");
    private static final Field CLIENT_ID_FIELD = REQUEST_HEADER.get("client_id");
    private static final Field CORRELATION_ID_FIELD = REQUEST_HEADER.get("correlation_id");

    private final short apiKey;
    private final short apiVersion;
    private final String clientId;
    private final int correlationId;

    public RequestHeader(Struct header) {
        super(header);
        apiKey = struct.getShort(API_KEY_FIELD);
        apiVersion = struct.getShort(API_VERSION_FIELD);
        clientId = struct.getString(CLIENT_ID_FIELD);
        correlationId = struct.getInt(CORRELATION_ID_FIELD);
    }

    public RequestHeader(short apiKey, String client, int correlation) {
        this(apiKey, ProtoUtils.latestVersion(apiKey), client, correlation);
    }

    public RequestHeader(short apiKey, short version, String client, int correlation) {
        super(new Struct(Protocol.REQUEST_HEADER));
        struct.set(API_KEY_FIELD, apiKey);
        struct.set(API_VERSION_FIELD, version);
        struct.set(CLIENT_ID_FIELD, client);
        struct.set(CORRELATION_ID_FIELD, correlation);
        this.apiKey = apiKey;
        this.apiVersion = version;
        this.clientId = client;
        this.correlationId = correlation;
    }

    public short apiKey() {
        return apiKey;
    }

    public short apiVersion() {
        return apiVersion;
    }

    public String clientId() {
        return clientId;
    }

    public int correlationId() {
        return correlationId;
    }

    public static RequestHeader parse(ByteBuffer buffer) {
        return new RequestHeader(Protocol.REQUEST_HEADER.read(buffer));
    }
}
