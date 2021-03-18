package com.hxm.requests;

import com.hxm.protocol.ApiKeys;
import com.hxm.protocol.Struct;

import java.nio.ByteBuffer;

public abstract class AbstractRequest extends AbstractRequestResponse {

    public AbstractRequest(Struct struct) {
        super(struct);
    }

    /**
     * Get an error response for a request for a given api version
     */
    public abstract AbstractRequestResponse getErrorResponse(int versionId, Throwable e);

    /**
     * Factory method for getting a request object based on ApiKey ID and a buffer
     */
    public static AbstractRequest getRequest(int requestId, int versionId, ByteBuffer buffer) {
        ApiKeys apiKey = ApiKeys.forId(requestId);
        switch (apiKey) {
            case PRODUCE:
                return ProduceRequest.parse(buffer, versionId);
            default:
                throw new AssertionError(String.format("ApiKey %s is not currently handled in `getRequest`, the " +
                        "code should be updated to do so.", apiKey));
        }
    }
}
