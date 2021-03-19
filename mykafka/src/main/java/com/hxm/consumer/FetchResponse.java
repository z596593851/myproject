package com.hxm.consumer;

import com.hxm.producer.TopicPartition;

public class FetchResponse {
    private final int correlationId;
    private final TopicPartition tp;
    private final FetchResponsePartitionData data;
    private final int requestVersion;
    private final int throttleTimeMs;

    public FetchResponse(int correlationId, TopicPartition tp, FetchResponsePartitionData data, int requestVersion, int throttleTimeMs) {
        this.correlationId = correlationId;
        this.tp = tp;
        this.data = data;
        this.requestVersion = requestVersion;
        this.throttleTimeMs = throttleTimeMs;
    }
}
