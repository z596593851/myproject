package com.hxm.consumer;

import com.hxm.producer.Time;

public class Fetcher {
    private final ConsumerNetworkClient client;
    private final Time time;
    private final int minBytes;
    private final int maxBytes;
    private final int maxWaitMs;
    private final int fetchSize;
    private final int maxPollRecords;

    public Fetcher(ConsumerNetworkClient client, Time time, int minBytes, int maxBytes, int maxWaitMs, int fetchSize, int maxPollRecords) {
        this.client = client;
        this.time = time;
        this.minBytes = minBytes;
        this.maxBytes = maxBytes;
        this.maxWaitMs = maxWaitMs;
        this.fetchSize = fetchSize;
        this.maxPollRecords = maxPollRecords;
    }

    public void sendFetches(){

    }

    public void createFetchRequests(){

    }


}
