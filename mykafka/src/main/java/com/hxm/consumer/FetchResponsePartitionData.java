package com.hxm.consumer;

import com.hxm.message.MessageSet;

public class FetchResponsePartitionData {
    private final long hw;
    private final MessageSet messages;

    public FetchResponsePartitionData(long hw, MessageSet messages) {
        this.hw = hw;
        this.messages = messages;
    }
}
