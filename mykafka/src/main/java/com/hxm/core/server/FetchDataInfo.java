package com.hxm.core.server;

import com.hxm.core.message.MessageSet;

public class FetchDataInfo {
    private LogOffsetMetadata fetchOffsetMetadata;
    private MessageSet messageSet;
    private boolean firstMessageSetIncomplete;

    public FetchDataInfo(LogOffsetMetadata fetchOffsetMetadata, MessageSet messageSet, boolean firstMessageSetIncomplete) {
        this.fetchOffsetMetadata = fetchOffsetMetadata;
        this.messageSet = messageSet;
        this.firstMessageSetIncomplete = firstMessageSetIncomplete;
    }

    public MessageSet getMessageSet() {
        return messageSet;
    }

    public LogOffsetMetadata getFetchOffsetMetadata() {
        return fetchOffsetMetadata;
    }

    public boolean isFirstMessageSetIncomplete() {
        return firstMessageSetIncomplete;
    }
}
