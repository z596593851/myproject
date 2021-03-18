package com.hxm.consumer;

public class PartitionFetchInfo {
    private final long offset;
    private final int fetchSize;

    public PartitionFetchInfo(long offset, int fetchSize) {
        this.offset = offset;
        this.fetchSize = fetchSize;
    }

    public long getOffset() {
        return offset;
    }

    public int getFetchSize() {
        return fetchSize;
    }
}
