package com.hxm.network;

import com.hxm.consumer.FetchResponsePartitionData;

import java.io.IOException;
import java.nio.channels.GatheringByteChannel;

public class PartitionDataSend implements Send{
    private final int partitionId;
    private final FetchResponsePartitionData partitionData;

    public PartitionDataSend(int partitionId, FetchResponsePartitionData partitionData) {
        this.partitionId = partitionId;
        this.partitionData = partitionData;
    }

    @Override
    public String destination() {
        return null;
    }

    @Override
    public boolean completed() {
        return false;
    }

    @Override
    public long writeTo(GatheringByteChannel channel) throws IOException {
        return 0;
    }

    @Override
    public long size() {
        return 0;
    }
}
