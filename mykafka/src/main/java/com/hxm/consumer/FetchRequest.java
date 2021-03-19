package com.hxm.consumer;

public class FetchRequest {
    private static short CurrentVersion=3;
    private static int DefaultCorrelationId=0;
    private final int maxWait;
    private final int replicaId;
    private final int minBytes;
    private final int maxBytes;
    private short versionId=CurrentVersion;
    private int correlationId=DefaultCorrelationId;

    public FetchRequest(int maxWait, int replicaId, int minBytes, int maxBytes) {
        this.maxWait = maxWait;
        this.replicaId = replicaId;
        this.minBytes = minBytes;
        this.maxBytes = maxBytes;
    }

    public int getCorrelationId() {
        return correlationId;
    }

    public int getMaxWait() {
        return maxWait;
    }

    public int getReplicaId() {
        return replicaId;
    }

    public int getMinBytes() {
        return minBytes;
    }

    public int getMaxBytes() {
        return maxBytes;
    }

    public short getVersionId() {
        return versionId;
    }
}
