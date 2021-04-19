package com.hxm.core.api;

import com.hxm.client.common.utils.Utils;
import com.hxm.client.common.TopicPartition;
import javafx.util.Pair;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * server端的FetchRequest（由scala翻译而来）
 */
public class FetchRequest {
    private short versionId=3;
    private int correlationId=0;
    private String clientId="";
    private int replicaId=1;
    private int maxWait=0;
    private int minBytes=0;
    private int maxBytes=Integer.MAX_VALUE;
    private List<Pair<TopicPartition, PartitionFetchInfo>> requestInfo;

    public FetchRequest(short versionId, int correlationId, String clientId, int replicaId, int maxWait, int minBytes, int maxBytes, List<Pair<TopicPartition, PartitionFetchInfo>> requestInfo) {
        this.versionId = versionId;
        this.correlationId = correlationId;
        this.clientId = clientId;
        this.replicaId = replicaId;
        this.maxWait = maxWait;
        this.minBytes = minBytes;
        this.maxBytes = maxBytes;
        this.requestInfo = requestInfo;
    }

    public static FetchRequest readFrom(ByteBuffer buffer){
        short versionId = buffer.getShort();
        int correlationId = buffer.getInt();
        String clientId = Utils.readShortString(buffer);
        int replicaId = buffer.getInt();
        int maxWait = buffer.getInt();
        int minBytes = buffer.getInt();
        int maxBytes = versionId < 3? Integer.MAX_VALUE : buffer.getInt();
        int topicCount = buffer.getInt();
        List<Pair<TopicPartition,PartitionFetchInfo>> pairs=new ArrayList<>();
        while(topicCount-- >0){
            String topic=Utils.readShortString(buffer);
            int partitionCount=buffer.getInt();
            while(partitionCount-- >0){
                int partitionId = buffer.getInt();
                long offset = buffer.getLong();
                int fetchSize = buffer.getInt();
                pairs.add(new Pair<>(new TopicPartition(topic,partitionId),new PartitionFetchInfo(offset,fetchSize)));
            }
        }
        System.out.printf("Fetchrequest: versionId-%d, correlationId-%d%n",versionId,correlationId);

        return new FetchRequest(versionId, correlationId, clientId, replicaId, maxWait, minBytes, maxBytes,pairs);
    }

    public short getVersionId() {
        return versionId;
    }

    public int getCorrelationId() {
        return correlationId;
    }

    public List<Pair<TopicPartition, PartitionFetchInfo>> getRequestInfo() {
        return requestInfo;
    }

    public String getClientId() {
        return clientId;
    }

    public int getReplicaId() {
        return replicaId;
    }

    public int getMaxWait() {
        return maxWait;
    }

    public int getMinBytes() {
        return minBytes;
    }

    public int getMaxBytes() {
        return maxBytes;
    }
}
