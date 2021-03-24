package com.hxm.consumer;

import com.hxm.producer.TopicPartition;
import com.hxm.protocol.ApiKeys;
import com.hxm.protocol.ProtoUtils;
import com.hxm.protocol.Schema;
import com.hxm.protocol.Struct;
import com.hxm.requests.AbstractRequest;
import com.hxm.requests.AbstractRequestResponse;
import javafx.util.Pair;

import java.util.*;

public class FetchRequest extends AbstractRequest {
    public static final int CONSUMER_REPLICA_ID = -1;
    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentRequestSchema(ApiKeys.FETCH.id);
    private static final String REPLICA_ID_KEY_NAME = "replica_id";
    private static final String MAX_WAIT_KEY_NAME = "max_wait_time";
    private static final String MIN_BYTES_KEY_NAME = "min_bytes";
    private static final String TOPICS_KEY_NAME = "topics";

    // request and partition level name
    private static final String MAX_BYTES_KEY_NAME = "max_bytes";

    // topic level field names
    private static final String TOPIC_KEY_NAME = "topic";
    private static final String PARTITIONS_KEY_NAME = "partitions";

    // partition level field names
    private static final String PARTITION_KEY_NAME = "partition";
    private static final String FETCH_OFFSET_KEY_NAME = "fetch_offset";

    // default values for older versions where a request level limit did not exist
    public static final int DEFAULT_RESPONSE_MAX_BYTES = Integer.MAX_VALUE;

    private static short CurrentVersion=3;
    private static int DefaultCorrelationId=0;
    private final int maxWait;
    private final int replicaId;
    private final int minBytes;
    private final int maxBytes;
    private short versionId=CurrentVersion;
    private int correlationId=DefaultCorrelationId;
    private List<Pair<TopicPartition,PartitionFetchInfo>> requestInfo;
    private final LinkedHashMap<TopicPartition, PartitionData> fetchData;

    @Override
    public AbstractRequestResponse getErrorResponse(int versionId, Throwable e) {
        return null;
    }

    public static final class PartitionData {
        public final long offset;
        public final int maxBytes;

        public PartitionData(long offset, int maxBytes) {
            this.offset = offset;
            this.maxBytes = maxBytes;
        }
    }

    public static final class TopicAndPartitionData<T> {
        public final String topic;
        public final LinkedHashMap<Integer, T> partitions;

        public TopicAndPartitionData(String topic) {
            this.topic = topic;
            this.partitions = new LinkedHashMap<>();
        }

        public static <T> List<TopicAndPartitionData<T>> batchByTopic(LinkedHashMap<TopicPartition, T> data) {
            List<TopicAndPartitionData<T>> topics = new ArrayList<>();
            for (Map.Entry<TopicPartition, T> topicEntry : data.entrySet()) {
                String topic = topicEntry.getKey().topic();
                int partition = topicEntry.getKey().partition();
                T partitionData = topicEntry.getValue();
                if (topics.isEmpty() || !topics.get(topics.size() - 1).topic.equals(topic)) {
                    topics.add(new TopicAndPartitionData(topic));
                }
                topics.get(topics.size() - 1).partitions.put(partition, partitionData);
            }
            return topics;
        }
    }

    public FetchRequest(int maxWait, int minBytes, int maxBytes, LinkedHashMap<TopicPartition, PartitionData> fetchData) {
        this(ProtoUtils.latestVersion(ApiKeys.FETCH.id), CONSUMER_REPLICA_ID, maxWait, minBytes, maxBytes, fetchData);
    }

    private FetchRequest(int version, int replicaId, int maxWait, int minBytes, int maxBytes,
                         LinkedHashMap<TopicPartition, PartitionData> fetchData) {
        super(new Struct(ProtoUtils.requestSchema(ApiKeys.FETCH.id, version)));
        List<TopicAndPartitionData<PartitionData>> topicsData = TopicAndPartitionData.batchByTopic(fetchData);

        struct.set(REPLICA_ID_KEY_NAME, replicaId);
        struct.set(MAX_WAIT_KEY_NAME, maxWait);
        struct.set(MIN_BYTES_KEY_NAME, minBytes);
        if (version >= 3) {
            struct.set(MAX_BYTES_KEY_NAME, maxBytes);
        }
        List<Struct> topicArray = new ArrayList<Struct>();
        for (TopicAndPartitionData<PartitionData> topicEntry : topicsData) {
            Struct topicData = struct.instance(TOPICS_KEY_NAME);
            topicData.set(TOPIC_KEY_NAME, topicEntry.topic);
            List<Struct> partitionArray = new ArrayList<Struct>();
            for (Map.Entry<Integer, PartitionData> partitionEntry : topicEntry.partitions.entrySet()) {
                PartitionData fetchPartitionData = partitionEntry.getValue();
                Struct partitionData = topicData.instance(PARTITIONS_KEY_NAME);
                partitionData.set(PARTITION_KEY_NAME, partitionEntry.getKey());
                partitionData.set(FETCH_OFFSET_KEY_NAME, fetchPartitionData.offset);
                partitionData.set(MAX_BYTES_KEY_NAME, fetchPartitionData.maxBytes);
                partitionArray.add(partitionData);
            }
            topicData.set(PARTITIONS_KEY_NAME, partitionArray.toArray());
            topicArray.add(topicData);
        }
        struct.set(TOPICS_KEY_NAME, topicArray.toArray());
        this.replicaId = replicaId;
        this.maxWait = maxWait;
        this.minBytes = minBytes;
        this.maxBytes = maxBytes;
        this.fetchData = fetchData;
    }

    public static Map<String, List<Pair<Integer,FetchResponsePartitionData>>> batchByTopic(List<Pair<TopicPartition,FetchResponsePartitionData>> datas){
        Map<String,List<Pair<Integer,FetchResponsePartitionData>>> result=new HashMap<>();
        for(Pair<TopicPartition,FetchResponsePartitionData> data:datas){
            TopicPartition tp=data.getKey();
            String topic=tp.topic();
            int partition=tp.partition();
            List<Pair<Integer,FetchResponsePartitionData>> value=result.get(tp.topic());
            if(result.isEmpty() || value==null){
                result.put(topic,new ArrayList<>());
            }
            result.get(topic).add(new Pair<>(partition,data.getValue()));
        }

        return result;
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

    public List<Pair<TopicPartition, PartitionFetchInfo>> getRequestInfo() {
        return requestInfo;
    }

    public LinkedHashMap<TopicPartition, PartitionData> fetchData() {
        return fetchData;
    }
}
