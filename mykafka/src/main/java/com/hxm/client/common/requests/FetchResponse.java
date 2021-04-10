package com.hxm.client.common.requests;

import com.hxm.client.common.TopicPartition;
import com.hxm.client.common.protocol.ApiKeys;
import com.hxm.client.common.protocol.ProtoUtils;
import com.hxm.client.common.protocol.Schema;
import com.hxm.client.common.protocol.Struct;
import com.hxm.client.common.requests.AbstractRequestResponse;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;

/**
 * consumer端的FetchResponse（java版）
 */
public class FetchResponse extends AbstractRequestResponse {
    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentResponseSchema(ApiKeys.FETCH.id);
    private static final String RESPONSES_KEY_NAME = "responses";

    // topic level field names
    private static final String TOPIC_KEY_NAME = "topic";
    private static final String PARTITIONS_KEY_NAME = "partition_responses";
    private static final String THROTTLE_TIME_KEY_NAME = "throttle_time_ms";

    // partition level field names
    private static final String PARTITION_KEY_NAME = "partition";
    private static final String ERROR_CODE_KEY_NAME = "error_code";

    // Default throttle time
    private static final int DEFAULT_THROTTLE_TIME = 0;
    private static final String HIGH_WATERMARK_KEY_NAME = "high_watermark";
    private static final String RECORD_SET_KEY_NAME = "record_set";

    private final LinkedHashMap<TopicPartition, PartitionData> responseData;
    private final int throttleTime;

    public static final class PartitionData {
        public final short errorCode;
        public final long highWatermark;
        public final ByteBuffer recordSet;

        public PartitionData(short errorCode, long highWatermark, ByteBuffer recordSet) {
            this.errorCode = errorCode;
            this.highWatermark = highWatermark;
            this.recordSet = recordSet;
        }
    }

    public FetchResponse(Struct struct) {
        super(struct);
        LinkedHashMap<TopicPartition, PartitionData> responseData = new LinkedHashMap<>();
        for (Object topicResponseObj : struct.getArray(RESPONSES_KEY_NAME)) {
            Struct topicResponse = (Struct) topicResponseObj;
            String topic = topicResponse.getString(TOPIC_KEY_NAME);
            for (Object partitionResponseObj : topicResponse.getArray(PARTITIONS_KEY_NAME)) {
                Struct partitionResponse = (Struct) partitionResponseObj;
                int partition = partitionResponse.getInt(PARTITION_KEY_NAME);
                //todo
                short errorCode = partitionResponse.getShort(ERROR_CODE_KEY_NAME);
                long highWatermark = partitionResponse.getLong(HIGH_WATERMARK_KEY_NAME);
                ByteBuffer recordSet = partitionResponse.getBytes(RECORD_SET_KEY_NAME);
                PartitionData partitionData = new PartitionData(errorCode, highWatermark, recordSet);
//                PartitionData partitionData = new PartitionData((short)0, 0L, recordSet);
                responseData.put(new TopicPartition(topic, partition), partitionData);
            }
        }
        this.responseData = responseData;
        this.throttleTime = struct.hasField(THROTTLE_TIME_KEY_NAME) ? struct.getInt(THROTTLE_TIME_KEY_NAME) : DEFAULT_THROTTLE_TIME;
    }

    public LinkedHashMap<TopicPartition, PartitionData> responseData() {
        return responseData;
    }

}
