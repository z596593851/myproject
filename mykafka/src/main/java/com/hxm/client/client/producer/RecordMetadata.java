package com.hxm.client.client.producer;

import com.hxm.client.common.TopicPartition;

public class RecordMetadata {
    public static final int UNKNOWN_PARTITION = -1;

    private final long offset;
    // The timestamp of the message.
    // If LogAppendTime is used for the topic, the timestamp will be the timestamp returned by the broker.
    // If CreateTime is used for the topic, the timestamp is the timestamp in the corresponding ProducerRecord if the
    // user provided one. Otherwise, it will be the producer local time when the producer record was handed to the
    // producer.
    private final long timestamp;
    private final long checksum;
    private final int serializedKeySize;
    private final int serializedValueSize;
    private final TopicPartition topicPartition;

    private RecordMetadata(TopicPartition topicPartition, long offset, long timestamp, long
            checksum, int serializedKeySize, int serializedValueSize) {
        super();
        this.offset = offset;
        this.timestamp = timestamp;
        this.checksum = checksum;
        this.serializedKeySize = serializedKeySize;
        this.serializedValueSize = serializedValueSize;
        this.topicPartition = topicPartition;
    }

    @Deprecated
    public RecordMetadata(TopicPartition topicPartition, long baseOffset, long relativeOffset) {
        this(topicPartition, baseOffset, relativeOffset, -1L, -1, -1, -1);
    }

    public RecordMetadata(TopicPartition topicPartition, long baseOffset, long relativeOffset,
                          long timestamp, long checksum, int serializedKeySize, int serializedValueSize) {
        // ignore the relativeOffset if the base offset is -1,
        // since this indicates the offset is unknown
        this(topicPartition, baseOffset == -1 ? baseOffset : baseOffset + relativeOffset,
                timestamp, checksum, serializedKeySize, serializedValueSize);
    }

    /**
     * The offset of the record in the topic/partition.
     */
    public long offset() {
        return this.offset;
    }

    /**
     * The timestamp of the record in the topic/partition.
     */
    public long timestamp() {
        return timestamp;
    }

    /**
     * The checksum (CRC32) of the record.
     */
    public long checksum() {
        return this.checksum;
    }

    /**
     * The size of the serialized, uncompressed key in bytes. If key is null, the returned size
     * is -1.
     */
    public int serializedKeySize() {
        return this.serializedKeySize;
    }

    /**
     * The size of the serialized, uncompressed value in bytes. If value is null, the returned
     * size is -1.
     */
    public int serializedValueSize() {
        return this.serializedValueSize;
    }

    /**
     * The topic the record was appended to
     */
    public String topic() {
        return this.topicPartition.topic();
    }

    /**
     * The partition the record was sent to
     */
    public int partition() {
        return this.topicPartition.partition();
    }

    @Override
    public String toString() {
        return topicPartition.toString() + "@" + offset;
    }
}
