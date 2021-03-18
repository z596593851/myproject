package com.hxm.producer;

public final class ProducerRecord {

    private final String topic;
    private final Integer partition;
    private final String key;
    private final String value;
    private final Long timestamp;

    public ProducerRecord(String topic, Integer partition, Long timestamp, String key, String value) {
        if (topic == null) {
            throw new IllegalArgumentException("Topic cannot be null.");
        }
        if (timestamp != null && timestamp < 0) {
            throw new IllegalArgumentException(
                    String.format("Invalid timestamp: %d. Timestamp should always be non-negative or null.", timestamp));
        }
        if (partition != null && partition < 0) {
            throw new IllegalArgumentException(
                    String.format("Invalid partition: %d. Partition number should always be non-negative or null.", partition));
        }
        this.topic = topic;
        this.partition = partition;
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
    }

    public ProducerRecord(String topic, String value) {
        this(topic, null, null, null, value);
    }

    public String topic() {
        return topic;
    }

    public String key() {
        return key;
    }

    public String value() {
        return value;
    }

    public Long timestamp() {
        return timestamp;
    }

    public Integer partition() {
        return partition;
    }

    @Override
    public String toString() {
        String key = this.key == null ? "null" : this.key.toString();
        String value = this.value == null ? "null" : this.value.toString();
        String timestamp = this.timestamp == null ? "null" : this.timestamp.toString();
        return "ProducerRecord(topic=" + topic + ", partition=" + partition + ", key=" + key + ", value=" + value +
            ", timestamp=" + timestamp + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (!(o instanceof ProducerRecord)) {
            return false;
        }

        ProducerRecord that = (ProducerRecord) o;

        if (key != null ? !key.equals(that.key) : that.key != null) {
            return false;
        } else if (partition != null ? !partition.equals(that.partition) : that.partition != null) {
            return false;
        } else if (topic != null ? !topic.equals(that.topic) : that.topic != null) {
            return false;
        } else if (value != null ? !value.equals(that.value) : that.value != null) {
            return false;
        } else if (timestamp != null ? !timestamp.equals(that.timestamp) : that.timestamp != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = topic != null ? topic.hashCode() : 0;
        result = 31 * result + (partition != null ? partition.hashCode() : 0);
        result = 31 * result + (key != null ? key.hashCode() : 0);
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + (timestamp != null ? timestamp.hashCode() : 0);
        return result;
    }
}
