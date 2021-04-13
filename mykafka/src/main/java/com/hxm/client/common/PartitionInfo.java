package com.hxm.client.common;

public class PartitionInfo {
    /**
     * 当前分区对应哪个主题
     */
    private final String topic;
    private final int partition;
    /**
     * 当前分区的leader对应对节点
     */
    private final Node leader;

    public PartitionInfo(String topic, int partition, Node leader) {
        this.topic = topic;
        this.partition = partition;
        this.leader = leader;
    }

    /**
     * The topic name
     */
    public String topic() {
        return topic;
    }

    /**
     * The partition id
     */
    public int partition() {
        return partition;
    }

    /**
     * The node id of the node currently acting as a leader for this partition or null if there is no leader
     */
    public Node leader() {
        return leader;
    }

    @Override
    public String toString() {
        return String.format("Partition(topic = %s, partition = %d, leader = %s)",
                topic,
                partition,
                leader == null ? "none" : leader.id());
    }
}
