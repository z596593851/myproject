package com.hxm.client.common;

import com.hxm.client.common.utils.Utils;

import java.util.*;

public class Cluster {

    private final boolean isBootstrapConfigured;
    /**
     * kafka节点信息
     */
    private final List<Node> nodes;
    /**
     * 【主题信息，分区信息】
     */
    private final Map<TopicPartition, PartitionInfo> partitionsByTopicPartition;
    private final Map<String, List<PartitionInfo>> partitionsByTopic;
    private final Map<String, List<PartitionInfo>> availablePartitionsByTopic;
    private final Map<Integer, List<PartitionInfo>> partitionsByNode;
    private final Map<Integer, Node> nodesById;

    public Cluster(boolean isBootstrapConfigured,
                    Collection<Node> nodes,
                    Collection<PartitionInfo> partitions) {
        this.isBootstrapConfigured = isBootstrapConfigured;
        // make a randomized, unmodifiable copy of the nodes
        List<Node> copy = new ArrayList<>(nodes);
        Collections.shuffle(copy);
        this.nodes = Collections.unmodifiableList(copy);
        this.nodesById = new HashMap<>();
        for (Node node : nodes) {
            this.nodesById.put(node.id(), node);
        }

        // index the partitions by topic/partition for quick lookup
        this.partitionsByTopicPartition = new HashMap<>(partitions.size());
        for (PartitionInfo p : partitions) {
            this.partitionsByTopicPartition.put(new TopicPartition(p.topic(), p.partition()), p);
        }

        // index the partitions by topic and node respectively, and make the lists
        // unmodifiable so we can hand them out in user-facing apis without risk
        // of the client modifying the contents
        HashMap<String, List<PartitionInfo>> partsForTopic = new HashMap<>();
        HashMap<Integer, List<PartitionInfo>> partsForNode = new HashMap<>();
        for (Node n : this.nodes) {
            partsForNode.put(n.id(), new ArrayList<>());
        }
        for (PartitionInfo p : partitions) {
            if (!partsForTopic.containsKey(p.topic())) {
                partsForTopic.put(p.topic(), new ArrayList<>());
            }
            List<PartitionInfo> psTopic = partsForTopic.get(p.topic());
            psTopic.add(p);

            if (p.leader() != null) {
                List<PartitionInfo> psNode = Utils.notNull(partsForNode.get(p.leader().id()));
                psNode.add(p);
            }
        }
        this.partitionsByTopic = new HashMap<>(partsForTopic.size());
        this.availablePartitionsByTopic = new HashMap<>(partsForTopic.size());
        for (Map.Entry<String, List<PartitionInfo>> entry : partsForTopic.entrySet()) {
            String topic = entry.getKey();
            List<PartitionInfo> partitionList = entry.getValue();
            this.partitionsByTopic.put(topic, Collections.unmodifiableList(partitionList));
            List<PartitionInfo> availablePartitions = new ArrayList<>();
            for (PartitionInfo part : partitionList) {
                if (part.leader() != null) {
                    availablePartitions.add(part);
                }
            }
            this.availablePartitionsByTopic.put(topic, Collections.unmodifiableList(availablePartitions));
        }
        this.partitionsByNode = new HashMap<>(partsForNode.size());
        for (Map.Entry<Integer, List<PartitionInfo>> entry : partsForNode.entrySet()) {
            this.partitionsByNode.put(entry.getKey(), Collections.unmodifiableList(entry.getValue()));
        }
    }

    public List<Node> nodes() {
        return this.nodes;
    }

    public Node leaderFor(TopicPartition topicPartition) {
        PartitionInfo info = partitionsByTopicPartition.get(topicPartition);
        if (info == null) {
            return null;
        } else {
            return info.leader();
        }
    }

    public List<PartitionInfo> partitionsForNode(int nodeId) {
        return this.partitionsByNode.get(nodeId);
    }
}
