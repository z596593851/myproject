package com.hxm.consumer;

import com.hxm.broker.Utils;
import com.hxm.producer.*;
import com.hxm.protocol.ApiKeys;
import com.hxm.requests.FetchResponse;
import com.sun.xml.internal.ws.encoding.soap.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Fetcher<K, V>{
    private static final Logger log = LoggerFactory.getLogger(Fetcher.class);
    private final ConsumerNetworkClient client;
    private final Time time;
    private final int minBytes;
    private final int maxBytes;
    private final int maxWaitMs;
    private final int fetchSize;
    private final int maxPollRecords;
    private final boolean checkCrcs;
    private final SubscriptionState subscriptions;
    private final ConcurrentLinkedQueue<CompletedFetch> completedFetches;
    private PartitionRecords<K, V> nextInLineRecords = null;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;

    public Fetcher(ConsumerNetworkClient client, Time time, int minBytes, int maxBytes, int maxWaitMs, int fetchSize, int maxPollRecords, boolean checkCrcs, SubscriptionState subscriptions,Deserializer<K> keyDeserializer,
                   Deserializer<V> valueDeserializer) {
        this.client = client;
        this.time = time;
        this.minBytes = minBytes;
        this.maxBytes = maxBytes;
        this.maxWaitMs = maxWaitMs;
        this.fetchSize = fetchSize;
        this.maxPollRecords = maxPollRecords;
        this.checkCrcs = checkCrcs;
        this.subscriptions=subscriptions;
        this.completedFetches = new ConcurrentLinkedQueue<>();
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
    }

    private static class PartitionRecords<K, V> {
        private long fetchOffset;
        private TopicPartition partition;
        private List<ConsumerRecord<K, V>> records;

        public PartitionRecords(long fetchOffset, TopicPartition partition, List<ConsumerRecord<K, V>> records) {
            this.fetchOffset = fetchOffset;
            this.partition = partition;
            this.records = records;
        }

        private boolean isEmpty() {
            return records == null || records.isEmpty();
        }

        private void discard() {
            this.records = null;
        }

        private List<ConsumerRecord<K, V>> take(int n) {
            if (records == null) {
                return new ArrayList<>();
            }

            if (n >= records.size()) {
                List<ConsumerRecord<K, V>> res = this.records;
                this.records = null;
                return res;
            }

            List<ConsumerRecord<K, V>> res = new ArrayList<>(n);
            Iterator<ConsumerRecord<K, V>> iterator = records.iterator();
            for (int i = 0; i < n; i++) {
                res.add(iterator.next());
                iterator.remove();
            }

            if (iterator.hasNext()) {
                this.fetchOffset = iterator.next().offset();
            }

            return res;
        }
    }

    private static class CompletedFetch {
        private final TopicPartition partition;
        private final long fetchedOffset;
        private final FetchResponse.PartitionData partitionData;
        private final Set<TopicPartition> partitions;

        public CompletedFetch(TopicPartition partition,
                              long fetchedOffset,
                              FetchResponse.PartitionData partitionData,
                              Set<TopicPartition> partitions) {
            this.partition = partition;
            this.fetchedOffset = fetchedOffset;
            this.partitionData = partitionData;
            this.partitions=partitions;
        }
    }

    public Map<TopicPartition, List<ConsumerRecord<K, V>>> fetchedRecords() {
        Map<TopicPartition, List<ConsumerRecord<K, V>>> drained = new HashMap<>();
        int recordsRemaining = maxPollRecords;

        while (recordsRemaining > 0) {
            if (nextInLineRecords == null || nextInLineRecords.isEmpty()) {
                CompletedFetch completedFetch = completedFetches.poll();
                if (completedFetch == null) {
                    break;
                }

                //解析completedFetch
                nextInLineRecords = parseFetchedData(completedFetch);
            } else {
                recordsRemaining -= append(drained, nextInLineRecords, recordsRemaining);
            }
        }

        return drained;
    }

    private int append(Map<TopicPartition, List<ConsumerRecord<K, V>>> drained,
                       PartitionRecords<K, V> partitionRecords,
                       int maxRecords) {
        if (partitionRecords.isEmpty()) {
            return 0;
        }

        if (!subscriptions.isAssigned(partitionRecords.partition)) {
            // this can happen when a rebalance happened before fetched records are returned to the consumer's poll call
            log.debug("Not returning fetched records for partition {} since it is no longer assigned", partitionRecords.partition);
        } else {
            // note that the consumed position should always be available as long as the partition is still assigned
            long position = subscriptions.position(partitionRecords.partition);
            if (!subscriptions.isFetchable(partitionRecords.partition)) {
                // this can happen when a partition is paused before fetched records are returned to the consumer's poll call
                log.debug("Not returning fetched records for assigned partition {} since it is no longer fetchable", partitionRecords.partition);
            } else if (partitionRecords.fetchOffset == position) {
                // we are ensured to have at least one record since we already checked for emptiness
                //获取消息集合
                List<ConsumerRecord<K, V>> partRecords = partitionRecords.take(maxRecords);
                //最后一个消息的offset
                long nextOffset = partRecords.get(partRecords.size() - 1).offset() + 1;

                log.trace("Returning fetched records at offset {} for assigned partition {} and update " +
                        "position to {}", position, partitionRecords.partition, nextOffset);

                List<ConsumerRecord<K, V>> records = drained.get(partitionRecords.partition);
                if (records == null) {
                    records = partRecords;
                    drained.put(partitionRecords.partition, records);
                } else {
                    records.addAll(partRecords);
                }

                //更新subscriptionState中对应topicPartitionState的position字段
                subscriptions.position(partitionRecords.partition, nextOffset);
                return partRecords.size();
            } else {
                // these records aren't next in line based on the last consumed position, ignore them
                // they must be from an obsolete request
                log.debug("Ignoring fetched records for {} at offset {} since the current position is {}",
                        partitionRecords.partition, partitionRecords.fetchOffset, position);
            }
        }

        partitionRecords.discard();
        return 0;
    }

    private PartitionRecords<K, V> parseFetchedData(CompletedFetch completedFetch) {
        TopicPartition tp = completedFetch.partition;
        FetchResponse.PartitionData partition = completedFetch.partitionData;
        long fetchOffset = completedFetch.fetchedOffset;
        int bytes = 0;
        int recordsCount = 0;
        PartitionRecords<K, V> parsedRecords = null;
        if (!subscriptions.isFetchable(tp)) {
            // this can happen when a rebalance happened or a partition consumption paused
            // while fetch is still in-flight
            log.debug("Ignoring fetched records for partition {} since it is no longer fetchable", tp);
        } else{
            // we are interested in this fetch only if the beginning offset matches the
            // current consumed position
            Long position = subscriptions.position(tp);
            if (position == null || position != fetchOffset) {
                log.debug("Discarding stale fetch response for partition {} since its offset {} does not match " +
                        "the expected offset {}", tp, fetchOffset, position);
                return null;
            }

            ByteBuffer buffer = partition.recordSet;
            MemoryRecords records = MemoryRecords.readableRecords(buffer);
            List<ConsumerRecord<K, V>> parsed = new ArrayList<>();
            //解压缩
            for (LogEntry logEntry : records) {
                // Skip the messages earlier than current position.
                //跳过早于position的消息
                if (logEntry.offset() >= position) {
                    parsed.add(parseRecord(tp, logEntry));
                    bytes += logEntry.size();
                }
            }

            recordsCount = parsed.size();

            if (!parsed.isEmpty()) {
                log.trace("Adding fetched record for partition {} with offset {} to buffered record list", tp, position);
                //将解析后的record集合封装成PartitionRecords
                parsedRecords = new PartitionRecords<>(fetchOffset, tp, parsed);
                ConsumerRecord<K, V> record = parsed.get(parsed.size() - 1);
            }
        }


        // we move the partition to the end if we received some bytes or if there was an error. This way, it's more
        // likely that partitions for the same topic can remain together (allowing for more efficient serialization).
        if (bytes > 0) {
            subscriptions.movePartitionToEnd(tp);
        }

        return parsedRecords;
    }

    /**
     * Parse the record entry, deserializing the key / value fields if necessary
     */
    private ConsumerRecord<K, V> parseRecord(TopicPartition partition, LogEntry logEntry) {
        Record record = logEntry.record();

        if (this.checkCrcs) {
            try {
                record.ensureValid();
            } catch (Exception e) {
                throw new RuntimeException("Record for partition " + partition + " at offset " + logEntry.offset()
                        + " is invalid, cause: " + e.getMessage());
            }
        }

        try {
            long offset = logEntry.offset();
            long timestamp = record.timestamp();
            TimestampType timestampType = record.timestampType();
            ByteBuffer keyBytes = record.key();
            byte[] keyByteArray = keyBytes == null ? null : Utils.toArray(keyBytes);
            K key = keyBytes == null ? null : this.keyDeserializer.deserialize(partition.topic(), keyByteArray);
            ByteBuffer valueBytes = record.value();
            byte[] valueByteArray = valueBytes == null ? null : Utils.toArray(valueBytes);
            V value = valueBytes == null ? null : this.valueDeserializer.deserialize(partition.topic(), valueByteArray);

            return new ConsumerRecord<>(partition.topic(), partition.partition(), offset,
                    timestamp, timestampType, record.checksum(),
                    keyByteArray == null ? ConsumerRecord.NULL_SIZE : keyByteArray.length,
                    valueByteArray == null ? ConsumerRecord.NULL_SIZE : valueByteArray.length,
                    key, value);
        } catch (RuntimeException e) {
            throw new SerializationException("Error deserializing key/value for partition " + partition +
                    " at offset " + logEntry.offset(), e);
        }
    }

    public void sendFetches(){
        for (Map.Entry<Node, FetchRequest> fetchEntry : createFetchRequests().entrySet()) {
            final FetchRequest request = fetchEntry.getValue();
            final Node fetchTarget = fetchEntry.getKey();
            //将发往每个node的fetchRequest都缓存到unsent队列上
            client.send(fetchTarget, ApiKeys.FETCH, request)
                    .addListener(new RequestFutureListener<ClientResponse>() {
                        @Override
                        public void onSuccess(ClientResponse resp) {
                            com.hxm.requests.FetchResponse response = new com.hxm.requests.FetchResponse(resp.responseBody());
                            if (!matchesRequestedPartitions(request, response)) {
                                log.warn("Ignoring fetch response containing partitions {} since it does not match " +
                                                "the requested partitions {}", response.responseData().keySet(),
                                        request.fetchData().keySet());
                                return;
                            }
                            Set<TopicPartition> partitions = new HashSet<>(response.responseData().keySet());

                            //遍历响应中的数据
                            for (Map.Entry<TopicPartition, FetchResponse.PartitionData> entry : response.responseData().entrySet()) {
                                TopicPartition partition = entry.getKey();
                                long fetchOffset = request.fetchData().get(partition).offset;
                                FetchResponse.PartitionData fetchData = entry.getValue();
                                //创建CompletedFetch并缓存到completedFetches队列中
                                //存储在completedFetches队列中的消息数据还是未解析的FetchResponse.PartitionData对象
                                //在fetchedRecords()方法中会将completedFetch中的消息进行解析，得到Record集合并返回
                                //同时还会修改对应TopicPartitionState的position，为下次fetch操作做好准备。
                                completedFetches.add(new CompletedFetch(partition, fetchOffset, fetchData, partitions));
                            }
                        }
                        @Override
                        public void onFailure(RuntimeException e) {
                            log.debug("Fetch request to {} failed", fetchTarget, e);
                        }
                    });
        }
    }

    private boolean matchesRequestedPartitions(FetchRequest request, FetchResponse response) {
        Set<TopicPartition> requestedPartitions = request.fetchData().keySet();
        Set<TopicPartition> fetchedPartitions = response.responseData().keySet();
        return fetchedPartitions.equals(requestedPartitions);
    }

    public Map<Node, FetchRequest> createFetchRequests(){
        Map<Node, LinkedHashMap<TopicPartition, FetchRequest.PartitionData>> fetchable = new LinkedHashMap<>();
        //fetchablePartitions()：获取可以发送FetchRequest的分区
        for (TopicPartition partition : fetchablePartitions()) {
            Node node=new Node(1,"127.0.0.1",6666);
            LinkedHashMap<TopicPartition, FetchRequest.PartitionData> fetch = fetchable.get(node);
            if (fetch == null) {
                fetch = new LinkedHashMap<>();
                fetchable.put(node, fetch);
            }

            long position = this.subscriptions.position(partition);
            //记录每个分区对应的position，即要fetch消息的offset
            fetch.put(partition, new FetchRequest.PartitionData(position, this.fetchSize));
        }
        // create the fetches
        Map<Node, FetchRequest> requests = new HashMap<>();
        //将上述fetchable进行转换，将发往同一node节点的所有topicPartition的position信息封装成一个fetchRequest对象
        for (Map.Entry<Node, LinkedHashMap<TopicPartition, FetchRequest.PartitionData>> entry : fetchable.entrySet()) {
            Node node = entry.getKey();
            FetchRequest fetch = new FetchRequest(this.maxWaitMs, this.minBytes, this.maxBytes, entry.getValue());
            requests.put(node, fetch);
        }
        return requests;

    }

    private List<TopicPartition> fetchablePartitions() {
        //分配给当前消费者的分区
        List<TopicPartition> fetchable=new ArrayList<>();
        fetchable.add(new TopicPartition("xiaoming",0));
        return fetchable;
    }

    /**
     * Return whether we have any completed fetches pending return to the user. This method is thread-safe.
     * @return true if there are completed fetches, false otherwise
     */
    public boolean hasCompletedFetches() {
        return !completedFetches.isEmpty();
    }




}
