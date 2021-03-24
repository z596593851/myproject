package com.hxm.consumer;

import com.hxm.producer.Time;
import com.hxm.producer.TopicPartition;
import com.hxm.protocol.ApiKeys;
import com.hxm.requests.FetchResponse;
import com.hxm.test.ClientResponse;
import com.hxm.test.RequestFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Fetcher {
    private static final Logger log = LoggerFactory.getLogger(Fetcher.class);
    private final ConsumerNetworkClient client;
    private final Time time;
    private final int minBytes;
    private final int maxBytes;
    private final int maxWaitMs;
    private final int fetchSize;
    private final int maxPollRecords;
    private final SubscriptionState subscriptions;
    private final ConcurrentLinkedQueue<CompletedFetch> completedFetches;

    public Fetcher(ConsumerNetworkClient client, Time time, int minBytes, int maxBytes, int maxWaitMs, int fetchSize, int maxPollRecords, SubscriptionState subscriptions) {
        this.client = client;
        this.time = time;
        this.minBytes = minBytes;
        this.maxBytes = maxBytes;
        this.maxWaitMs = maxWaitMs;
        this.fetchSize = fetchSize;
        this.maxPollRecords = maxPollRecords;
        this.subscriptions=subscriptions;
        this.completedFetches = new ConcurrentLinkedQueue<>();

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
        fetchable.add(new TopicPartition("xiaoming",1));
        return fetchable;
    }


}
