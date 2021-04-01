package com.hxm.consumer;

import com.hxm.broker.KSelector;
import com.hxm.clients.NetworkClient;
import com.hxm.producer.Time;
import com.hxm.producer.TopicPartition;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;

@Slf4j
public class KafkaConsumer<K,V> {

    private final Fetcher<K,V> fetcher;
    private final ConsumerNetworkClient client;
    private final Time time;
    private final SubscriptionState subscriptions;
    private final Deserializer keyDeserializer;
    private final Deserializer valueDeserializer;

    public KafkaConsumer() {
        this.time=new Time();
        NetworkClient networkClient = new NetworkClient(new KSelector(102400),"1","127.0.0.1",6666,time);
        this.subscriptions=new SubscriptionState();
        //暂时手动插入
        TopicPartition tp1=new TopicPartition("liu",0);
        subscriptions.assignFromSubscribed(asList(tp1));
        //暂时手动提交offset。其实是由定时任务提交
        subscriptions.seek(tp1,1);


        this.client=new ConsumerNetworkClient(networkClient);
        this.keyDeserializer=new IntegerDeserializer();
        this.valueDeserializer=new StringDeserializer();
        this.fetcher=new Fetcher<K,V>(client,time,1,50 * 1024 * 1024,100,10 * 1024,500, false, subscriptions,keyDeserializer,valueDeserializer);
    }

    public void subscribe(Collection<String> topics) {
        if (topics == null || topics.isEmpty()) {
            throw new IllegalArgumentException("Topic collection to subscribe to cannot be null");
        } else {
            for (String topic : topics) {
                if (topic == null || topic.trim().isEmpty()) {
                    throw new IllegalArgumentException("Topic collection to subscribe to cannot contain null or empty topic");
                }
            }
        }
        this.subscriptions.subscribe(new HashSet<>(topics));
    }

    public ConsumerRecords<K, V> poll(long timeout) {

        if (timeout < 0) {
            throw new IllegalArgumentException("Timeout must not be negative");
        }
        // poll for new data until the timeout expires
        long start = time.milliseconds();
        long remaining = timeout;
        do {
            //pollOnce会先通过ConsumerCoordinator与GroupCoordinator交互完成Rebalance操作，
            //之后从GroupCoordinator获取最近一次提交的offset（或重置position）
            //最后才是用Fetcher从kafka获取消息进行消费
            Map<TopicPartition, List<ConsumerRecord<K, V>>> records = pollOnce(remaining);
            if (!records.isEmpty()) {
                //创建并缓存FetchRequest
                //为了提升效率，在对records集合进行处理之前，先发送一次FetchRequest，这样线程处理完本次records集合的操作，
                //与FethRequest及其响应在网络上传输以及在服务端的处理就变成并行的了
                //这样就可以减少等待网络IO的时间
                fetcher.sendFetches();
                //发送FetchRequest，此pollNoWakeup不会阻塞，不能被中断
                client.pollNoWakeup();
                return new ConsumerRecords<>(records);
            }

            //计算超时时间
            long elapsed = time.milliseconds() - start;
            remaining = timeout - elapsed;
        } while (remaining > 0);

        return ConsumerRecords.empty();
    }

    private Map<TopicPartition, List<ConsumerRecord<K, V>>> pollOnce(long timeout) {
        //模仿其他请求，进行阻塞式调用
        //作为测试方案先poll一下以取消掉connect事件
        client.poll();

        //尝试从completedFetches缓存中解析消息
        Map<TopicPartition, List<ConsumerRecord<K, V>>> records = fetcher.fetchedRecords();
        if (!records.isEmpty()) {
            return records;
        }

        // send any new fetches (won't resend pending fetches)
        //创建并缓存FetchRequest请求
        fetcher.sendFetches();

        long now = time.milliseconds();

        //发送FetchRequest
        client.poll(timeout, now, new ConsumerNetworkClient.PollCondition() {
            @Override
            public boolean shouldBlock() {
                // since a fetch might be completed by the background thread, we need this poll condition
                // to ensure that we do not block unnecessarily in poll()
                return !fetcher.hasCompletedFetches();
            }
        });

        return fetcher.fetchedRecords();
    }

}
