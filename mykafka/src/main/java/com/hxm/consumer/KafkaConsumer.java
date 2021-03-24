package com.hxm.consumer;

import com.hxm.broker.KSelector;
import com.hxm.clients.NetworkClient;
import com.hxm.producer.Time;

import java.util.Collection;

public class KafkaConsumer {

    private final Fetcher fetcher;
    private final ConsumerNetworkClient client;
    private final Time time;
    private final SubscriptionState subscriptions;

    public KafkaConsumer(Fetcher fetcher, ConsumerNetworkClient client) {
        NetworkClient networkClient = new NetworkClient(new KSelector(102400),"1","127.0.0.1",6666);
        this.time=new Time();
        this.subscriptions=new SubscriptionState();
        this.client=new ConsumerNetworkClient(networkClient);
        this.fetcher=new Fetcher(client,time,1,50 * 1024 * 1024,100,10 * 1024, 500,subscriptions);
    }

    public void subscribe(Collection<String> topics) {

    }
}
