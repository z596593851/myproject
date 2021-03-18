package com.hxm.producer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ProduceRequestResult {

    private final CountDownLatch latch = new CountDownLatch(1);
    private volatile TopicPartition topicPartition;
    private volatile long baseOffset = -1L;
    private volatile RuntimeException error;

    public ProduceRequestResult() {
    }

    public void done(TopicPartition topicPartition, long baseOffset, RuntimeException error) {
        this.topicPartition = topicPartition;
        this.baseOffset = baseOffset;
        this.error = error;
        this.latch.countDown();
    }

    public void await() throws InterruptedException {
        latch.await();
    }

    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        return latch.await(timeout, unit);
    }

    public long baseOffset() {
        return baseOffset;
    }

    public RuntimeException error() {
        return error;
    }

    public TopicPartition topicPartition() {
        return topicPartition;
    }

    public boolean completed() {
        return this.latch.getCount() == 0L;
    }
}
