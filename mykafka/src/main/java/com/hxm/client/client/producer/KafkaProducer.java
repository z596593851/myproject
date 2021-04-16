package com.hxm.client.client.producer;

import com.hxm.client.client.NetworkClient;
import com.hxm.client.client.producer.internals.Callback;
import com.hxm.client.client.producer.internals.RecordAccumulator;
import com.hxm.client.client.producer.internals.Sender;
import com.hxm.client.common.TopicPartition;
import com.hxm.client.common.network.KSelector;
import com.hxm.client.common.record.CompressionType;
import com.hxm.client.common.serialization.StringSerializer;
import com.hxm.client.common.utils.KafkaThread;
import com.hxm.client.common.utils.Time;
import com.sun.xml.internal.ws.encoding.soap.SerializationException;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class KafkaProducer {
    private final String clientId;
    private final StringSerializer keySerializer;
    private final StringSerializer valueSerializer;
    private final Time time;
    private final RecordAccumulator accumulator;
    private final CompressionType compressionType;
    private final long BUFFER_MEMORY=32*1024*1024;
    private final int BATCH_SIZE=8*1024;
    private final Sender sender;
    private final Thread ioThread;

    public KafkaProducer(){
        this.clientId="DemoProducer";
        this.keySerializer=new StringSerializer();
        this.valueSerializer=new StringSerializer();
        this.time=new Time();
        //todo 压缩模式
        this.compressionType=CompressionType.NONE;
        //this.accumulator=new RecordAccumulator(BATCH_SIZE,BUFFER_MEMORY, CompressionType.GZIP,new Time());
        this.accumulator=new RecordAccumulator(BATCH_SIZE,BUFFER_MEMORY,compressionType,10L,100,time);
        NetworkClient client=new NetworkClient(new KSelector(102400),clientId,50L,time);
        this.sender=new Sender(client,accumulator,new Time(), (short) 1,1,1024*1024);
        this.ioThread=new KafkaThread("kafka-producer-network-thread",sender,true);
        this.ioThread.start();
    }
    public Future<RecordMetadata> doSend(ProducerRecord record, Callback callback) {
        try {
            sender.wakeup();
            byte[] serializedKey;
            try {
                serializedKey = keySerializer.serialize(record.topic(), record.key());
            } catch (ClassCastException cce) {
                throw new SerializationException("Can't convert key");
            }
            byte[] serializedValue;
            try {
                serializedValue = valueSerializer.serialize(record.topic(), record.value());
            } catch (ClassCastException cce) {
                throw new SerializationException("Can't convert value");
            }
            //根据分区器计算分区
            int partition=0;
            long remainingWaitMs=100;
            TopicPartition tp=new TopicPartition(record.topic(), partition);
            long timestamp = record.timestamp() == null ? time.milliseconds() : record.timestamp();
            //将消息放入accumulator（一个32M的内存），然后由accumulator把消息封装成一个批次一个批次去发送
            RecordAccumulator.RecordAppendResult result = accumulator.append(tp, timestamp, serializedKey, serializedValue, callback, remainingWaitMs);
            if (result.batchIsFull || result.newBatchCreated) {
                //唤醒sender线程发送数据
                this.sender.wakeup();
             }
            return result.future;
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void close() {
        close(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    public void close(long timeout, TimeUnit timeUnit) {
        close(timeout, timeUnit, false);
    }

    private void close(long timeout, TimeUnit timeUnit, boolean swallowException) {
        if (timeout < 0) {
            throw new IllegalArgumentException("The timeout cannot be negative.");
        }

        AtomicReference<Throwable> firstException = new AtomicReference<>();
        boolean invokedFromCallback = Thread.currentThread() == this.ioThread;
        if (timeout > 0) {
            if (invokedFromCallback) {
                System.out.println("Overriding close timeout "+timeout+" ms to 0 ms in order to prevent useless blocking due to self-join. " +
                        "This means you have incorrectly invoked close with a non-zero timeout from the producer call-back.");
            } else {
                // Try to close gracefully.
                if (this.sender != null) {
//                    this.sender.initiateClose();
                }
                if (this.ioThread != null) {
                    try {
                        this.ioThread.join(timeUnit.toMillis(timeout));
                    } catch (InterruptedException t) {
                        firstException.compareAndSet(null, t);
                        System.out.println("Interrupted while joining ioThread");
                    }
                }
            }
        }
    }


}
