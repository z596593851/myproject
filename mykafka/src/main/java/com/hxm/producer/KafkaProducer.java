package com.hxm.producer;

import com.hxm.broker.KSelector;
import com.hxm.broker.KafkaThread;
import com.hxm.clients.NetworkClient;
import com.hxm.protocol.ApiKeys;
import com.hxm.requests.ProduceRequest;
import com.hxm.test.ClientRequest;
import com.hxm.test.ClientResponse;
import com.hxm.test.RequestCompletionHandler;
import com.hxm.test.RequestSend;
import com.sun.xml.internal.ws.encoding.soap.SerializationException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class KafkaProducer {
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
        this.keySerializer=new StringSerializer();
        this.valueSerializer=new StringSerializer();
        this.time=new Time();
        this.compressionType=CompressionType.GZIP;
        //this.accumulator=new RecordAccumulator(BATCH_SIZE,BUFFER_MEMORY, CompressionType.GZIP,new Time());
        this.accumulator=new RecordAccumulator(BATCH_SIZE,BUFFER_MEMORY,compressionType,time);
        NetworkClient client=new NetworkClient(new KSelector(102400),"1","127.0.0.1",6666);
        this.sender=new Sender(client,accumulator,new Time(), (short) 1,1);
        this.ioThread=new KafkaThread("kafka-producer-network-thread",sender,true);
        this.ioThread.start();
    }
    public Future<RecordMetadata> doSend(ProducerRecord record, Callback callback) {
        try {
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

        AtomicReference<Throwable> firstException = new AtomicReference<Throwable>();
        boolean invokedFromCallback = Thread.currentThread() == this.ioThread;
        if (timeout > 0) {
            if (invokedFromCallback) {
                System.out.println("Overriding close timeout "+timeout+" ms to 0 ms in order to prevent useless blocking due to self-join. " +
                        "This means you have incorrectly invoked close with a non-zero timeout from the producer call-back.");
            } else {
                // Try to close gracefully.
                if (this.sender != null) {
                    this.sender.initiateClose();
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

//        if (this.sender != null && this.ioThread != null && this.ioThread.isAlive()) {
//            System.out.println("Proceeding to force close the producer since pending requests could not be completed " +
//                    "within timeout "+timeout+" ms.");
//            this.sender.forceClose();
//            // Only join the sender thread when not calling from callback.
//            if (!invokedFromCallback) {
//                try {
//                    this.ioThread.join();
//                } catch (InterruptedException e) {
//                    firstException.compareAndSet(null, e);
//                }
//            }
//        }
//
//        ClientUtils.closeQuietly(interceptors, "producer interceptors", firstException);
//        ClientUtils.closeQuietly(metrics, "producer metrics", firstException);
//        ClientUtils.closeQuietly(keySerializer, "producer keySerializer", firstException);
//        ClientUtils.closeQuietly(valueSerializer, "producer valueSerializer", firstException);
//        AppInfoParser.unregisterAppInfo(JMX_PREFIX, clientId);
//        log.debug("The Kafka producer has closed.");
//        if (firstException.get() != null && !swallowException)
//            throw new KafkaException("Failed to close kafka producer", firstException.get());
    }

    public static void main(String[] args) {
        long BUFFER_MEMORY=32*1024*1024;
        int BATCH_SIZE=8*1024;
        RecordAccumulator accumulator=new RecordAccumulator(BATCH_SIZE,BUFFER_MEMORY,CompressionType.GZIP,new Time());
        KafkaProducer kafkaProducer=new KafkaProducer();

        Sender sender=new Sender(null,accumulator,new Time(), (short) 1,1);
        kafkaProducer.doSend(new ProducerRecord("xiaoming","123"),null);
        kafkaProducer.doSend(new ProducerRecord("xiaohong","456"),null);
        Map<Integer, List<RecordBatch>> batches = accumulator.drain();
        //将发往同一个broker上面partition的数据组合成一个请求，而不是一个partition发一个请求，从而减少网络IO
        List<ClientRequest> requests = sender.createProduceRequests(batches, 1L);
    }


}
