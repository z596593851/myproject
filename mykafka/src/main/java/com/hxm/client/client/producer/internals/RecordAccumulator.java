package com.hxm.client.client.producer.internals;

import com.hxm.client.common.utils.Utils;
import com.hxm.client.common.TopicPartition;
import com.hxm.client.common.record.CompressionType;
import com.hxm.client.common.record.MemoryRecords;
import com.hxm.client.common.record.Record;
import com.hxm.client.common.record.Records;
import com.hxm.client.common.utils.Time;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class RecordAccumulator {

    private volatile boolean closed;
    private final AtomicInteger appendsInProgress;
    private final int batchSize;
    private final BufferPool free;
    private final Time time;
    private final ConcurrentMap<TopicPartition, Deque<RecordBatch>> batches;
    private final CompressionType compression;
    private final IncompleteRecordBatches incomplete;

    public RecordAccumulator(int batchSize,
                             long totalSize,
                             CompressionType compression,
                             Time time) {
        this.closed = false;
        this.appendsInProgress = new AtomicInteger(0);
        this.batchSize = batchSize;
        this.free = new BufferPool(totalSize, batchSize, time);
        this.time = time;
        this.batches=new ConcurrentHashMap<>();
        this.compression=compression;
        this.incomplete=new IncompleteRecordBatches();

    }

    public final static class RecordAppendResult {
        public final FutureRecordMetadata future;
        public final boolean batchIsFull;
        public final boolean newBatchCreated;

        public RecordAppendResult(FutureRecordMetadata future, boolean batchIsFull, boolean newBatchCreated) {
            this.future = future;
            this.batchIsFull = batchIsFull;
            this.newBatchCreated = newBatchCreated;
        }
    }

    public RecordAppendResult append(TopicPartition tp,
                                     long timestamp,
                                     byte[] key,
                                     byte[] value,
                                     Callback callback,
                                     long maxTimeToBlock) throws InterruptedException {
        appendsInProgress.incrementAndGet();
        try {
            // check if we have an in-progress batch
            //1、根据分区找到应该插入到哪个队列
            Deque<RecordBatch> dq = getOrCreateDeque(tp);
            synchronized (dq) {
                if (closed) {
                    throw new IllegalStateException("Cannot send after the producer is closed.");
                }
                //2、尝试往队列里添加数据。由于数据是要放到批次对象，而第一次运行时还没有给批次对象分配内存，所以会添加失败。
                RecordAppendResult appendResult = tryAppend(timestamp, key, value, callback, dq);
                if (appendResult != null) {
                    return appendResult;
                }
            }

            // we don't have an in-progress record batch try to allocate a new batch
            //3、计算一个批次的大小
            //虽然默认是16k，但是会从每条消息的大小和16k取一个最大值作为当前批次的大小
            //所以如果一条消息大于16k，那么就会一条消息作为一个批次发送，那么丧失的批次的意义
            int size = Math.max(this.batchSize, Records.LOG_OVERHEAD + Record.recordSize(key, value));
            //log.trace("Allocating a new {} byte message buffer for topic {} partition {}", size, tp.topic(), tp.partition());
            //4、从内存池分配批次内存
            ByteBuffer buffer = free.allocate(size, maxTimeToBlock);
            synchronized (dq) {
                // Need to check if producer is closed again after grabbing the dequeue lock.
                if (closed) {
                    throw new IllegalStateException("Cannot send after the producer is closed.");
                }
                //5、尝试把数据写入批次队列里的最后一个批次。
                //如果批次里有剩余空间，先往剩余空间里写
                RecordAppendResult appendResult = tryAppend(timestamp, key, value, callback, dq);
                if (appendResult != null) {
                    // Somebody else found us a batch, return the one we waited for! Hopefully this doesn't happen often...
                    free.deallocate(buffer);
                    return appendResult;
                }
                //6、如果批次队列已经没有空间，或者批次队列里还没有批次，则开辟一个新的批次(16k)
                MemoryRecords records = MemoryRecords.emptyRecords(buffer, compression, this.batchSize);
                RecordBatch batch = new RecordBatch(tp, records, time.milliseconds());
                //尝试往批次里写数据，会执行成功
                FutureRecordMetadata future = Utils.notNull(batch.tryAppend(timestamp, key, value, callback, time.milliseconds()));
                //7、把批次放入队列的队尾
                dq.addLast(batch);
                incomplete.add(batch);
                return new RecordAppendResult(future, dq.size() > 1 || batch.records.isFull(), true);
            }
        } finally {
            appendsInProgress.decrementAndGet();
        }
    }

    private Deque<RecordBatch> getOrCreateDeque(TopicPartition tp) {
        Deque<RecordBatch> d = this.batches.get(tp);
        if (d != null) {
            return d;
        }
        d = new ArrayDeque<>();
        Deque<RecordBatch> previous = this.batches.putIfAbsent(tp, d);
        if (previous == null) {
            return d;
        } else {
            return previous;
        }
    }

    private RecordAppendResult tryAppend(long timestamp, byte[] key, byte[] value, Callback callback, Deque<RecordBatch> deque) {
        RecordBatch last = deque.peekLast();
        if (last != null) {
            FutureRecordMetadata future = last.tryAppend(timestamp, key, value, callback, time.milliseconds());
            if (future == null) {
                last.records.close();
            } else {
                return new RecordAppendResult(future, deque.size() > 1 || last.records.isFull(), false);
            }
        }
        return null;
    }

    private Deque<RecordBatch> getDeque(TopicPartition tp) {
        return batches.get(tp);
    }

    public Map<Integer, List<RecordBatch>> drain(){
        Map<Integer, List<RecordBatch>> batches = new HashMap<>();
        List<RecordBatch> batchList=new ArrayList<>();
        for(Deque<RecordBatch> deque:this.batches.values()){
            while (!deque.isEmpty()){
                RecordBatch batch=deque.pollFirst();
                batchList.add(batch);
                batch.records.close();
            }
        }
        if(!batchList.isEmpty()){
            batches.put(1,batchList);
        }
        return batches;
    }

    private final static class IncompleteRecordBatches {
        private final Set<RecordBatch> incomplete;

        public IncompleteRecordBatches() {
            this.incomplete = new HashSet<RecordBatch>();
        }

        public void add(RecordBatch batch) {
            synchronized (incomplete) {
                this.incomplete.add(batch);
            }
        }

        public void remove(RecordBatch batch) {
            synchronized (incomplete) {
                boolean removed = this.incomplete.remove(batch);
                if (!removed) {
                    throw new IllegalStateException("Remove from the incomplete set failed. This should be impossible.");
                }
            }
        }

        public Iterable<RecordBatch> all() {
            synchronized (incomplete) {
                return new ArrayList<>(this.incomplete);
            }
        }
    }
}
