package com.hxm.client.client.producer.internals;

import com.hxm.client.common.Cluster;
import com.hxm.client.common.Node;
import com.hxm.client.common.PartitionInfo;
import com.hxm.client.common.utils.Utils;
import com.hxm.client.common.TopicPartition;
import com.hxm.client.common.record.CompressionType;
import com.hxm.client.common.record.MemoryRecords;
import com.hxm.client.common.record.Record;
import com.hxm.client.common.record.Records;
import com.hxm.client.common.utils.Time;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class RecordAccumulator {

    private volatile boolean closed;
    private final AtomicInteger appendsInProgress;
    private final int batchSize;
    private final BufferPool free;
    private final Time time;
    private final ConcurrentMap<TopicPartition, Deque<RecordBatch>> batches;
    private final CompressionType compression;
    private final IncompleteRecordBatches incomplete;
    private final long lingerMs;
    private final long retryBackoffMs;
    private int drainIndex;


    public RecordAccumulator(int batchSize,
                             long totalSize,
                             CompressionType compression,
                             long lingerMs,
                             long retryBackoffMs,
                             Time time) {
        this.drainIndex=0;
        this.closed = false;
        this.appendsInProgress = new AtomicInteger(0);
        this.batchSize = batchSize;
        this.free = new BufferPool(totalSize, batchSize, time);
        this.time = time;
        this.batches=new ConcurrentHashMap<>();
        this.compression=compression;
        this.incomplete=new IncompleteRecordBatches();
        this.lingerMs=lingerMs;
        this.retryBackoffMs=retryBackoffMs;

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
            //3、计算一个批次的大小
            //虽然默认是16k，但是会从每条消息的大小和16k取一个最大值作为当前批次的大小
            //所以如果一条消息大于16k，那么就会一条消息作为一个批次发送，那么丧失的批次的意义
            int size = Math.max(this.batchSize, Records.LOG_OVERHEAD + Record.recordSize(key, value));
            log.trace("Allocating a new {} byte message buffer for topic {} partition {}", size, tp.topic(), tp.partition());
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
                    //释放空间
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
                // dq.size() > 1 和 batch.records.isFull() 都可以说明已经有一个写满的batch，
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
            //当前RecordBatch写满了
            if (future == null) {
                last.records.close();
            } else {
                return new RecordAppendResult(future, deque.size() > 1 || last.records.isFull(), false);
            }
        }
        return null;
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

    public Map<Integer, List<RecordBatch>> drain(Cluster cluster,
                                                 Set<Node> nodes,
                                                 int maxSize,
                                                 long now) {
        if (nodes.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<Integer, List<RecordBatch>> batches = new HashMap<>();
        for (Node node : nodes) {
            int size = 0;
            List<PartitionInfo> parts = cluster.partitionsForNode(node.id());
            List<RecordBatch> ready = new ArrayList<>();
            /* to make starvation less likely this loop doesn't start at 0 */
            int start = drainIndex = drainIndex % parts.size();
            do {
                PartitionInfo part = parts.get(drainIndex);
                // Only proceed if the partition has no in-flight batches.
                Deque<RecordBatch> deque = getDeque(new TopicPartition(part.topic(), part.partition()));
                if (deque != null) {
                    synchronized (deque) {
                        RecordBatch first = deque.peekFirst();
                        if (first != null) {
                            if (size + first.records.sizeInBytes() > maxSize && !ready.isEmpty()) {
                                // there is a rare case that a single batch size is larger than the request size due
                                // to compression; in this case we will still eventually send this batch in a single
                                // request
                                break;
                            } else {
                                RecordBatch batch = deque.pollFirst();
                                batch.records.close();
                                size += batch.records.sizeInBytes();
                                ready.add(batch);
                                batch.drainedMs = now;
                            }
                        }
                    }
                }

                this.drainIndex = (this.drainIndex + 1) % parts.size();
            } while (start != drainIndex);
            batches.put(node.id(), ready);
        }
        return batches;
    }

    public ReadyCheckResult ready(Cluster cluster, long nowMs) {
        Set<Node> readyNodes = new HashSet<>();
        long nextReadyCheckDelayMs = Long.MAX_VALUE;
        //由于内存池内存不足而等待的线程数
        boolean exhausted = this.free.queued() > 0;
        //遍历batches中所有的队列
        for (Map.Entry<TopicPartition, Deque<RecordBatch>> entry : this.batches.entrySet()) {
            TopicPartition part = entry.getKey();
            Deque<RecordBatch> deque = entry.getValue();
            //根据分区获取到该分区的leader partition所在的broker
            Node leader = cluster.leaderFor(part);
            synchronized (deque) {
                if (!readyNodes.contains(leader)) {
                    //从队列的队头获取批次
                    RecordBatch batch = deque.peekFirst();
                    if (batch != null) {
                        /*
                        batch.lastAttemptMs：上一次重试时间
                        retryBackoffMs：重试的时间间隔
                        backingOff：重新发送的时间到了
                         */
                        //waitedTimeMs：这个批次已经等了多久
                        long waitedTimeMs = nowMs - batch.lastAttemptMs;
                        //lingerMs：如果没有凑齐一个批次，要等待多久再发送。默认0
                        //如果不设置的话，就代表来一条消息就发送一条，明显不合适，所以需要配置
                        //一条消息最多等多久
                        long timeToWaitMs =lingerMs;
                        //最多等多久-等了多久=还要等多久
                        long timeLeftMs = Math.max(timeToWaitMs - waitedTimeMs, 0);
                        //deque.size() > 1说明队列里至少有一个批次已经写满了，或者队列里只有一个批次且已经写满
                        boolean full = deque.size() > 1 || batch.records.isFull();
                        boolean expired = waitedTimeMs >= timeToWaitMs;
                        //一个批次写满了 || 时间到了（即使批次没写满） || 内存不足 ||
                        boolean sendable = full || expired || exhausted || closed;
                        if (sendable) {
                            //把可以发送批次的partition的leader partition所在的主机加入到readyNodes
                            readyNodes.add(leader);
                        } else {
                            // Note that this results in a conservative estimate since an un-sendable partition may have
                            // a leader that will later be found to have sendable data. However, this is good enough
                            // since we'll just wake up and then sleep again for the remaining time.
                            nextReadyCheckDelayMs = Math.min(timeLeftMs, nextReadyCheckDelayMs);
                        }
                    }
                }
            }
        }
        return new ReadyCheckResult(readyNodes, nextReadyCheckDelayMs);
    }

    public void close() {
        this.closed = true;
    }


    private Deque<RecordBatch> getDeque(TopicPartition tp) {
        return batches.get(tp);
    }

    public final static class ReadyCheckResult {
        public final Set<Node> readyNodes;
        public final long nextReadyCheckDelayMs;

        public ReadyCheckResult(Set<Node> readyNodes, long nextReadyCheckDelayMs) {
            this.readyNodes = readyNodes;
            this.nextReadyCheckDelayMs = nextReadyCheckDelayMs;
        }
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
