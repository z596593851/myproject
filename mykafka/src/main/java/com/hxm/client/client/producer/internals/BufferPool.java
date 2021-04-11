package com.hxm.client.client.producer.internals;

import com.hxm.client.common.utils.Time;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class BufferPool {

    private final long totalMemory;
    private final int poolableSize;
    private final ReentrantLock lock;
    private final Deque<ByteBuffer> free;
    private final Deque<Condition> waiters;
    private long availableMemory;
    private final Time time;

    public BufferPool(long memory, int poolableSize, Time time){
        this.totalMemory=memory;
        this.poolableSize=poolableSize;
        this.lock = new ReentrantLock();
        this.free = new ArrayDeque<ByteBuffer>();
        this.waiters = new ArrayDeque<Condition>();
        this.availableMemory = memory;
        this.time=time;
    }

    public ByteBuffer allocate(int size, long maxTimeToBlockMs) throws InterruptedException {
        //内存池默认32M
        if (size > this.totalMemory) {
            throw new IllegalArgumentException("Attempt to allocate " + size
                    + " bytes, but there is a hard limit of "
                    + this.totalMemory
                    + " on memory allocations.");
        }

        this.lock.lock();
        try {
            //如果要分配的内存正好是设定好的一个批次的大小（16k）,则直接分配
            //第一次执行是获取不到的
            if (size == poolableSize && !this.free.isEmpty()) {
                return this.free.pollFirst();
            }
            int freeListSize = this.free.size() * this.poolableSize;
            //内存从 availableMemory + free 够用，则直接分配
            if (this.availableMemory + freeListSize >= size) {
                //如果availableMemory不够，从free回收
                freeUp(size);
                this.availableMemory -= size;
                lock.unlock();
                return ByteBuffer.allocate(size);
            } else {
                //可能内存池子只剩余10k，但是一条消息需要32k，则一点一点分配
                //统计分配出的内存
                int accumulated = 0;
                ByteBuffer buffer = null;
                Condition moreMemory = this.lock.newCondition();
                long remainingTimeToBlockNs = TimeUnit.MILLISECONDS.toNanos(maxTimeToBlockMs);
                this.waiters.addLast(moreMemory);
                //已分配的大小还是小于要分配的
                while (accumulated < size) {
                    long startWaitNs = time.nanoseconds();
                    long timeNs;
                    boolean waitingTimeElapsed;
                    try {
                        //等待其他人释放内存，会唤醒
                        waitingTimeElapsed = !moreMemory.await(remainingTimeToBlockNs, TimeUnit.NANOSECONDS);
                    } catch (InterruptedException e) {
                        this.waiters.remove(moreMemory);
                        throw e;
                    } finally {
                        long endWaitNs = time.nanoseconds();
                        timeNs = Math.max(0L, endWaitNs - startWaitNs);
                    }
                    //等待超时
                    if (waitingTimeElapsed) {
                        this.waiters.remove(moreMemory);
                        throw new RuntimeException("Failed to allocate memory within the configured max blocking time " + maxTimeToBlockMs + " ms.");
                    }

                    remainingTimeToBlockNs -= timeNs;
                    //再次尝试从free获取内存
                    //如果所需正好是16k，直接从free中pop一个
                    if (accumulated == 0 && size == this.poolableSize && !this.free.isEmpty()) {
                        // just grab a buffer from the free list
                        buffer = this.free.pollFirst();
                        accumulated = size;
                    } else {
                        // we'll need to allocate memory, but we may only get
                        // part of what we need on this iteration
                        freeUp(size - accumulated);
                        int got = (int) Math.min(size - accumulated, this.availableMemory);
                        this.availableMemory -= got;
                        accumulated += got;
                    }
                }

                Condition removed = this.waiters.removeFirst();
                if (removed != moreMemory) {
                    throw new IllegalStateException("Wrong condition: this shouldn't happen.");
                }

                // 唤醒其他等待的信号
                if (this.availableMemory > 0 || !this.free.isEmpty()) {
                    if (!this.waiters.isEmpty()) {
                        this.waiters.peekFirst().signal();
                    }
                }

                // unlock and return the buffer
                lock.unlock();
                if (buffer == null) {
                    return ByteBuffer.allocate(size);
                } else {
                    return buffer;
                }
            }
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    private void freeUp(int size) {
        while (!this.free.isEmpty() && this.availableMemory < size) {
            this.availableMemory += this.free.pollLast().capacity();
        }
    }

    public void deallocate(ByteBuffer buffer, int size) {
        lock.lock();
        try {
            //只有还回的内存大小等于一个批次大小，才会放入free
            if (size == this.poolableSize && size == buffer.capacity()) {
                buffer.clear();
                this.free.add(buffer);
            } else {
                this.availableMemory += size;
            }
            //唤醒之前阻塞的信号量
            Condition moreMem = this.waiters.peekFirst();
            if (moreMem != null) {
                moreMem.signal();
            }
        } finally {
            lock.unlock();
        }
    }

    public void deallocate(ByteBuffer buffer) {
        deallocate(buffer, buffer.capacity());
    }
}
