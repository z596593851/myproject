package com.hxm.core.common;

public class LongRef {

    private long value;

    public LongRef(long value) {
        this.value = value;
    }

    public long getValue() {
        return value;
    }

    public long addAndGet(long delta){
        value+=delta;
        return value;
    }

    public long getAndAdd(long delta){
        long result=value;
        value+=delta;
        return result;
    }

    public long getAndIncrement(){
        long v=value;
        value+=1;
        return v;
    }

    public long incrementAndGet(){
        value+=1;
        return value;
    }

    public long getAndDecrement(){
        long v=value;
        value-=1;
        return v;
    }

    public long decrementAndGet(){
        value-=1;
        return value;
    }
}
