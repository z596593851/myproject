package com.hxm.producer;

public interface Callback {
    public void onCompletion(RecordMetadata metadata, Exception exception);
}
