package com.hxm.client.client.producer.internals;

import com.hxm.client.client.producer.RecordMetadata;

public interface Callback {
    public void onCompletion(RecordMetadata metadata, Exception exception);
}
