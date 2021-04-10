package com.hxm.client.client.consumer.internals;

public interface RequestFutureListener<T>{

    void onSuccess(T value);

    void onFailure(RuntimeException e);
}
