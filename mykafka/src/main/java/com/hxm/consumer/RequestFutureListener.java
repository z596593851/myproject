package com.hxm.consumer;

public interface RequestFutureListener<T>{

    void onSuccess(T value);

    void onFailure(RuntimeException e);
}
