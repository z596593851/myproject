package com.hxm.test;

public interface RequestFutureListener<T>{

    void onSuccess(T value);

    void onFailure(RuntimeException e);
}
