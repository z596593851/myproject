package com.hxm.client.client.consumer.internals;

/**
 * 将RequestFuture从F类型适配到T类型
 * @param <F>
 * @param <T>
 */
public abstract class RequestFutureAdapter<F,T> {
    public abstract void onSuccess(F value, RequestFuture<T> future);

    public void onFailure(RuntimeException e, RequestFuture<T> future) {
        future.raise(e);
    }
}
