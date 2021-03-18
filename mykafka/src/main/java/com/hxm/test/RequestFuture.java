package com.hxm.test;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

public class RequestFuture<T> {

    private static final Object INCOMPLETE_SENTINEL = new Object();
    private final AtomicReference<Object> result = new AtomicReference<>(INCOMPLETE_SENTINEL);
    private final Queue<RequestFutureListener<T>> listeners = new ConcurrentLinkedQueue<>();

    public boolean isDone() {
        return result.get() != INCOMPLETE_SENTINEL;
    }

    public T value() {
        if (!succeeded()){
            throw new IllegalStateException("Attempt to retrieve value from future which hasn't successfully completed");
        }
        return (T) result.get();
    }

    public boolean succeeded() {
        return isDone() && !failed();
    }

    public boolean failed() {
        return result.get() instanceof RuntimeException;
    }

    public RuntimeException exception() {
        if (!failed()) {
            throw new IllegalStateException("Attempt to retrieve exception from future which hasn't failed");
        }
        return (RuntimeException) result.get();
    }

    /**
     * 回调listener的onSuccess
     */
    private void fireSuccess() {
        T value = value();
        while (true) {
            RequestFutureListener<T> listener = listeners.poll();
            if (listener == null) {
                break;
            }
            listener.onSuccess(value);
        }
    }

    /**
     * 回调listener的onFailure
     */
    private void fireFailure() {
        RuntimeException exception = exception();
        while (true) {
            RequestFutureListener<T> listener = listeners.poll();
            if (listener == null) {
                break;
            }
            listener.onFailure(exception);
        }
    }

    /**
     * 以cas方式修改result的值
     * @param value 修改值
     */
    public void complete(T value) {
        if (value instanceof RuntimeException) {
            throw new IllegalArgumentException("The argument to complete can not be an instance of RuntimeException");
        }

        if (!result.compareAndSet(INCOMPLETE_SENTINEL, value)) {
            throw new IllegalStateException("Invalid attempt to complete a request future which is already complete");
        }
        fireSuccess();
    }

    public void raise(RuntimeException e) {
        if (e == null) {
            throw new IllegalArgumentException("The exception passed to raise must not be null");
        }

        if (!result.compareAndSet(INCOMPLETE_SENTINEL, e)) {
            throw new IllegalStateException("Invalid attempt to complete a request future which is already complete");
        }
        fireFailure();
    }

    public void addListener(RequestFutureListener<T> listener) {
        this.listeners.add(listener);
        if (failed()) {
            fireFailure();
        } else if (succeeded()) {
            fireSuccess();
        }
    }

    /**
     * 将RequestFuture从T类型适配到S类型
     * @param adapter 适配器
     * @param <S> 结果类型
     * @return
     */
    public <S> RequestFuture<S> compose(final RequestFutureAdapter<T, S> adapter) {
        final RequestFuture<S> adapted = new RequestFuture<>();
        addListener(new RequestFutureListener<T>() {
            @Override
            public void onSuccess(T value) {
                adapter.onSuccess(value, adapted);
            }

            @Override
            public void onFailure(RuntimeException e) {
                adapter.onFailure(e, adapted);
            }
        });
        return adapted;
    }

    /**
     * 通过RequestFutureListener监听器将value传递给下一个RequestFutured对象
     * @param future
     */
    public void chain(final RequestFuture<T> future) {
        addListener(new RequestFutureListener<T>() {
            @Override
            public void onSuccess(T value) {
                future.complete(value);
            }

            @Override
            public void onFailure(RuntimeException e) {
                future.raise(e);
            }
        });
    }



}
