package com.hxm.client.common.utils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafkaThread extends Thread {

    public KafkaThread(final String name, Runnable runnable, boolean daemon) {
        super(runnable, name);
        //设置为守护线程
        setDaemon(daemon);
        setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                log.error("Uncaught exception in " + name + ": ", e);
            }
        });
    }

}
