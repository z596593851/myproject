package com.hxm.broker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaThread extends Thread {

    private final Logger log = LoggerFactory.getLogger(getClass());

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
