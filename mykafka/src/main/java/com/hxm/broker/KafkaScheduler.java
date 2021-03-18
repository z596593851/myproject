package com.hxm.broker;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class KafkaScheduler {
    private final int threads;
    private final String threadNamePrefix="kafka-scheduler-";
    private final boolean daemon=true;
    private ScheduledThreadPoolExecutor executor;
    private AtomicInteger schedulerThreadId=new AtomicInteger(0);
    public KafkaScheduler(int threads){
        this.threads=threads;
    }

    public void startup(){
        if(isStarted()){
            throw new IllegalStateException("This scheduler has already been started!");
        }
        executor = new ScheduledThreadPoolExecutor(threads);
        //shutdown后是否执行周期
        executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        //shutdown后是否执行延迟
        executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        executor.setThreadFactory(r -> Utils.newThread(threadNamePrefix + schedulerThreadId.getAndIncrement(), r, daemon));
    }

    public void schedule(String name, Fun fun, long delay, long period, TimeUnit unit){
        ensureRunning();
        Runnable runnable= fun::apply;
        if(period >= 0) {
            //周期性执行
            executor.scheduleAtFixedRate(runnable, delay, period, unit);
        } else {
            //只执行一次
            executor.schedule(runnable, delay, unit);
        }

    }


    private boolean isStarted(){
        return executor!=null;
    }

    private void ensureRunning(){
        if(!isStarted()){
            throw new IllegalStateException("Kafka scheduler is not running.");
        }
    }
}
