package com.hxm.broker;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractServerThread implements Runnable {

    private final CountDownLatch startupLatch = new CountDownLatch(1);
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);
    private final AtomicBoolean alive=new AtomicBoolean(true);

    public abstract void wakeup();

    public void shutdown() throws InterruptedException {
        this.alive.set(false);
        wakeup();
        this.shutdownLatch.await();
    }

    public void awaitStartup()  {
        try {
            this.startupLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void startupComplete(){
        this.startupLatch.countDown();
    }

    public void shutdownComplete(){
        this.shutdownLatch.countDown();
    }

    public boolean isRunning(){
        return this.alive.get();
    }

    public void close(SocketChannel channel) throws IOException {
        if(channel!=null){
            channel.socket().close();
            channel.close();
        }
    }

    public void close(KSelector selector , String connectionId) {
        KafkaChannel channel = selector.channel(connectionId);
        if (channel != null) {
            selector.close(connectionId);
        }
    }

}
