package com.hxm.broker;

public class KafkaRequestHandlerPool {

    private Thread[] threads;
    private KafkaRequestHandler[] runnables;
    private int numThreads;
    private ReplicaManager replicaManager;

    public KafkaRequestHandlerPool(int numThreads,RequestChannel requestChannel,ReplicaManager replicaManager){
        this.replicaManager=replicaManager;
        this.numThreads=numThreads;
        this.threads=new Thread[this.numThreads];
        this.runnables=new KafkaRequestHandler[this.numThreads];
        for(int i=0; i<numThreads; i++){
            runnables[i]=new KafkaRequestHandler(requestChannel,replicaManager);
            threads[i]=Utils.daemonThread("request-handler-"+i,this.runnables[i]);
            threads[i].start();
        }
    }
}
