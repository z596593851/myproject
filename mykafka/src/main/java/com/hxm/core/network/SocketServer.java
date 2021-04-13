package com.hxm.core.network;

import com.hxm.client.common.utils.Utils;

public class SocketServer {

    /**
     * Processor线程个数
     */
    private final int numProcessorThreads;
    /**
     * 网卡个数
     */
    private final int endpoints;
    /**
     * Processor线程总个数
     */
    private final int totalProcessorThreads;
    /**
     * 队列最大容量
     */
    private final int maxQueuedRequests;
    //processor线程与handler线程之间交换数据的队列,总共有totalProcessorThreads个
    private final RequestChannel requestChannel;
    /**
     * processor线程的集合
     */
    private final Processor[] processors;
    private final String host;
    private final int port;

    public SocketServer(String host, int port){
        this.endpoints=1;
        this.numProcessorThreads=3;
        this.maxQueuedRequests=10;
        this.totalProcessorThreads=this.endpoints*numProcessorThreads;
        this.requestChannel = new RequestChannel(this.numProcessorThreads, this.maxQueuedRequests);
        this.processors=new Processor[this.numProcessorThreads];
        this.host=host;
        this.port=port;
    }
    public void startup(){
        //每个endpoint对应一个acceptor，每个acceptor对应多个processor
        for(int i=0; i<this.numProcessorThreads; i++){
            processors[i]=new Processor(i,requestChannel,304857600);
        }
        Acceptor acceptor=new Acceptor(this.host,this.port,102400,processors);
        //创建acceptor对应线程并启动
        Utils.newThread("kafka-socket-acceptor", acceptor, false).start();
        //主线程阻塞等待acceptor线程启动完成
        acceptor.awaitStartup();
        System.out.println("Acceptor启动完成");
    }
    public RequestChannel getRequestChannel(){
        return this.requestChannel;
    }

}
