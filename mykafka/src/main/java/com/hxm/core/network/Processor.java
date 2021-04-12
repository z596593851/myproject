package com.hxm.core.network;

import com.hxm.client.common.network.KSelector;
import com.hxm.client.common.network.KafkaChannel;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Processor extends AbstractServerThread {
    ConcurrentLinkedQueue<SocketChannel> newConnections;
    Map<String, RequestChannel.Response> inflightResponses;
    KSelector selector;
    RequestChannel requestChannel;
    //Processor下标
    int id;

    public Processor(int id,RequestChannel requestChannel, int maxRequestSize){
        this.newConnections=new ConcurrentLinkedQueue<>();
        this.inflightResponses=new HashMap<>();
        this.selector=new KSelector(maxRequestSize);
        this.requestChannel=requestChannel;
        this.id=id;
    }

    @Override
    public void wakeup() {
        this.selector.wakeup();
    }

    @Override
    public void run() {
        startupComplete();
        System.out.println("Processor "+id+"启动");
        while (isRunning()){
            configureNewConnections();
            //将requestChannel.responseQueues上的响应绑到channel上等待发送
            //同时存在inflightResponses里标记为待发送
            processNewResponses();
            //将发往服务端的请求read完毕后，存在completedReceives，
            //将服务端回写的响应write完毕后，存在CompletedSends，标记为已发送
            poll();
            //处理completedReceives队列，将发给服务端的请求交给requestChannel.requestQueue等待handle处理
            //handle处理完后会在requestChannel.responseQueues里加一个响应
            processCompletedReceives();
            //从inflightResponses里移除CompletedSends里的响应
            processCompletedSends();
            processDisconnected();
        }
        closeAll();
        shutdownComplete();

    }

    public void poll(){
        try {
            selector.poll(300);
        } catch (IOException e) {
            closeAll();
            shutdownComplete();
            throw new RuntimeException(e);
        }
    }

    public void closeAll(){
        selector.channels().forEach(channel->close(selector,channel.id()));
    }

    public void configureNewConnections() {
        while (!newConnections.isEmpty()) {
            try {
                SocketChannel channel=newConnections.poll();
                String localHost=channel.socket().getLocalAddress().getHostAddress();
                int localPort=channel.socket().getLocalPort();
                String remoteHost = channel.socket().getInetAddress().getHostAddress();
                int remotePort = channel.socket().getPort();
                String connectionId=localHost+":"+localPort+"-"+remoteHost+":"+remotePort;
                System.out.println("connectionId："+connectionId);
                selector.register(connectionId,channel);
            } catch (ClosedChannelException e) {
                e.printStackTrace();
            }
        }
    }

    public void processNewResponses(){
        RequestChannel.Response curr=this.requestChannel.receiveResponse(id);
        while(curr!=null){
            try {
                switch (curr.responseAction){
                    //没有响应需要发给客户端
                    case NoOpAction:
                        //注册OP_READ事件
                        selector.unmute(curr.request.connectionId);
                        break;
                        //该响应需要发给客户端
                    case SendAction:
                        sendResponse(curr);
                        break;
                    case CloseConnectionAction:
                        close(selector,curr.request.connectionId);
                        break;
                    default:break;
                }
            }finally {
                curr=this.requestChannel.receiveResponse(id);
            }
        }
    }

    public void sendResponse(RequestChannel.Response response){
        System.out.println("有响应发送给客户端");
        KafkaChannel channel=selector.channel(response.send.destination());
        if(channel!=null){
            selector.send(response.getResponseSend());
            this.inflightResponses.put(response.request.connectionId,response);
        }
    }

    public void processCompletedReceives(){
        this.selector.completedReceives().forEach(receive->{
            //KafkaChannel channel=selector.channel(receive.source());
            RequestChannel.Request req= new RequestChannel.Request(this.id,receive.source(),receive.payload());
            requestChannel.sendRequest(req);
            //取消注册的OP_READ事件，表示在发送响应之前此连接不能再读取任何请求
            selector.mute(receive.source());
            System.out.println("取消OP_READ");
        });
    }

    public void processCompletedSends(){
        this.selector.completedSends().forEach(send -> {
            inflightResponses.remove(send.destination());
            //重新注册OP_READ事件，允许此连接继续读取数据
            selector.unmute(send.destination());
            System.out.println("重新注册OP_READ");
        });
    }

    public void processDisconnected() {
        this.selector.disconnected().forEach(connectionId->{
            //从inflightResponses删除该连接对应的所有response
            inflightResponses.remove(connectionId);
        });
    }

    public void accept(SocketChannel socketChannel) {
        //将SocketChannel放到线程安全的ConcurrentLinkedQueue队列newConnections中，
        //然后唤醒processor线程来处理newConnections队列
        newConnections.add(socketChannel);
        wakeup();
    }


}
