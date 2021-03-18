package com.hxm.broker;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.*;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;

public class Acceptor extends AbstractServerThread{
    Selector nioSelector;
    ServerSocketChannel serverChannel;
    Processor[] processors;
    int sendBufferSize;

    public Acceptor(String host, Integer port, int sendBufferSize, Processor[] processors) {
        try {
            this.nioSelector = Selector.open();
            this.serverChannel=openServerSocket(host,port);
        } catch (IOException e) {
            e.printStackTrace();
        }
        Arrays.stream(processors).forEach(processor ->
            Utils.newThread(String.format("kafka-network-thread-%d",processor.id), processor, false).start()
        );
        this.processors=processors;
        this.sendBufferSize=sendBufferSize;
    }

    @Override
    public void wakeup() {
        this.nioSelector.wakeup();
    }

    @Override
    public void run() {
        System.out.println("Acceptor线程启动中...");
        try {
            //注册OP_ACCEPT事件
            serverChannel.register(nioSelector, SelectionKey.OP_ACCEPT);
        } catch (ClosedChannelException e) {
            e.printStackTrace();
        }
        startupComplete();
        try {
            int currentProcessor = 0;
            while (isRunning()) {
                try {
                    int ready = nioSelector.select(500);
                    if (ready > 0) {
                        Set<SelectionKey> keys = nioSelector.selectedKeys();
                        Iterator<SelectionKey> iter = keys.iterator();
                        while (iter.hasNext() && isRunning()) {
                            try {
                                SelectionKey key = iter.next();
                                iter.remove();
                                if (key.isAcceptable()) {
                                    System.out.println("监听accept，交给processor "+currentProcessor+"处理");
                                    //调用accept()方法处理OP_ACCEPT事件
                                    accept(key, processors[currentProcessor]);
                                } else {
                                    throw new IllegalStateException("Unrecognized key state for acceptor thread.");
                                }

                                // round robin to the next processor thread
                                currentProcessor = (currentProcessor + 1) % processors.length;
                            } catch(Exception e) {
                                throw new RuntimeException("Error while accepting connection", e);
                            }
                        }
                    }
                }
                catch(Exception e) {
                    throw new RuntimeException("Error occurred", e);
                }
            }
        } finally {
            try {
                nioSelector.close();
                serverChannel.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            shutdownComplete();
        }

    }

    public ServerSocketChannel openServerSocket(String host, Integer port) throws IOException {
        InetSocketAddress socketAddress=new InetSocketAddress(host, port);
        ServerSocketChannel serverChannel=ServerSocketChannel.open();
        serverChannel.configureBlocking(false);
        serverChannel.socket().bind(socketAddress);
        return serverChannel;
    }

    public void accept(SelectionKey key, Processor processor) throws IOException {
        ServerSocketChannel serverSocketChannel = (ServerSocketChannel)key.channel();
        //创建socketChannel
        SocketChannel socketChannel = serverSocketChannel.accept();
        try {
            socketChannel.configureBlocking(false);
            socketChannel.socket().setTcpNoDelay(true);
            socketChannel.socket().setKeepAlive(true);
            if (sendBufferSize != -1) {
                socketChannel.socket().setSendBufferSize(sendBufferSize);
            }
            //将socketChannel交给processor处理
            System.out.println("客户端地址："+socketChannel.getLocalAddress());
            processor.accept(socketChannel);
        } catch(Exception e) {
            close(socketChannel);
        }
    }

}
