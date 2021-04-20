package com.hxm.client.common.network;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.*;
import java.util.*;

public class KSelector {
    private final Map<String, KafkaChannel> channels;
    private final Selector nioSelector;
    private final List<Send> completedSends;
    private final List<String> failedSends;
    private final List<String> connected;
    private final Map<KafkaChannel, Deque<NetworkReceive>> stagedReceives;
    private final int maxReceiveSize;
    private final List<String> disconnected;
    private final List<NetworkReceive> completedReceives;
    private final Set<SelectionKey> immediatelyConnectedKeys;

    public KSelector(int maxReceiveSize){
        this.channels=new HashMap<>();
        try {
            this.nioSelector=Selector.open();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.completedSends=new ArrayList<>();
        this.failedSends=new ArrayList<>();
        this.connected=new ArrayList<>();
        this.stagedReceives=new HashMap<>();
        this.maxReceiveSize=maxReceiveSize;
        this.disconnected=new ArrayList<>();
        this.completedReceives=new ArrayList<>();
        this.immediatelyConnectedKeys=new HashSet<>();

    }

    public void connect(String id, InetSocketAddress address, int sendBufferSize, int receiveBufferSize) throws IOException {
        if (this.channels.containsKey(id)) {
            throw new IllegalStateException("There is already a connection for id " + id);
        }

        SocketChannel socketChannel = SocketChannel.open();
        socketChannel.configureBlocking(false);
        Socket socket = socketChannel.socket();
        socket.setKeepAlive(true);
        if (sendBufferSize != -1) {
            socket.setSendBufferSize(sendBufferSize);
        }
        if (receiveBufferSize != -1) {
            socket.setReceiveBufferSize(receiveBufferSize);
        }
        //默认是false，设置成true代表要开启Nagle算法
        //它会把网络中的一些小的数据包收集起来，组合成大的数据包发送
        //但是kafka中是会有小的数据包的，如果开启这个选项，会导致消息发送延迟
        socket.setTcpNoDelay(true);
        boolean connected;
        try {
            //尝试与服务器连接。由于是非阻塞，所以可能立马会连接成功，返回true，也可能很久才连接成功，返回false
            connected = socketChannel.connect(address);
        } catch (UnresolvedAddressException e) {
            socketChannel.close();
            throw new IOException("Can't resolve address: " + address, e);
        } catch (IOException e) {
            socketChannel.close();
            throw e;
        }
        SelectionKey key = socketChannel.register(nioSelector, SelectionKey.OP_CONNECT);
        PlaintextTransportLayer transportLayer = new PlaintextTransportLayer(key);
        KafkaChannel channel=new KafkaChannel(id,transportLayer,maxReceiveSize);
//        KafkaChannel channel = new KafkaChannel(id, key,maxReceiveSize);
        //key和channel关联起来，可以根据key找到channel，可以用channel找到key
        key.attach(channel);
        this.channels.put(id, channel);
        //如果立马就连接成功，取消OP_CONNECT时间
        //一般是无法立马连接成功的，会在之后的this.client.poll连接成功
        if (connected) {
            immediatelyConnectedKeys.add(key);
            key.interestOps(0);
        }
    }

    public void register(String id, SocketChannel socketChannel) throws ClosedChannelException {
        SelectionKey key = socketChannel.register(nioSelector, SelectionKey.OP_READ);
        System.out.println("注册OP_READ事件");
        KafkaChannel channel=null;
        try {
            PlaintextTransportLayer transportLayer = new PlaintextTransportLayer(key);
            channel=new KafkaChannel(id,transportLayer,maxReceiveSize);
        } catch (IOException e) {
            e.printStackTrace();
        }


        // socketChannel.register(nioSelector, SelectionKey.OP_CONNECT);
        //KafkaChannel channel = new KafkaChannel(id,key,maxReceiveSize);
        key.attach(channel);
        this.channels.put(id, channel);
    }

    public void wakeup() {
        this.nioSelector.wakeup();
    }

    public void close(String id) {
        KafkaChannel channel = this.channels.get(id);
        if (channel != null) {
            close(channel);
        }
    }

    private void close(KafkaChannel channel) {
        channel.close();
        this.stagedReceives.remove(channel);
        this.channels.remove(channel.id());
    }

    public void close() {
        List<String> connections = new ArrayList<>(channels.keySet());
        try {
            this.nioSelector.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void send(Send send) {
        //获取一个kafkachannel
        KafkaChannel channel = this.channels.get(send.destination());
        if (channel == null) {
            throw new IllegalStateException("Attempt to retrieve channel for which there is no open connection. Connection id " + send.destination() + " existing connections " + channels.keySet());
        }
        try {
            //往channel上存一个发送的请求
            channel.setSend(send);
        } catch (CancelledKeyException e) {
            this.failedSends.add(send.destination());
        }
    }

    public void mute(String id) {
        KafkaChannel channel = this.channels.get(id);
        mute(channel);
    }

    private void mute(KafkaChannel channel) {
        channel.mute();
    }

    public void unmute(String id) {
        KafkaChannel channel = this.channels.get(id);
        unmute(channel);
    }

    private void unmute(KafkaChannel channel) {
        channel.unmute();
    }

    public KafkaChannel channel(String id) {
        return this.channels.get(id);
    }
    private KafkaChannel channel(SelectionKey key) {
        return (KafkaChannel) key.attachment();
    }

    private int select(long ms) throws IOException {
        if (ms < 0L) {
            throw new IllegalArgumentException("timeout should be >= 0");
        }

        if (ms == 0L) {
            return this.nioSelector.select();
//            return this.nioSelector.selectNow();
        } else {
            return this.nioSelector.select(ms);
        }
    }

    public void poll(long timeout) throws IOException{

        clear();
        //看Selector注册了多少事件
        int readyKeys = select(timeout);
        if (readyKeys > 0 || !immediatelyConnectedKeys.isEmpty()) {
            //处理selector上的key
            //存放接收到的响应
            //System.out.println("事件数："+readyKeys);
            pollSelectionKeys(this.nioSelector.selectedKeys(), false);
            pollSelectionKeys(immediatelyConnectedKeys, true);
        }
        //处理接收到的响应
        addToCompletedReceives();

    }

    private void addToCompletedReceives() {
        if (!this.stagedReceives.isEmpty()) {
            Iterator<Map.Entry<KafkaChannel, Deque<NetworkReceive>>> iter = this.stagedReceives.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<KafkaChannel, Deque<NetworkReceive>> entry = iter.next();
                KafkaChannel channel = entry.getKey();
                if (!channel.isMute()) {
                    Deque<NetworkReceive> deque = entry.getValue();
                    NetworkReceive networkReceive = deque.poll();
                    this.completedReceives.add(networkReceive);
                    if (deque.isEmpty()) {
                        iter.remove();
                    }
                }
            }
        }
    }

    private void pollSelectionKeys(Iterable<SelectionKey> selectionKeys,
                                   boolean isImmediatelyConnected) {
        Iterator<SelectionKey> iterator = selectionKeys.iterator();
        while (iterator.hasNext()) {
            SelectionKey key = iterator.next();
            iterator.remove();
            //根据key找到channel
            KafkaChannel channel = channel(key);
            try {
                //客户端第一次走这个分支，因为是建立连接
                if (isImmediatelyConnected || key.isConnectable()) {
                    System.out.println("有connect事件");
                    //如果之前初始化的时候没有完成连接，那么这里完成网络连接
                    if (channel.finishConnect()) {
                        //网络连接建立以后，缓存这个channel
                        this.connected.add(channel.id());
                       // SocketChannel socketChannel = (SocketChannel) key.channel();
                    } else {
                        continue;
                    }
                }
                if(key.isAcceptable()){
                    System.out.println("有accept事件");
                }
                //接收服务端发送回来的响应
                if (key.isReadable() && !hasStagedReceive(channel)) {
//                    System.out.println("有read事件");
                    NetworkReceive networkReceive;
                    while ((networkReceive = channel.read()) != null) {
                        addToStagedReceives(channel, networkReceive);
                    }
                }
                if (key.isWritable()) {
//                    System.out.println("有write事件");
                    //给borker发送请求
                    Send send = channel.write();
                    if (send != null) {
                        this.completedSends.add(send);
                    }
                }
                if (!key.isValid()) {
                    close(channel);
                    this.disconnected.add(channel.id());
                }
            } catch (Exception e) {
                String desc=channel.socketDescription();
                if (e instanceof IOException) {
                    System.out.println(String.format("Connection with %s disconnected:%s",desc,e));
                } else {
                    System.out.println(String.format("Unexpected error from %s; closing connection:%s",desc,e));
                }
                close(channel);
                this.disconnected.add(channel.id());
            }
        }

    }

    private void addToStagedReceives(KafkaChannel channel, NetworkReceive receive) {
        //一个channel对应一个与kafka主机的连接，用stagedReceives来存放
        if (!stagedReceives.containsKey(channel)) {
            stagedReceives.put(channel, new ArrayDeque<>());
        }

        Deque<NetworkReceive> deque = stagedReceives.get(channel);
        //往队列里存放接收到到响应
        //下面的addToCompletedReceives处理这些响应
        deque.add(receive);
    }

    public boolean isChannelReady(String id) {
        KafkaChannel channel = this.channels.get(id);
        return channel != null && channel.ready();
    }
    private boolean hasStagedReceive(KafkaChannel channel) {
        return stagedReceives.containsKey(channel);
    }

    public List<KafkaChannel> channels() {
        return new ArrayList<>(channels.values());
    }
    public List<Send> completedSends() {
        return this.completedSends;
    }

    public List<NetworkReceive> completedReceives() {
        return this.completedReceives;
    }

    public List<String> disconnected() {
        return this.disconnected;
    }

    public List<String> connected() {
        return this.connected;
    }

    private void clear() {
        this.completedSends.clear();
        this.completedReceives.clear();
        this.connected.clear();
        this.disconnected.clear();
        this.disconnected.addAll(this.failedSends);
        this.failedSends.clear();
    }




}
