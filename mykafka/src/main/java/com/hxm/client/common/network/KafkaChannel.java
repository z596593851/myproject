package com.hxm.client.common.network;

import java.io.IOException;
import java.net.Socket;
import java.nio.channels.SelectionKey;

public class KafkaChannel {
    private String id;
    private Send send;
    private NetworkReceive receive;
    private final int maxReceiveSize;
    private TransportLayer transportLayer;

    public KafkaChannel(String id, TransportLayer transportLayer,int maxReceiveSize) {
        this.id = id;
        this.transportLayer = transportLayer;
        this.maxReceiveSize=maxReceiveSize;
    }

    public void setSend(Send send) {
        if (this.send != null) {
            throw new IllegalStateException("Attempt to begin a send operation with prior send operation still in progress.");
        }
        this.send = send;
        //绑定一个OP_WRITE事件
        System.out.println("注册OP_WRITE");
        this.transportLayer.addInterestOps(SelectionKey.OP_WRITE);
    }

    public void mute(){
        transportLayer.removeInterestOps(SelectionKey.OP_READ);
    }

    public void unmute(){
        transportLayer.addInterestOps(SelectionKey.OP_READ);
    }

    public void close(){
        try {
            this.transportLayer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public boolean finishConnect() throws IOException {
        return transportLayer.finishConnect();
    }

    public NetworkReceive read() throws IOException {
        NetworkReceive result = null;

        if (receive == null) {
            receive = new NetworkReceive(maxReceiveSize, id);
        }

        receive(receive);
        //是否读完一个完整数据
        if (receive.complete()) {
            receive.payload().rewind();
            result = receive;
            receive = null;
        }
        return result;
    }

    public Send write() throws IOException {
        Send result = null;
        if (send != null && send(send)) {
            result = send;
            send = null;
        }
        return result;
    }

    private boolean send(Send send) throws IOException {
        send.writeTo(transportLayer);
        if (send.completed()) {
            transportLayer.removeInterestOps(SelectionKey.OP_WRITE);
        }
        return send.completed();
    }

    private long receive(NetworkReceive receive) throws IOException {
        return receive.readFrom(transportLayer);
    }

    public String id() {
        return id;
    }

    public boolean isMute() {
//        return key.isValid() && (key.interestOps() & SelectionKey.OP_READ) == 0;
        return transportLayer.isMute();
    }

    public String socketDescription() {
        Socket socket = transportLayer.socketChannel().socket();
        if (socket.getInetAddress() == null) {
            return socket.getLocalAddress().toString();
        }
        return socket.getInetAddress().toString();
    }

    public boolean ready() {
        return transportLayer.ready();
    }
}
