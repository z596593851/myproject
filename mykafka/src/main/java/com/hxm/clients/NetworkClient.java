package com.hxm.clients;

import com.hxm.broker.KSelector;
import com.hxm.protocol.ApiKeys;
import com.hxm.requests.RequestHeader;
import com.hxm.consumer.ClientRequest;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class NetworkClient {

    private KSelector selector;
    private boolean isConnect=false;
    private String id;
    private String host;
    private int port;
    private int correlation;

    public NetworkClient(KSelector selector, String id, String host, int port){
        this.selector=selector;
        this.id=id;
        this.host=host;
        this.port=port;
        this.correlation=0;
    }

    public void send(ClientRequest request){
        selector.send(request.request());
    }

    public void poll(long timeout){
        initiateConnect();
        try {
            selector.poll(timeout);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void initiateConnect(){
        if(!isConnect){
            try {
                isConnect=true;
                selector.connect(id,new InetSocketAddress(host,port),102400,102400);
            } catch (IOException e) {
                isConnect=false;
                e.printStackTrace();
            }
        }
    }

    public RequestHeader nextRequestHeader(ApiKeys key) {
        return new RequestHeader(key.id, id, correlation++);
    }

    public RequestHeader nextRequestHeader(ApiKeys key, short version) {
        return new RequestHeader(key.id,version, id, correlation++);
    }

    public void wakeup() {
        this.selector.wakeup();
    }

    public static void main(String[] args) {

        Charset charset = StandardCharsets.UTF_8;
        String req="是是是我我我是是的的的123321";
        ByteBuffer buffer =ByteBuffer.allocate(4+req.getBytes().length);
        buffer.putInt(req.getBytes().length);
        buffer.put(req.getBytes());
        buffer.rewind();
//        ClientRequest request=new ClientRequest(new RequestSend("1",buffer),null);
        KSelector selector=new KSelector(102400);
        NetworkClient networkClient=new NetworkClient(selector,"1","127.0.0.1",6666);
//        networkClient.initiateConnect();
        networkClient.poll(500);
//        networkClient.send(request);
        networkClient.poll(500);
//        System.out.println(buffer.getInt());
//        System.out.println(charset.decode(buffer).toString());

    }

}
