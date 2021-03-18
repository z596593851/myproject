package com.hxm.test;

public class TestDemo {
    public Client client;
    public TestDemo(Client client){
        this.client=client;
    }


    public class Test2ResponseHandler extends RequestFutureAdapter<ClientResponse,Void>{
        @Override
        public void onSuccess(ClientResponse value, RequestFuture<Void> future) {
            // request-1结束以后再通知request-test4的回调
            client.send("1","request-test4").compose(new Test1ResponseHandler()).chain(future);
        }
    }

    public void test(){
        RequestFuture<Void> requestFuture1 = client.send("1","request-test1").compose(new Test2ResponseHandler());
        client.send("1","request-test2").compose(new Test2ResponseHandler());
        //client.send("1","request-test3").compose(new Test1ResponseHandler());
        client.poll(requestFuture1);
    }

    public static void main(String[] args) {
        Client client=new Client();
        TestDemo testDemo=new TestDemo(client);
        testDemo.test();

    }

}
