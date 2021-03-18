package com.hxm.test;

public class Test1ResponseHandler extends RequestFutureAdapter<ClientResponse,Void>{

    @Override
    public void onSuccess(ClientResponse resp, RequestFuture<Void> future) {
//        System.out.println("处理response："+resp.request().request().request());
//        future.complete(null);
    }
}
