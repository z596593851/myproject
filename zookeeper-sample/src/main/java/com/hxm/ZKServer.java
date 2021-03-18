package com.hxm;

import org.apache.zookeeper.*;

public class ZKServer {
    private String connectStr="localhost:2181";
    private ZooKeeper zkCli;
    private int sessionTimeout=2000;

    public static void main(String[] args) throws Exception{
        ZKServer zkServer=new ZKServer();
        zkServer.getConnect();
        zkServer.regist(args[0]);
        zkServer.business();

    }

    private void getConnect() throws Exception{
        zkCli=new ZooKeeper(connectStr, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {

            }
        });
    }
    private void regist(String hostname) throws Exception{
        String path=zkCli.create("/servers/server",hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(path+" is online");
    }
    private void business() throws Exception{
        Thread.sleep(Long.MAX_VALUE);
    }
}
