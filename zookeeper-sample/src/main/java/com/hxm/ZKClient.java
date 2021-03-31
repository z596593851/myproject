package com.hxm;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.ArrayList;
import java.util.List;

public class ZKClient {
    private String connectStr="localhost:2181";
    private ZooKeeper zkCli;
    private int sessionTimeout=2000;

    public static void main(String[] args) throws Exception{
        ZKClient zkClient=new ZKClient();
        zkClient.getConnect();
        zkClient.getConnect();
        zkClient.business();
    }

    private void getConnect() throws Exception{
        zkCli=new ZooKeeper(connectStr, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                try {
                    getChildren();
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }
    private void getChildren() throws KeeperException, InterruptedException {
        //true代表监听
        List<String> children=zkCli.getChildren("/servers",true);
        ArrayList<String> hosts=new ArrayList<>();
        for(String child:children){
            byte[] data=zkCli.getData("/servers/"+child,false,null);
            hosts.add(new String(data));
        }
        System.out.println(hosts);
    }
    private void business() throws Exception{
        Thread.sleep(Long.MAX_VALUE);
    }
}
