package com.hxm;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class ZookeeperTest {
    private String connectStr="localhost:2181";
    private ZooKeeper zkCli;
    private int sessionTimeout=2000;

    @Before
    public void init() throws Exception {
        zkCli=new ZooKeeper(connectStr, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
//                List<String> list= null;
//                try {
//                    list = zkCli.getChildren("/",true);
//                } catch (KeeperException e) {
//                    e.printStackTrace();
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//                list.forEach(System.out::println);
//                System.out.println("----");
            }
        });
    }

    /**
     * 创建节点
     */
    @Test
    public void createNode() throws KeeperException, InterruptedException {
        String path=zkCli.create("/atguigu2","me".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(path);
    }

    /**
     * 获取节点数据
     */
    @Test
    public void getData() throws KeeperException, InterruptedException{
        List<String> list=zkCli.getChildren("/",false);
        list.forEach(System.out::println);
    }

    /**
     * 监听节点变化
     */
    @Test
    public void getDataAndWatch() throws KeeperException, InterruptedException{
       // List<String> list = zkCli.getChildren("/",true);
        Thread.sleep(Long.MAX_VALUE);
    }

    @Test
    public void exist() throws KeeperException, InterruptedException{
        Stat stat=zkCli.exists("/atguigu2",false);
        System.out.println(stat==null?"not exist":"exist");
    }
}
