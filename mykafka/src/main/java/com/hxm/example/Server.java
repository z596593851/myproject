package com.hxm.example;

import com.hxm.core.server.KafkaRequestHandlerPool;
import com.hxm.core.utils.KafkaScheduler;
import com.hxm.core.server.ReplicaManager;
import com.hxm.core.network.SocketServer;
import com.hxm.core.log.LogManager;
import com.hxm.client.common.utils.Time;
import com.hxm.client.common.TopicPartition;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class Server {
    private static String path="/Users/apple/Desktop/log";

    public static void main(String[] args) {
        File[] files= {new File(path)};
        Time time=new Time();
        KafkaScheduler kafkaScheduler=new KafkaScheduler(2);
        kafkaScheduler.startup();
        //flushCheckMs:log刷盘间隔  flushCheckpointMs：checkpoints刷盘间隔
        LogManager logManager=new LogManager(files,2,kafkaScheduler,2000,2000);
        logManager.startup();
        SocketServer socketServer=new SocketServer("127.0.0.1",6666);
        socketServer.startup();

        ReplicaManager replicaManager=new ReplicaManager(logManager,time);
        List<TopicPartition> topicPartitionList=new ArrayList<>(1);
        topicPartitionList.add(new TopicPartition("liu",0));
        replicaManager.becomeLeaderOrFollower(topicPartitionList);

        KafkaRequestHandlerPool pool=new KafkaRequestHandlerPool(2,socketServer.getRequestChannel(),replicaManager);


    }
}
