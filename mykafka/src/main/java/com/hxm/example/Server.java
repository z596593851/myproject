package com.hxm.example;

import com.hxm.broker.KafkaRequestHandlerPool;
import com.hxm.broker.KafkaScheduler;
import com.hxm.broker.ReplicaManager;
import com.hxm.broker.SocketServer;
import com.hxm.log.LogManager;
import com.hxm.producer.Time;
import com.hxm.producer.TopicPartition;

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
        LogManager logManager=new LogManager(files,2,kafkaScheduler,2000,60000);
        logManager.startup();
        SocketServer socketServer=new SocketServer("127.0.0.1",6666);
        socketServer.startup();

        ReplicaManager replicaManager=new ReplicaManager(logManager,time);
        List<TopicPartition> topicPartitionList=new ArrayList<>(1);
        topicPartitionList.add(new TopicPartition("xiaoming",0));
        topicPartitionList.add(new TopicPartition("linlin",0));
        replicaManager.becomeLeaderOrFollower(topicPartitionList);

        KafkaRequestHandlerPool pool=new KafkaRequestHandlerPool(2,socketServer.getRequestChannel(),replicaManager);


    }
}
