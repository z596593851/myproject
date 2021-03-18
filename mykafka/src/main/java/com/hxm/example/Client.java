package com.hxm.example;

import com.hxm.producer.KafkaProducer;
import com.hxm.producer.ProducerRecord;

public class Client {
    public static void main(String[] args) throws InterruptedException {
        KafkaProducer kafkaProducer=new KafkaProducer();
        kafkaProducer.doSend(new ProducerRecord("linlin","1414jjjjj"),null);
        //Thread.sleep(1000);
        kafkaProducer.close();
//        kafkaProducer.doSend(new ProducerRecord("xiaohong","456"),null);
    }
}
