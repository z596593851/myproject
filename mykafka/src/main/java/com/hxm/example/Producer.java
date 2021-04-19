package com.hxm.example;

import com.hxm.client.client.producer.KafkaProducer;
import com.hxm.client.client.producer.ProducerRecord;

public class Producer {
    public static void main(String[] args) throws InterruptedException {
        KafkaProducer kafkaProducer=new KafkaProducer();
        kafkaProducer.doSend(new ProducerRecord("liu","你妈的"),null);
        kafkaProducer.doSend(new ProducerRecord("liu","你妈的"),null);
        System.out.println("kafkaProducer is closing...");
        kafkaProducer.close();
    }
}
