package com.hxm.example;

import com.hxm.producer.KafkaProducer;
import com.hxm.producer.ProducerRecord;

public class Producer {
    public static void main(String[] args) throws InterruptedException {
        KafkaProducer kafkaProducer=new KafkaProducer();
        kafkaProducer.doSend(new ProducerRecord("liu","cc"),null);
        kafkaProducer.close();
    }
}
