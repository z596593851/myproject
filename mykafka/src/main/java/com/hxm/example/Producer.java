package com.hxm.example;

import com.hxm.client.client.producer.KafkaProducer;
import com.hxm.client.client.producer.ProducerRecord;

public class Producer {
    public static void main(String[] args) throws InterruptedException {
        KafkaProducer kafkaProducer=new KafkaProducer();
        int count=1;

        while(count<=8){
            kafkaProducer.doSend(new ProducerRecord("liu","嗯嗯"+count),null);
            count++;
        }
        System.out.println("kafkaProducer is closing...");
        kafkaProducer.close();
    }
}
