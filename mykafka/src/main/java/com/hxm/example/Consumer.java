package com.hxm.example;

import com.hxm.client.client.consumer.ConsumerRecord;
import com.hxm.client.client.consumer.ConsumerRecords;
import com.hxm.client.client.consumer.KafkaConsumer;

import java.util.Arrays;

public class Consumer {
    public static void main(String[] args) {
        KafkaConsumer<String,String> consumer=new KafkaConsumer<>();
        consumer.subscribe(Arrays.asList("liu"));
        int count=4;
        while (true){
            ConsumerRecords<String,String> consumerRecords=consumer.poll(1000);
            for(ConsumerRecord<String,String> consumerRecord:consumerRecords){
                System.out.println(consumerRecord.topic()+"--"+consumerRecord.value());
            }
        }

    }
}
