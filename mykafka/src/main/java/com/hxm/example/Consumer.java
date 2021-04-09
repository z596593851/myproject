package com.hxm.example;

import com.hxm.consumer.ConsumerRecord;
import com.hxm.consumer.ConsumerRecords;
import com.hxm.consumer.KafkaConsumer;

import java.util.Arrays;

public class Consumer {
    public static void main(String[] args) {
        KafkaConsumer<String,String> consumer=new KafkaConsumer<>();
        consumer.subscribe(Arrays.asList("liu"));
        int count=5;
        while (count-->0){
            ConsumerRecords<String,String> consumerRecords=consumer.poll(0);
            for(ConsumerRecord<String,String> consumerRecord:consumerRecords){
                System.out.println(consumerRecord.topic()+"--"+consumerRecord.value());
            }
        }

    }
}
