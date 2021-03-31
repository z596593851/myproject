package com.hxm.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class MyConsumer {
    public static void main(String[] args) {
        Properties properties=new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"group1");
        KafkaConsumer<String,String> consumer=new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList("xiaoming"));
        while (true){
            ConsumerRecords<String,String> consumerRecords=consumer.poll(0);
            for(ConsumerRecord<String,String> consumerRecord:consumerRecords){
                System.out.println(consumerRecord.key()+"--"+consumerRecord.value());
            }
        }
    }



}
