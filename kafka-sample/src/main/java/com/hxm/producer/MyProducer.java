package com.hxm.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class MyProducer {
    public static void main(String[] args) {
        Properties properties=new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String,String> producer=new KafkaProducer<>(properties);
        for(int i=0;i<10;i++){
            producer.send(new ProducerRecord<>("xiaoming", "msg--"+i), (metadata,exception)->{
                if(exception==null){
                    System.out.println(metadata.partition()+"--"+metadata.offset());
                }else {
                    exception.printStackTrace();
                }
            });
        }

        producer.close();

    }
}
