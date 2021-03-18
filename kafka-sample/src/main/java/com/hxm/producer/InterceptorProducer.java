package com.hxm.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class InterceptorProducer {
    public static void main(String[] args) {
        Properties properties=new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        //拦截器
        List<String> interceptors=new ArrayList<>(2);
        interceptors.add("com.hxm.interceptor.TimeInterceptor");
        interceptors.add("com.hxm.interceptor.CounterInterceptor");
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,interceptors);

        KafkaProducer<String,String> producer=new KafkaProducer<>(properties);
        for(int i=0;i<10;i++){
            producer.send(new ProducerRecord<>("xiaoming", "msg--"+i), (metadata, exception)->{
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
