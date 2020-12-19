package com.redhat.training;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class Consumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "servera:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, UserDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "Consumer1");
        props.setProperty(UserDeserializer.TYPE_CONFIG, UserKafka.class.getName());

        try(KafkaConsumer<String,UserKafka> consumer = new KafkaConsumer<>(props)){

            consumer.subscribe(Collections.singletonList("REDHAT_TRAINING_STREAMS_CUSTOM"));

            while(true){
                ConsumerRecords<String,UserKafka> records = consumer.poll(Duration.ofMillis(1000));
                if(records.isEmpty()){
                    System.out.println("não existem registros!!!");
                }else {

                    for (ConsumerRecord<String, UserKafka> record : records) {
                        System.out.println("======================================================");
                        System.out.println("Offset: " + record.offset());
                        System.out.println("Value: " + record.value());
                    }
                }

            }

        }
    }
}
