package com.redhat.training;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class ConsumerDLQ {
    public static void main(String[] args) {

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "servera:9092,serverb:9092,serverc:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "CONSUMER-DLQ");
        try(KafkaConsumer<String,String> consumer = new KafkaConsumer<>(props)){
            consumer.subscribe(Collections.singletonList("REDHAT_TRAINING_STREAMS_ERROR"));
            while(true){
                ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        throw new RuntimeException("Sending message to DLQ!!!");
                        
                    } catch (Exception e) {
                        System.out.println(e.getMessage());
                        sendDLQ("REDHAT_TRAINING_STREAMS_DLQ", record.key(), record.value());
                    }
                }
            }
        }
    }

    private static void sendDLQ(String topic, String key, String value){
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "servera:9092,serverb:9092,serverc:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.RETRIES_CONFIG, "10");
        props.setProperty(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "5000");
        try(KafkaProducer<String, String> producer = new KafkaProducer<>(props)){
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            producer.send(record, (data, ex) -> {
                if(ex != null){
                    System.out.println("Error sending message to DLQ");
                } else {
                    System.out.println("Sending message to DLQ - " + topic);
                    System.out.println("Message written in the offset: " + data.offset());
                    System.out.println("Message written in the partition: " + data.partition());
                }
            });
        }
    }
}
