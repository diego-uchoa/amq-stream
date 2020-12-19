package com.redhat.training;

import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ExampleProducer {
    
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "servera:9092,serverb:9092,serverc:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.ACKS_CONFIG, "1");
        props.setProperty(ProducerConfig.RETRIES_CONFIG, "10");
        props.setProperty(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "5000");

        try(KafkaProducer<String, String> producer = new KafkaProducer<>(props)){

            String topic = "REDHAT_TRAINING_STREAMS";

            for(int i = 0; i<= 1000; i++){
                String key = UUID.randomUUID().toString();
                String value = key + UUID.randomUUID().toString();

                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
                producer.send(record);

            }

        }   
    }

}
