package com.redhat.training;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class ProducerAcks {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "servera:9091,serverb:9092,serverc:9093");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer .class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            long start = System.currentTimeMillis();
            for (int i = 0; i < 100; i++) {
                String key = UUID.randomUUID().toString();
                String value = key + UUID.randomUUID().toString();
                ProducerRecord<String, String> record = new ProducerRecord<>("REDHAT-TRAINING-ACKS", key, value);
                producer.send(record, (data, ex) -> {
                    if (ex != null) {
                        System.out.println("error *********************************");
                        ex.printStackTrace();
                    } else {
                        System.out.println("======================================================");
                        System.out.println("Offset: " + data.offset());
                    }
                });
            }
            long end = System.currentTimeMillis();
            System.out.println(" ################# Time spent: " + (end - start));
        }
    }

}
