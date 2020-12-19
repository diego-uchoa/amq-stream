package com.redhat.training;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class Producer {
    public static void main(String[] args){
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "servera:9092,serverb:9092,serverc:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, UserSerializer.class.getName());

        try (KafkaProducer<String, UserKafka> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < 100; i++) {

                String key = UUID.randomUUID().toString();

                UserKafka value = new UserKafka((456 * i), "Diego", "diego@email.com");

                ProducerRecord<String, UserKafka> record = new ProducerRecord<>("REDHAT_TRAINING_STREAMS_CUSTOM", key, value);

                producer.send(record, (data, ex) -> {
                    if (ex != null) {
                        System.out.println("error *********************************");
                        ex.getStackTrace();
                    } else {
                        System.out.println("======================================================");
                        System.out.println("Offset: " + data.offset());
                    }
                }).get();
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        } finally {
            System.out.println("após envio.");
        }

    }
}
