package com.redhat.training;

import java.util.ArrayList;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class BatchProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "33554432");
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "5000");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ArrayList<ProducerRecord<String, String>> lista = new ArrayList<ProducerRecord<String, String>>();
        for (int i = 0; i < 1000; i++) {

            String key = "" + UUID.randomUUID();
            String value = key + UUID.randomUUID();

            ProducerRecord<String, String> record = new ProducerRecord<>("my-topic", key, value);

            lista.add(record);
        }
        int j = 0;
        while (j < 1000) {
            ProducerRecord<String, String> e = lista.get(j);

            try {
                System.out.println(" ==================== " + e.toString());
                producer.send(e, (data, ex) -> {
                    if(ex != null){
                        System.out.println("Deu erro");
                    }else{
                        System.out.println("Gravado no offset: " + data.offset());
                    }
                }).get();
            } catch (InterruptedException | ExecutionException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            j++;
        }
        producer.close();

    }
}