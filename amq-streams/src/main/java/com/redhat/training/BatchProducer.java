package com.redhat.training;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class BatchProducer {
    public static void main(String[] args) throws IOException {
        Logger.getRootLogger().setLevel(Level.TRACE);
        Logger.getLogger(org.apache.kafka.clients.producer.internals.Sender.class).setLevel(Level.TRACE);
        Layout layout = new PatternLayout("[%d] %p %m (%c)%n");
        Logger.getLogger(org.apache.kafka.clients.producer.internals.Sender.class).addAppender(new ConsoleAppender(layout));
        Logger.getRootLogger().addAppender(new ConsoleAppender(layout));
        Logger.getRootLogger().addAppender(new FileAppender(layout, "git-logger.log", true));

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.RETRIES_CONFIG,"5");
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "2000");
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "16384");//65536
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "10000");

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

            System.out.println(" ==================== " + e.toString());

            producer.send(e, (data, ex) -> {
                if(ex != null){
                    System.out.println("Deu erro");
                }else{
                    System.out.println("Gravado no offset: " + data.offset());
                }
            });

            j++;
        }
        producer.close();

    }
}