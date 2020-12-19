package com.redhat.training;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumerOffset {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "servera:9092,serverb:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "CONSUMER_FROM_BEGINING");

        try(KafkaConsumer<String,String> consumer = new KafkaConsumer<>(props)){

            consumer.subscribe(Collections.singletonList("REDHAT_TRAINING_STREAMS"));

            Set<TopicPartition> assignment = new HashSet<>();
            while (assignment.size() == 0) {
                consumer.poll(Duration.ofMillis(100));
                assignment = consumer.assignment();
            }
            //Step 1
            //consumer.seekToBeginning(assignment);

            //Step 2
            for (TopicPartition tp : assignment) {
                consumer.seek(tp, 995);
            }
            
            while(true){
                ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(1000));
                if(records.isEmpty()){
                    System.out.println("There are no records to show!!!");
                }else {

                    for (ConsumerRecord<String, String> record : records) {
                        System.out.println("======================================================");
                        System.out.println("Offset: " + record.offset());
                        System.out.println("Value: " + record.value());
                    }
                }

            }

        }
    }
}
