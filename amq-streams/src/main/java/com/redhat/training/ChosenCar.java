package com.redhat.training;

import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

public class ChosenCar {
    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "redhat-cars-chosen");
        prop.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "servera:9091,serverb:9092,serverc:9092");
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        prop.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        prop.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> textLines = builder.stream("redhat-cars-input-topic");
        KStream<String, String> usersAndCars = textLines
                .filter((key, value) -> value.contains(","))
                .selectKey((key, value) -> value.split(",")[0].toLowerCase())
                .mapValues(value -> value.split(",")[1].toLowerCase())
                .filter((user, car) -> Arrays.asList("vw", "bmw", "audi", "mercedes", "lexus", "honda").contains(car));
        usersAndCars.to("redhat-userkey-cars-topic");
        KTable<String, String> usersAndCarsTable = builder.table("redhat-userkey-cars-topic");
        KTable<String, Long> favouriteCars = usersAndCarsTable
                .groupBy((user, car) -> new KeyValue<>(car, car))
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("CountsByCars")
                        .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()));
        favouriteCars.toStream().to("redhat-cars-output-topic", Produced.with(Serdes.String(), Serdes.Long()));
        KafkaStreams streams = new KafkaStreams(builder.build(), prop);
        streams.start();
        streams.localThreadsMetadata().forEach(data -> System.out.println(data));
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
