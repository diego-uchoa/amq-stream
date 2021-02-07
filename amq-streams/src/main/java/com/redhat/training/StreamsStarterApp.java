package com.redhat.training;

import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

public class StreamsStarterApp {
    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "redhat-word-count");
        prop.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "servera:9091,serverb:9092,serverc:9092");
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        prop.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        prop.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> wordCountInput = builder.stream("redhat-word-input");
        KTable<String, Long> wordCounts = wordCountInput
                // Transforming all to lower case
                .mapValues(textLine -> textLine.toLowerCase())
                // splitting by space
                .flatMapValues(lowercasedTextLine -> Arrays.asList(lowercasedTextLine.split(" ")))
                // Transforming the key equal to the value
                .selectKey((ignoredKey, word) -> word)
                // grouping by the key
                .groupByKey()
                // performing the count operation
                .count(Materialized.as("Counts"));

        wordCounts.toStream().to("redhat-word-output", Produced.with(Serdes.String(), Serdes.Long()));
        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, prop);
        streams.start();
        System.out.println(topology.describe());
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}