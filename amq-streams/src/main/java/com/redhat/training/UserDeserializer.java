package com.redhat.training;

import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.apache.kafka.common.serialization.Deserializer;

public class UserDeserializer<UserKafka> implements Deserializer<UserKafka>{

    private final Gson gson = new GsonBuilder().create();
    public static final String TYPE_CONFIG = "";
    private Class<UserKafka> cls;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        try {
            cls = (Class<UserKafka>) Class.forName(String.valueOf(configs.get(TYPE_CONFIG)));
        } catch (ClassNotFoundException e){
            System.out.println("Error, class not found in the classpath!");
            e.printStackTrace();
        }
    }

    @Override
    public UserKafka deserialize(String topic, byte[] bytes) {
        return gson.fromJson(new String(bytes), cls);
    }
    
}
