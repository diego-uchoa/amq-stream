package com.redhat.training;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.apache.kafka.common.serialization.Serializer;

public class UserSerializer<User> implements Serializer<User> {

    private final Gson gson = new GsonBuilder().create();

    @Override
    public byte[] serialize(String topic, User user) {
        return gson.toJson(user).getBytes();
    }
}
