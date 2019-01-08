package com.bigdata.study.kafkastream.serdes;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * @Description
 * @Author hasee
 * @Date 2019/1/8
 **/
public class GenericSerializer<T> implements Serializer<T> {
    private Class<T> tClass;
    private static ObjectMapper mapper = new ObjectMapper();

    public GenericSerializer() {
    }

    public GenericSerializer(Class<T> tClass) {
        this.tClass = tClass;
    }

    @Override
    public void configure(Map<String, ?> map, boolean b) {
        if (tClass != null) {
            return;
        }
        String type = b ? "key.serializer.type" : "value.serializer.type";
        String typeName = String.valueOf(map.get(type));
        try {
            tClass = (Class<T>) Class.forName(typeName);
        } catch (ClassNotFoundException e) {
            throw new SerializationException("failed to initialize GenericSerializer:+" + typeName, e);
        }
    }

    @Override
    public byte[] serialize(String s, T t) {
        if (t == null) {
            return new byte[0];
        }
        try {
            return mapper.writerFor(tClass).writeValueAsBytes(t);
        } catch (JsonProcessingException e) {
            throw new SerializationException(e);
        }
    }

    @Override
    public void close() {

    }
}
