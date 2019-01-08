package com.bigdata.study.kafkastream.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.xml.internal.ws.encoding.soap.DeserializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

/**
 * @Description
 * @Author hasee
 * @Date 2019/1/8
 **/
public class GenericDeserializer<T> implements Deserializer<T> {
    private Class<T> type;
    private static ObjectMapper mapper = new ObjectMapper();

    public GenericDeserializer() {
    }

    public GenericDeserializer(Class<T> type) {
        this.type = type;
    }

    @Override
    public void configure(Map<String, ?> map, boolean b) {
        if (type != null) {
            return;
        }
        String typeProp = b ? "key.deserializer.type" : "value.deserializer.type";
        String typeName = String.valueOf(map.get(typeProp));
        try {
            type = (Class<T>) Class.forName(typeName);
        } catch (ClassNotFoundException e) {
            throw new DeserializationException("failed to initialize GenericDeserializer for " + typeName, e);
        }
    }

    @Override
    public T deserialize(String s, byte[] bytes) {
        if (bytes.length < 1) {
            return null;
        }
        try {
            return mapper.readValue(bytes, type);
        } catch (IOException e) {
            throw new DeserializationException(e);
        }
    }

    @Override
    public void close() {

    }
}
