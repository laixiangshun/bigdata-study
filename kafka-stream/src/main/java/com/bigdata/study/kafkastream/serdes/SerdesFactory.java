package com.bigdata.study.kafkastream.serdes;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

/**
 * @Description
 * @Author hasee
 * @Date 2019/1/8
 **/
public class SerdesFactory {

    public static <T> Serde<T> serdeFrom(Class<T> tClass) {
        return Serdes.serdeFrom(new GenericSerializer<>(tClass), new GenericDeserializer<>(tClass));
    }
}
