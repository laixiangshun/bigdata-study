package com.bigdata.study.flinkkafkahbase.core;

import com.bigdata.study.flinkkafkahbase.source.FlinkHbaseSource;
import com.bigdata.study.flinkkafkahbase.watermarks.FlinkHbaseWaterMarks;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * 从Hbase读取数据到kafka
 **/
public class Hbase2Kafka {
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) {
        final String ZOOKEEPER_HOST = "192.168.20.48:2181,192.168.20.51:2181,192.168.20.52:2181";
        final String KAFKA_HOST = "192.168.20.48:9092";
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<Map<String, String>> dataStreamSource = env.addSource(new FlinkHbaseSource());
        dataStreamSource.assignTimestampsAndWatermarks(new FlinkHbaseWaterMarks());
        DataStream<String> dataStream = dataStreamSource.filter(data -> CollectionUtils.isNotEmpty(Collections.singleton(data))).flatMap(new FlatMapFunction<Map<String, String>, String>() {
            @Override
            public void flatMap(Map<String, String> stringStringMap, Collector<String> collector) throws Exception {
                String value = mapper.writeValueAsString(stringStringMap);
                collector.collect(value);
            }
        });
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", KAFKA_HOST);
        prop.put("zookeeper.connect", ZOOKEEPER_HOST);
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("auto.offset.reset", "latest");
        FlinkKafkaProducer011<String> producer011 = new FlinkKafkaProducer011<>("hbase-kafka", new SimpleStringSchema(), prop);
        producer011.setWriteTimestampToKafka(true);
        dataStream.addSink(producer011);
        try {
            env.execute("flink hbase 2 kafka11");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
