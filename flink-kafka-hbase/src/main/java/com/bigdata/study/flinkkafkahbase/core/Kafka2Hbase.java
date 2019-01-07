package com.bigdata.study.flinkkafkahbase.core;

import com.bigdata.study.flinkkafkahbase.model.Metric;
import com.bigdata.study.flinkkafkahbase.sink.FlinkHbaseSink;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.hadoop.hbase.TableName;

import java.util.Properties;

/**
 * flink 处理数据从kafka到hbase
 **/
public class Kafka2Hbase {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) {
        final String ZOOKEEPER_HOST = "192.168.20.48:2181,192.168.20.51:2181,192.168.20.52:2181";
        final String KAFKA_HOST = "192.168.20.48:9092";
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", KAFKA_HOST);
        prop.put("zookeeper.connect", ZOOKEEPER_HOST);
        prop.put("group.id", "kafka-hbase-group");
        DataStreamSource<String> dataStreamSource = env.addSource(new FlinkKafkaConsumer011<>("kafka-hbase", new SimpleStringSchema(), prop));
        DataStream<Metric> metricDataStream = dataStreamSource.rebalance().filter(StringUtils::isNotBlank).map(m -> {
            Metric metric = mapper.readValue(m, Metric.class);
            return metric;
        });
        metricDataStream.addSink(new FlinkHbaseSink());
        env.setParallelism(2);
        try {
            env.execute("flink kafka hbase sink");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
