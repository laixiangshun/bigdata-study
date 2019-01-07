package utils;

import constant.PropertiesConstants;
import model.Metrics;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import schemas.MetricSchema;
import watermarks.MetricWatermark;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * kafka 工具类
 **/
public class KafkaUtils {

    private static Properties buildKafkaProp(ParameterTool parameterTool) {
        Properties properties = parameterTool.getProperties();
        properties.put("bootstrap.servers", parameterTool.get(PropertiesConstants.KAFKA_BROKERS));
        properties.put("zookeeper.connect", parameterTool.get(PropertiesConstants.KAFKA_ZOOKEEPER_CONNECT));
        properties.put("group.id", parameterTool.get(PropertiesConstants.KAFKA_GROUP_ID));
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("auto.offset.reset", "latest");
        return properties;
    }

    public static DataStreamSource<Metrics> buildSource(StreamExecutionEnvironment env) {
        ParameterTool parameterTool = (ParameterTool) env.getConfig().getGlobalJobParameters();
        String topic = parameterTool.getRequired(PropertiesConstants.METRICS_TOPIC);
        long consumerTime = parameterTool.getLong(PropertiesConstants.CONSUMER_FROM_TIME, 0L);
        return buildSource(env, topic, consumerTime);
    }

    public static DataStreamSource<Metrics> buildSource(StreamExecutionEnvironment env, String topic, Long time) {
        ParameterTool parameterTool = (ParameterTool) env.getConfig().getGlobalJobParameters();
        Properties properties = buildKafkaProp(parameterTool);
        FlinkKafkaConsumer011<Metrics> consumer011 = new FlinkKafkaConsumer011<>(topic, new MetricSchema(), properties);
        //重装offset到time处
        if (time != null && time != 0L) {
            properties.setProperty("group.id", "query_time_" + time);
            KafkaConsumer consumer = new KafkaConsumer(properties);
            List<PartitionInfo> partitionsFor = consumer.partitionsFor(PropertiesConstants.METRICS_TOPIC);
            Map<TopicPartition, Long> partitionLongMap = new HashMap<>();
            for (PartitionInfo partitionInfo : partitionsFor) {
                partitionLongMap.put(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()), time);
            }
            Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = consumer.offsetsForTimes(partitionLongMap);
            Map<KafkaTopicPartition, Long> partitionOffsetMap = new HashMap<>();
            for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : offsetsForTimes.entrySet()) {
                TopicPartition topicPartition = entry.getKey();
                partitionOffsetMap.put(new KafkaTopicPartition(topicPartition.topic(), topicPartition.partition()), entry.getValue().offset());
            }
            consumer.close();
            consumer011.setStartFromSpecificOffsets(partitionOffsetMap);
        }
        return env.addSource(consumer011);
    }

    public static SingleOutputStreamOperator<Metrics> parseSource(DataStreamSource<Metrics> dataStreamSource) {
        return dataStreamSource.assignTimestampsAndWatermarks(new MetricWatermark());
    }
}
