package com.bigdata.study.flinkkafkasource;

import com.bigdata.study.flinkkafkasource.watermarks.ConsumerWaterMarkEmitter;
import constant.PropertiesConstants;
import model.Metrics;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import schemas.MetricSchema;
import utils.ExecutionEnvUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@SpringBootApplication
public class FlinkKafkaSourceApplication {

    public static void main(String[] args) {
//        SpringApplication.run(FlinkKafkaSourceApplication.class, args);
        try {
            ParameterTool parameterPool = ExecutionEnvUtil.createParameterPool(args);
            StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterPool);

            Properties properties = new Properties();
            properties.put("bootstrap.servers", parameterPool.get(PropertiesConstants.KAFKA_BROKERS));
            properties.put("zookeeper.connect", parameterPool.get(PropertiesConstants.KAFKA_ZOOKEEPER_CONNECT));
            properties.put("group.id", parameterPool.get(PropertiesConstants.KAFKA_GROUP_ID));
            properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            properties.put("auto.offset.reset", "latest");

            String topic = parameterPool.getRequired(PropertiesConstants.METRICS_TOPIC);
            long consumerTime = parameterPool.getLong(PropertiesConstants.CONSUMER_FROM_TIME, 0L);

            FlinkKafkaConsumer011<Metrics> consumer011 = new FlinkKafkaConsumer011<>(topic, new MetricSchema(), properties);

            //设置消费者开始位置
            //指定消费者应从每个分区开始的确切偏移量
            if (consumerTime != 0L) {
                properties.setProperty("group.id", "query_time_" + consumerTime);
                KafkaConsumer consumer = new KafkaConsumer(properties);
                List<PartitionInfo> partitionsFor = consumer.partitionsFor(topic);
                Map<TopicPartition, Long> topicPartitionMap = new HashMap<>();
                for (PartitionInfo partitionInfo : partitionsFor) {
                    topicPartitionMap.put(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()), consumerTime);
                }
                Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = consumer.offsetsForTimes(topicPartitionMap);
                Map<KafkaTopicPartition, Long> kafkaTopicPartitionMap = new HashMap<>();
                for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : offsetsForTimes.entrySet()) {
                    TopicPartition topicPartition = entry.getKey();
                    KafkaTopicPartition kafkaTopicPartition = new KafkaTopicPartition(topicPartition.topic(), topicPartition.partition());
                    kafkaTopicPartitionMap.put(kafkaTopicPartition, entry.getValue().offset());
                }
                consumer.close();
                consumer011.setStartFromSpecificOffsets(kafkaTopicPartitionMap);
            }
            //指定自定义水印发射器
            consumer011.assignTimestampsAndWatermarks(new ConsumerWaterMarkEmitter());
            DataStreamSource<Metrics> streamSource = env.addSource(consumer011);
            streamSource.print();
            env.execute("flink kafka source");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

