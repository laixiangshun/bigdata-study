package com.bigdata.study.flinkkafkasink;

import constant.PropertiesConstants;
import model.Metrics;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import schemas.MetricSchema;
import utils.ExecutionEnvUtil;
import utils.KafkaUtils;

/**
 * flink从kafka中读取数据，并写入kafka中
 */
@SpringBootApplication
public class FlinkKafkaSinkApplication {

    public static void main(String[] args) {
//        SpringApplication.run(FlinkKafkaSinkApplication.class, args);
        try {
            ParameterTool parameterPool = ExecutionEnvUtil.createParameterPool(args);
            StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterPool);
            DataStreamSource<Metrics> dataStreamSource = KafkaUtils.buildSource(env);
            String brokers = parameterPool.get(PropertiesConstants.KAFKA_BROKERS);
            FlinkKafkaProducer011<Metrics> kafkaProducer011 = new FlinkKafkaProducer011<>(brokers,
                    parameterPool.get(PropertiesConstants.METRICS_TOPIC), new MetricSchema());
            kafkaProducer011.setWriteTimestampToKafka(true);
            kafkaProducer011.setLogFailuresOnly(false);
            dataStreamSource.addSink(kafkaProducer011);
            env.execute("flink kafka sink");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

