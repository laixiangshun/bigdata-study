package com.bigdata.study.dataflowstreamkafkasource.config;

import com.bigdata.study.dataflowstreamkafkasource.prop.KafkaSourceProperties;
import com.bigdata.study.dataflowstreamkafkasource.utils.JsonMapper;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * kafka source
 **/
@Configuration
@EnableConfigurationProperties(KafkaSourceProperties.class)
@EnableBinding(Source.class)
public class KafkaSourceConfig implements InitializingBean {

    private StreamsConfig streamsConfig;

    @Autowired
    private KafkaSourceProperties kafkaSourceProperties;

    @Autowired
    private Source source;

    //    @InboundChannelAdapter(channel = Source.OUTPUT)
    public void sendMessage() {
        KStreamBuilder builder = new KStreamBuilder();
        builder.stream(kafkaSourceProperties.getTopic()).foreach(new ForeachAction<Object, Object>() {
            @Override
            public void apply(Object key, Object value) {
                Message<Map> message;
                try {
                    Map map = JsonMapper.defaultMapper().fromJson(String.valueOf(value), Map.class);
                    message = MessageBuilder.withPayload(map).build();
                    source.output().send(message);
                    System.out.println("成功发送消息：" + message.getPayload());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
        KafkaStreams kafkaStreams = new KafkaStreams(builder, streamsConfig);
        kafkaStreams.start();
    }


    @Override
    public void afterPropertiesSet() {
        Map<String, Object> prop = new HashMap<>();
        prop.put(StreamsConfig.APPLICATION_ID_CONFIG, kafkaSourceProperties.getGroupId());
        prop.put("bootstrap.servers", kafkaSourceProperties.getServers());
        prop.put("zookeeper.connect", kafkaSourceProperties.getZkNodes());
        prop.put("key.serde", kafkaSourceProperties.getKeyDeserializer());
        prop.put("value.serde", kafkaSourceProperties.getValueDeserializer());
        prop.put("batch.size", kafkaSourceProperties.getBatchSize());
        streamsConfig = new StreamsConfig(prop);
    }
}
