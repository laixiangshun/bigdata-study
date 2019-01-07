package com.bigdata.study.dataflowstreamredispubsink.config;

import com.bigdata.study.dataflowstreamredispubsink.prop.RedisPubProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.messaging.Message;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * @Description
 * @Author hasee
 * @Date 2019/1/2
 **/
@Configuration
@EnableConfigurationProperties(RedisPubProperties.class)
@EnableBinding(Sink.class)
public class RedisStreamPubConfig implements InitializingBean {

    @Autowired
    private RedisPubProperties redisPubProperties;

    @Autowired
    private RedisTemplate<String, String> redisTemplate;

    private ChannelTopic topic;

    private static final ObjectMapper mapper = new ObjectMapper();

    @StreamListener(value = Sink.INPUT)
    public void pubRedis(Message<?> message) {
        try {
            redisTemplate.convertAndSend(topic.getTopic(), mapper.writeValueAsString(message.getPayload()));
            System.out.println("向redis中发送消息：" + mapper.writeValueAsString(message.getPayload()));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        topic = new ChannelTopic(redisPubProperties.getTopic());
    }
}
