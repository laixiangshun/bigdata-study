package com.bigdata.study.dataflowstreamredispubsink.prop;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @Description
 * @Author hasee
 * @Date 2019/1/2
 **/
@ConfigurationProperties("redis")
public class RedisPubProperties {

    private String topic;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }
}
