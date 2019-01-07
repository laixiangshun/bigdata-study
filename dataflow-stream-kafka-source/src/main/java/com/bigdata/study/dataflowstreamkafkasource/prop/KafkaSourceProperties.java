package com.bigdata.study.dataflowstreamkafkasource.prop;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * kafka 配置属性
 **/
@ConfigurationProperties("kafka")
public class KafkaSourceProperties {
    private String topic = "test";

    private String servers = "ebmas-02:6667,ebmas-01:6667,ebmas-03:6667";

    private String groupId = "test-group";

    private long batchSize = 1024;

    private String zkNodes="192.168.10.120:2181,192.168.10.121:2181,192.168.10.122:2181";

    private String keyDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";

    private String valueDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getServers() {
        return servers;
    }

    public void setServers(String servers) {
        this.servers = servers;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getZkNodes() {
        return zkNodes;
    }

    public void setZkNodes(String zkNodes) {
        this.zkNodes = zkNodes;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public long getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(long batchSize) {
        this.batchSize = batchSize;
    }

    public String getKeyDeserializer() {
        return keyDeserializer;
    }

    public void setKeyDeserializer(String keyDeserializer) {
        this.keyDeserializer = keyDeserializer;
    }

    public String getValueDeserializer() {
        return valueDeserializer;
    }

    public void setValueDeserializer(String valueDeserializer) {
        this.valueDeserializer = valueDeserializer;
    }
}
