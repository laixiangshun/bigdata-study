package com.bigdata.study.kafkastream.utils;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

/**
 * 自定义kafka分区规则
 **/
public class HashPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitionInfos = cluster.partitionsForTopic(topic);
        int numPartitions = partitionInfos.size();
        if (keyBytes.length > 1) {
            int hashCode;
            if (key instanceof Integer || key instanceof Long) {
                hashCode = (int) key;
            } else {
                hashCode = key.hashCode();
            }
            hashCode = hashCode & 0x7fffffff;
            return hashCode % numPartitions;
        } else {
            return 0;
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
