package com.bigdata.study.kafkastream.timeextractor;

import com.bigdata.study.kafkastream.model.Item;
import com.bigdata.study.kafkastream.model.Order;
import com.bigdata.study.kafkastream.model.User;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * 自定义从topic中抽取时间
 **/
public class OrderTimestampExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord) {
        Object value = consumerRecord.value();
        if (value instanceof Order) {
            return ((Order) value).getTransactionDate();
        } else if (value instanceof JsonNode) {
            return ((JsonNode) value).get("transactionDate").longValue();
        } else if (value instanceof User) {
            return LocalDateTime.of(2015, 12, 11, 1, 0, 10)
                    .toEpochSecond(ZoneOffset.UTC) * 1000;
        } else if (value instanceof Item) {
            return LocalDateTime.of(2015, 12, 11, 0, 0, 10).toEpochSecond(ZoneOffset.UTC) * 1000;
        } else {
            return LocalDateTime.of(2015, 11, 10, 0, 0, 10).toEpochSecond(ZoneOffset.UTC) * 1000;
        }
    }
}
