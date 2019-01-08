package com.bigdata.study.kafkastream.producer;

import com.bigdata.study.kafkastream.model.Item;
import com.bigdata.study.kafkastream.serdes.GenericSerializer;
import com.bigdata.study.kafkastream.utils.HashPartitioner;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * 商品生产者
 **/
public class ItemProducer {
    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.put("bootstrap.servers", "192.168.20.48:9092");
        prop.put("zookeeper.connect", "192.168.20.48:2181,192.168.20.51:2181,192.168.20.52:2181");
        prop.put("acks", "all");
        prop.put("retries", 3);
        prop.put("batch.size", 16348);
        prop.put("linger.ms", 1);
        prop.put("buffer.memory", 33554432);
        prop.put("key.serializer", StringSerializer.class.getCanonicalName());
        prop.put("value.serializer", GenericSerializer.class.getCanonicalName());
        prop.put("value.serializer.type", Item.class.getCanonicalName());
        prop.put("partitioner.class", HashPartitioner.class.getCanonicalName());
        KafkaProducer<String, Item> kafkaProducer = new KafkaProducer<>(prop);
        try {
            List<Item> items = readItem();
            items.forEach(item -> {
                ProducerRecord<String, Item> record = new ProducerRecord<>("items", item.getItemName(), item);
                kafkaProducer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            System.err.printf("发送商品消息[topic:%s,partition:%d,offset:%d,keysize:%d,valuesize:%d]失败",
                                    recordMetadata.topic(),
                                    recordMetadata.partition(),
                                    recordMetadata.offset(),
                                    recordMetadata.serializedKeySize(),
                                    recordMetadata.serializedValueSize());
                            e.printStackTrace();
                        }
                        System.out.printf("成功发送商品消息[topic:%s,partition:%d,offset:%d,keysize:%d,valuesize:%d]",
                                recordMetadata.topic(),
                                recordMetadata.partition(),
                                recordMetadata.offset(),
                                recordMetadata.serializedKeySize(),
                                recordMetadata.serializedValueSize());
                    }
                });
            });
        } catch (IOException e) {
            throw new KafkaException("发送商品数据到kafka出错", e);
        } finally {
            kafkaProducer.close();
        }
    }

    private static List<Item> readItem() throws IOException {
        List<String> lines = IOUtils.readLines(ItemProducer.class.getResourceAsStream("/items.csv"), Charset.forName("utf-8"));
        return lines.stream().filter(StringUtils::isNotBlank)
                .map(line -> line.split("\\s*,\\s*"))
                .filter(value -> value.length == 4)
                .map(value -> new Item(value[0], value[1], value[2], Double.parseDouble(value[3])))
                .collect(Collectors.toList());
    }
}
