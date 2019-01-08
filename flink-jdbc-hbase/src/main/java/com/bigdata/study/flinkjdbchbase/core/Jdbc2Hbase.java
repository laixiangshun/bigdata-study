package com.bigdata.study.flinkjdbchbase.core;

import com.bigdata.study.flinkjdbchbase.sink.HbaseSink;
import com.bigdata.study.flinkjdbchbase.source.JdbcSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Description
 * @Author hasee
 * @Date 2019/1/7
 **/
public class Jdbc2Hbase {
    private static final Logger logger = LoggerFactory.getLogger(Jdbc2Hbase.class);

    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(1000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> dataStreamSource = env.addSource(new JdbcSource());
        String hbase_zk = "namenode1.xxx.com";
        String hbase_port = "2181";
        String hbase_table = "ns:table1";
        String hbase_family = "cf1";
        DataStream<String> process = dataStreamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                logger.info("接收到消息：{}", s);
                return s;
            }
        }).process(new HbaseSink(hbase_zk, hbase_port, hbase_table, hbase_family));
        try {
            env.execute("flink from mysql 2 hbase");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
