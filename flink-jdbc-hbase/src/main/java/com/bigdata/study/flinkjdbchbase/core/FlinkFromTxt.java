package com.bigdata.study.flinkjdbchbase.core;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @Description
 * @Author hasee
 * @Date 2019/1/7
 **/
public class FlinkFromTxt {

    public static void main(String[] args) {
        String file_input = "C:\\Users\\hasee\\Desktop\\spark.txt";
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        DataStream<String> dataStreamSource = env.readTextFile(file_input);
        DataStream<Tuple2<String, Integer>> reduce = dataStreamSource.filter(StringUtils::isNotBlank)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.toLowerCase().split("\\W+");
                for (String word : words) {
                    if (word.length() > 0) {
                        Tuple2<String, Integer> tuple2 = new Tuple2<>();
                        tuple2.f0 = word;
                        tuple2.f1 = 1;
                        collector.collect(tuple2);
                    }
                }
            }
        }).keyBy(0).timeWindow(Time.seconds(30)).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
                return new Tuple2<>(t1.f0, t1.f1 + t2.f1);
            }
        });
        reduce.print();
        try {
            env.execute("flink read txt");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
