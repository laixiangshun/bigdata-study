package com.bigdata.study.flinkhdfs.core;

import com.bigdata.study.flinkhdfs.utils.HadoopConfig;
import com.bigdata.study.flinkhdfs.utils.HadoopHelper;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.fs.Clock;
import org.apache.flink.streaming.connectors.fs.StringWriter;
import org.apache.flink.streaming.connectors.fs.bucketing.Bucketer;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.Map;

/**
 * @Description
 * @Author hasee
 * @Date 2019/1/7
 **/
public class FlinkHdfs implements CommandLineRunner {

    @Autowired
    private HadoopConfig hadoopConfig;

    @Override
    public void run(String... strings) throws Exception {
        String file_input = "C:\\Users\\hasee\\Desktop\\spark.txt";
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(2);
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
        DataStream<Tuple2<IntWritable, Text>> hdfsStream = reduce.flatMap(new FlatMapFunction<Tuple2<String, Integer>, Tuple2<IntWritable, Text>>() {
            @Override
            public void flatMap(Tuple2<String, Integer> in, Collector<Tuple2<IntWritable, Text>> collector) throws Exception {
                Tuple2<IntWritable, Text> tuple2 = new Tuple2<>();
                tuple2.f0 = new IntWritable(in.f1);
                tuple2.f1 = new Text(in.f0);
                collector.collect(tuple2);
            }
        });
        DataStream<String> outStream = hdfsStream.flatMap(new FlatMapFunction<Tuple2<IntWritable, Text>, String>() {
            @Override
            public void flatMap(Tuple2<IntWritable, Text> in, Collector<String> collector) throws Exception {
                StringBuilder builder = new StringBuilder();
                String name = in.f1.toString();
                int num = in.f0.get();
                builder.append(name).append("\t").append(num).append("\n");
                collector.collect(builder.toString());
            }
        });
        BucketingSink<String> bucketingSink = new BucketingSink<>("/base/path");
//        bucketingSink.setBucketer(new DateTimeBucketer<>("yyyy-MM-dd--HHmm"));
        //使用自定义分桶
        bucketingSink.setBucketer(new DateHourBucketer());
        bucketingSink.setWriter(new StringWriter<>());
        bucketingSink.setBatchSize(1024 * 1024 * 4);
        bucketingSink.setBatchRolloverInterval(Integer.MAX_VALUE);
        bucketingSink.setInactiveBucketCheckInterval(60);
        bucketingSink.setInactiveBucketThreshold(60);
        HadoopHelper hadoopHelper = new HadoopHelper(hadoopConfig);
        Configuration configuration = hadoopHelper.getConfig();
        bucketingSink.setFSConfig(configuration);
        outStream.addSink(bucketingSink);
    }

    private static ObjectMapper mapper = new ObjectMapper();

    /**
     * 自定义hdfs分桶规则
     */
    private class DateHourBucketer implements Bucketer<String> {
        @Override
        public Path getBucketPath(Clock clock, Path path, String s) {
            try {
                Map map = mapper.readValue(s, Map.class);
                Long timeStamp = (Long) map.get("TimeStamp");
                Date date = new Date(timeStamp);
                String format = DateFormatUtils.format(date, "yyyy-MM-dd--HH");
                return new Path(path + File.separator + format);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }
    }
}
