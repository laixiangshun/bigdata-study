package com.bigdata.study.flinkhdfs.zip;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.hadoop.mapred.HadoopOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;

/**
 * 使用Flink内置sink API将数据以压缩的格式写入到HDFS上
 * 将数据以gz压缩格式将处理后的数据写入到HDFS上
 **/
public class FlinkHdfsZip {

    public static void main(String[] args) {
        String file_input = "C:\\Users\\hasee\\Desktop\\spark.txt";
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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
        DataStream<Tuple2<Text, IntWritable>> outStream = hdfsStream.map(new MapFunction<Tuple2<IntWritable, Text>, Tuple2<Text, IntWritable>>() {
            @Override
            public Tuple2<Text, IntWritable> map(Tuple2<IntWritable, Text> in) throws Exception {
                Tuple2<Text, IntWritable> tuple2 = new Tuple2<>();
                tuple2.f0 = in.f1;
                tuple2.f1 = in.f0;
                return tuple2;
            }
        });
//        DataStream<Tuple2<Text, IntWritable>> outStream = hdfsStream.flatMap(new FlatMapFunction<Tuple2<IntWritable, Text>, Tuple2<Text, IntWritable>>() {
//            @Override
//            public void flatMap(Tuple2<IntWritable, Text> in, Collector<Tuple2<Text, IntWritable>> collector) throws Exception {
//                Tuple2<Text, IntWritable> tuple2 = new Tuple2<>();
//                tuple2.f0 = in.f1;
//                tuple2.f1 = in.f0;
//                collector.collect(tuple2);
//            }
//        });
        //用gz格式压缩文件
        HadoopOutputFormat<Text, IntWritable> hadoopOutputFormat = new HadoopOutputFormat<>(new TextOutputFormat<>(), new JobConf());
        hadoopOutputFormat.getJobConf().set("mapred.textoutputformat.separator", " ");
        hadoopOutputFormat.getJobConf().setCompressMapOutput(true);
        hadoopOutputFormat.getJobConf().set("mapred.output.compress", "true");
        hadoopOutputFormat.getJobConf().setMapOutputCompressorClass(GzipCodec.class);
        //GzipCodec.class.getCanonicalName() 获取类名，等于getName()
        hadoopOutputFormat.getJobConf().set("mapred.output.compression.codec", GzipCodec.class.getCanonicalName());
        hadoopOutputFormat.getJobConf().set("mapred.output.compression.type", SequenceFile.CompressionType.BLOCK.toString());
        FileOutputFormat.setOutputPath(hadoopOutputFormat.getJobConf(), new Path("/tmp/data/"));
        outStream.writeUsingOutputFormat(hadoopOutputFormat);
        try {
            env.execute("Hadoop Compat WordCount");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
