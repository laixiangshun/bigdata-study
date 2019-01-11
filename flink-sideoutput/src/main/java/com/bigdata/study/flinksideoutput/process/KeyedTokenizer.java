package com.bigdata.study.flinksideoutput.process;

import com.bigdata.study.flinksideoutput.tag.SideOutputTag;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 以用户自定义FlatMapFunction函数的形式来实现分词器功能，该分词器会将分词封装为(word,1)，
 * 同时不接受单词长度大于5的，也即是侧输出都是单词长度大于5的单词。
 **/
public class KeyedTokenizer extends KeyedProcessFunction<Integer, String, Tuple2<String, Integer>> {
    @Override
    public void processElement(String value, Context context, Collector<Tuple2<String, Integer>> collector) throws Exception {
        String[] tokens = value.toLowerCase().split("\\W+");
        for (String token : tokens) {
            if (token.length() > 5) {
                context.output(SideOutputTag.wordTag, token);
            } else {
                collector.collect(new Tuple2<>(token, 1));
            }
        }
    }

}
