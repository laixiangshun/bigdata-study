package com.bigdata.study.flinksideoutput.process;

import com.bigdata.study.flinksideoutput.tag.SideOutputTag;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Description
 * @Author hasee
 * @Date 2019/1/11
 **/
public class ProcessTokenizer extends ProcessFunction<String, Tuple2<String, Integer>> {
    @Override
    public void processElement(String value, Context context, Collector<Tuple2<String, Integer>> collector) throws Exception {
        String[] tokens = value.toLowerCase().split("\\w+");
        for (String token : tokens) {
            if (token.length() > 5) {
                context.output(SideOutputTag.wordTag, token);
            } else {
                collector.collect(new Tuple2<>(token, 1));
            }
        }
    }
}
