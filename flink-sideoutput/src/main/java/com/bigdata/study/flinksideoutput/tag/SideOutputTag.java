package com.bigdata.study.flinksideoutput.tag;

import org.apache.flink.util.OutputTag;

/**
 * @Description
 * @Author hasee
 * @Date 2019/1/11
 **/
public class SideOutputTag {
    public static final OutputTag<String> wordTag = new OutputTag<String>("rejected") {
    };
}
