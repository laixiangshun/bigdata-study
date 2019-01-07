package com.bigdata.study.sparkphoenix.phoenix;

import com.bigdata.study.sparkphoenix.SparkPhoenixApplication;
import org.junit.Test;
import org.springframework.boot.SpringApplication;

import java.util.ArrayList;
import java.util.List;

/**
 * @Description
 * @Author hasee
 * @Date 2019/1/3
 **/
public class SparkPhoenixTest {
    public static void main(String[] args) {
        List<String> params = new ArrayList<>();
        params.add("2017-06-01");
        params.add("2017-07-01");
        String[] argArray = params.toArray(new String[]{});
        SpringApplication.run(SparkPhoenixApplication.class, argArray);
    }
}
