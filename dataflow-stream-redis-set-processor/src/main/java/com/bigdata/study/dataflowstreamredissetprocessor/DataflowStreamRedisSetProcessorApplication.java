package com.bigdata.study.dataflowstreamredissetprocessor;

import com.bigdata.study.dataflowstreamredissetprocessor.config.RedisStreamProcessorConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@ComponentScan("com.bigdata")
@Import({RedisStreamProcessorConfig.class})
public class DataflowStreamRedisSetProcessorApplication {

    public static void main(String[] args) {
        SpringApplication.run(DataflowStreamRedisSetProcessorApplication.class, args);
    }

}

