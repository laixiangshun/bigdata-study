package com.bigdata.study.dataflowstreamredispubsink;

import com.bigdata.study.dataflowstreamredispubsink.config.RedisStreamPubConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@ComponentScan("com.bigdata")
@Import({RedisStreamPubConfig.class})
public class DataflowStreamRedisPubSinkApplication {

    public static void main(String[] args) {
        SpringApplication.run(DataflowStreamRedisPubSinkApplication.class, args);
    }

}

