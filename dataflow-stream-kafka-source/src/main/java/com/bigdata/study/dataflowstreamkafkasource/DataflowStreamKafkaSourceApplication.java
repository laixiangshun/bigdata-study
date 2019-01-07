package com.bigdata.study.dataflowstreamkafkasource;

import com.bigdata.study.dataflowstreamkafkasource.config.KafkaSourceConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@ComponentScan("com.bigdata")
@Import({KafkaSourceConfig.class})
public class DataflowStreamKafkaSourceApplication {

    public static void main(String[] args) {
        SpringApplication.run(DataflowStreamKafkaSourceApplication.class, args);
    }

}

