package com.bigdata.study.dataflowstreamredissetprocessor.prop;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @Description
 * @Author hasee
 * @Date 2019/1/2
 **/
@ConfigurationProperties("redis")
public class RedisSetProperties {

    private String setName;

    public String getSetName() {
        return setName;
    }

    public void setSetName(String setName) {
        this.setName = setName;
    }
}
