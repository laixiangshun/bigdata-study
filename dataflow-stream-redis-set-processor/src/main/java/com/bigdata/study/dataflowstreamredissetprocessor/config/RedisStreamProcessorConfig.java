package com.bigdata.study.dataflowstreamredissetprocessor.config;

import com.bigdata.study.dataflowstreamredissetprocessor.prop.RedisSetProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.BoundSetOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.integration.annotation.Filter;
import org.springframework.messaging.Message;

import java.util.Map;

/**
 * redis 流水任务 processor
 **/
@Configuration
@EnableConfigurationProperties(RedisSetProperties.class)
@EnableBinding(Processor.class)
public class RedisStreamProcessorConfig {

    private static final SpelExpressionParser expressionParser = new SpelExpressionParser();
    private static final String expressionString = "payload[\"index\"]";

    @Autowired
    private RedisSetProperties redisSetProperties;

    @Autowired
    private RedisTemplate<String, String> redisTemplate;

    @Filter(inputChannel = Processor.INPUT, outputChannel = Processor.OUTPUT)
    public boolean filter(Message<?> message) {
        final Expression expression = expressionParser.parseExpression(expressionString);
        Map map = (Map) message.getPayload();
        BoundSetOperations<String, String> boundSetOperations = redisTemplate.boundSetOps(redisSetProperties.getSetName());
        boolean member = boundSetOperations.isMember(expression.getValue(map, String.class));
        return member;
    }
}
