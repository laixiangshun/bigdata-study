package com.bigdata.study.flinkelasticsearchsink;

import com.bigdata.study.flinkelasticsearchsink.handler.FlinkFailHandler;
import constant.PropertiesConstants;
import model.Metrics;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import utils.ExecutionEnvUtil;
import utils.GsonUtils;
import utils.KafkaUtils;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * flink从kafka中读取数据，经过flink处理后写入es6中
 */
@SpringBootApplication
public class FlinkElasticsearchSinkApplication {

    public static void main(String[] args) {
//        SpringApplication.run(FlinkElasticsearchSinkApplication.class, args);
        try {
            ParameterTool parameterPool = ExecutionEnvUtil.createParameterPool(args);
            StreamExecutionEnvironment environment = ExecutionEnvUtil.prepare(parameterPool);
            DataStreamSource<Metrics> dataStreamSource = KafkaUtils.buildSource(environment);
            List<HttpHost> httpHosts = parseEsHost(parameterPool.get(PropertiesConstants.ELASTICSEARCH_HOSTS));
            int bulkSize = parameterPool.getInt(PropertiesConstants.ELASTICSEARCH_BULK_FLUSH_MAX_ACTIONS, 40);
            int parallelism = parameterPool.getInt(PropertiesConstants.STREAM_SINK_PARALLELISM, 4);
            ElasticsearchSink.Builder<Metrics> builder = new ElasticsearchSink.Builder<>(httpHosts, new ElasticsearchSinkFunction<Metrics>() {
                @Override
                public void process(Metrics metrics, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                    requestIndexer.add(Requests.indexRequest()
                            .index("flink_" + metrics.getName())
                            .type("document")
                            .source(GsonUtils.toJsonBytes(metrics), XContentType.JSON));
                }
            });
            //复杂配置

            //是否开启重试机制
            builder.setBulkFlushBackoff(true);
            //重试策略
            //CONSTANT 常数型，表示多次重试之间的时间间隔为固定常数。eg:2 -> 2 -> 2 ...
            builder.setBulkFlushBackoffType(ElasticsearchSinkBase.FlushBackoffType.CONSTANT);
            //指数型，表示多次重试之间的时间间隔按照指数方式进行增长。eg:2 -> 4 -> 8 ...
//            builder.setBulkFlushBackoffType(ElasticsearchSinkBase.FlushBackoffType.EXPONENTIAL);
            //进行重试的时间间隔。对于指数型则表示起始的基数
            builder.setBulkFlushBackoffDelay(2);
            //失败重试次数
            builder.setBulkFlushBackoffRetries(3);

            //批量写入时的最大写入条数
            builder.setBulkFlushMaxActions(bulkSize);
            //批量写入时的最大数据量
            builder.setBulkFlushMaxSizeMb(10);

            //想要使用EsSink的失败重试机制，则需要通过env.enableCheckpoint()方法来开启Flink任务对checkpoint的支持，
            // 如果没有开启checkpoint机制的话，则失败重试策略是无法生效的
            boolean checkpoint = parameterPool.getBoolean(PropertiesConstants.STREAM_CHECKPOINT_ENABLE);
            if (checkpoint) {
                //设置失败策略
                builder.setFailureHandler(new FlinkFailHandler());
            }

            dataStreamSource.addSink(builder.build()).setParallelism(parallelism);
            environment.execute("flink connectors es6");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static List<HttpHost> parseEsHost(String hosts) throws MalformedURLException {
        String[] hostArray = hosts.split(",");
        List<HttpHost> httpHosts = new ArrayList<>();
        for (String host : hostArray) {
            if (StringUtils.startsWith(host, "http:")) {
                URL url = new URL(host);
                httpHosts.add(new HttpHost(url.getHost(), url.getPort()));
            } else {
                String[] parts = host.split(":", 2);
                if (parts.length > 1) {
                    httpHosts.add(new HttpHost(parts[0], Integer.parseInt(parts[1])));
                } else {
                    throw new MalformedURLException("invalid elasticsearch hosts exception!");
                }
            }
        }
        return httpHosts;
    }
}

