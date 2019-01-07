package com.bigdata.study.flinkelasticsearchsink.handler;

import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;

/**
 * 自定义es写入失败处理
 **/
public class FlinkFailHandler implements ActionRequestFailureHandler {

    @Override
    public void onFailure(ActionRequest actionRequest, Throwable throwable, int requestStatusCode, RequestIndexer requestIndexer) throws Throwable {
        if (throwable instanceof EsRejectedExecutionException) {
            //将失败请求继续加入队列，后续进行重试写入
            requestIndexer.add(actionRequest);
        } else if (throwable instanceof ElasticsearchParseException) {
            //自定义异常处理
            throwable.printStackTrace();
        } else {
            throw throwable;
        }
    }
}
