package com.bigdata.study.flinkasyncio.async;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.util.ExecutorUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * 使用线程模仿async IO 操作
 **/
public class AsyncDataBaseRequest extends RichAsyncFunction<Integer, String> {

    private static final long serialVersionUID = -1L;

    private transient ExecutorService executorService;

    private final long sleepFactor;

    private final float failRatio;

    private final long shutdownWaitTS;

    public AsyncDataBaseRequest(long sleepFactor, float failRatio, long shutdownWaitTS) {
        this.sleepFactor = sleepFactor;
        this.failRatio = failRatio;
        this.shutdownWaitTS = shutdownWaitTS;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        executorService = Executors.newFixedThreadPool(10);
    }

    @Override
    public void close() throws Exception {
        super.close();
        ExecutorUtils.gracefulShutdown(shutdownWaitTS, TimeUnit.MICROSECONDS, executorService);
    }

    @Override
    public void asyncInvoke(Integer integer, ResultFuture<String> resultFuture) throws Exception {
        executorService.submit(() -> {
            long sleep = (long) (ThreadLocalRandom.current().nextFloat() * sleepFactor);
            try {
                Thread.sleep(sleep);
                if (ThreadLocalRandom.current().nextFloat() < failRatio) {
                    resultFuture.completeExceptionally(new Exception("数据太小了。。。"));
                } else {
                    resultFuture.complete(Collections.singletonList("key-" + (integer % 10)));
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
                resultFuture.complete(new ArrayList<>(0));
            }
        });
    }
}
