package com.bigdata.study.flinkasyncio;

import com.bigdata.study.flinkasyncio.async.AsyncDataBaseRequest;
import com.bigdata.study.flinkasyncio.source.SimpleSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * 使用async IO
 */
//@SpringBootApplication
public class FlinkAsyncIoApplication {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkAsyncIoApplication.class);

    public static void main(String[] args) {
//        SpringApplication.run(FlinkAsyncIoApplication.class, args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool params = ParameterTool.fromArgs(args);
        String statePath = null;
        String cpMode = null;
        int maxCount = 0;
        long sleepFactor = 0;
        float failRatio = 0;
        String mode = null;
        int taskNum = 0;
        String timeType = null;
        long shutdownWaitTS = 0;
        long timeout = 0;

        try {
            // check the configuration for the job
            statePath = params.get("fsStatePath", null);
            cpMode = params.get("checkpointMode", "exactly_once");
            maxCount = params.getInt("maxCount", 100000);
            sleepFactor = params.getLong("sleepFactor", 100);
            failRatio = params.getFloat("failRatio", 0.001f);
            mode = params.get("waitMode", "ordered");
            taskNum = params.getInt("waitOperatorParallelism", 1);
            timeType = params.get("eventType", "EventTime");
            shutdownWaitTS = params.getLong("shutdownWaitTS", 20000);
            timeout = params.getLong("timeout", 10000L);
        } catch (Exception e) {
            e.printStackTrace();
        }

        StringBuilder configStringBuilder = new StringBuilder();

        final String lineSeparator = System.getProperty("line.separator");

        configStringBuilder
                .append("Job configuration").append(lineSeparator)
                .append("FS state path=").append(statePath).append(lineSeparator)
                .append("Checkpoint mode=").append(cpMode).append(lineSeparator)
                .append("Max count of input from source=").append(maxCount).append(lineSeparator)
                .append("Sleep factor=").append(sleepFactor).append(lineSeparator)
                .append("Fail ratio=").append(failRatio).append(lineSeparator)
                .append("Waiting mode=").append(mode).append(lineSeparator)
                .append("Parallelism for async wait operator=").append(taskNum).append(lineSeparator)
                .append("Event type=").append(timeType).append(lineSeparator)
                .append("Shutdown wait timestamp=").append(shutdownWaitTS);

        LOG.info(configStringBuilder.toString());

        if (StringUtils.isNotBlank(statePath)) {
            env.setStateBackend(new FsStateBackend(statePath));
        }
        if ("exactly_once".equals(cpMode)) {
            env.enableCheckpointing(1000L, CheckpointingMode.EXACTLY_ONCE);
        } else {
            env.enableCheckpointing(1000L, CheckpointingMode.AT_LEAST_ONCE);
        }

        if ("event_time".equals(timeType)) {
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        } else if ("ingestion_time".equals(timeType)) {
            env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        } else {
            env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        }

        DataStream<Integer> inputStream = env.addSource(new SimpleSource(maxCount));
        AsyncFunction<Integer, String> asyncFunction = new AsyncDataBaseRequest(sleepFactor, failRatio, shutdownWaitTS);

        DataStream<String> result;
        if ("ordered".equals(mode)) {
            result = AsyncDataStream.orderedWait(inputStream, asyncFunction, timeout, TimeUnit.MILLISECONDS, 20).setParallelism(taskNum);
        } else {
            result = AsyncDataStream.unorderedWait(inputStream, asyncFunction, timeout, TimeUnit.MILLISECONDS, 20).setParallelism(taskNum);
        }

        DataStream<Tuple2<String, Integer>> outputStream = result.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                collector.collect(new Tuple2<>(s, 1));
            }
        });
        DataStream<Tuple2<String, Integer>> sum = outputStream.keyBy(0).sum(1);
        sum.print();
        try {
            env.execute("flink async io example");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

