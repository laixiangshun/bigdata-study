package com.bigdata.study.flinkasyncio.source;

import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.springframework.util.CollectionUtils;

import java.util.Collections;
import java.util.List;

/**
 * @Description
 * @Author hasee
 * @Date 2019/1/15
 **/
public class SimpleSource implements SourceFunction<Integer>, ListCheckpointed<Integer> {

    private volatile boolean isRunning = true;

    private int counter = 0;

    private int start = 0;

    public SimpleSource(int counter) {
        this.counter = counter;
    }

    @Override
    public List<Integer> snapshotState(long l, long l1) throws Exception {
        return Collections.singletonList(start);
    }

    @Override
    public void restoreState(List<Integer> list) throws Exception {
        for (Integer state : list) {
            this.start = state;
        }
    }

    @Override
    public void run(SourceContext<Integer> sourceContext) throws Exception {
        while ((start < counter || counter == -1) && isRunning) {
            synchronized (sourceContext.getCheckpointLock()) {
                sourceContext.collect(start);
                ++start;
                if (start == Integer.MAX_VALUE) {
                    start = 0;
                }
            }
            Thread.sleep(10L);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
