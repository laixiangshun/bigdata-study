package com.bigdata.study.flinkkafkahbase.watermarks;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * 自定义水印
 **/
public class FlinkHbaseWaterMarks implements AssignerWithPeriodicWatermarks<Map<String, String>> {
    private long currentTime;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentTime);
    }

    @Override
    public long extractTimestamp(Map<String, String> stringStringMap, long l) {
        currentTime = l;
        return l;
    }
}
