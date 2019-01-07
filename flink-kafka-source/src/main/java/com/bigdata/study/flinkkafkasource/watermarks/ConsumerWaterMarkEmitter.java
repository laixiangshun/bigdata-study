package com.bigdata.study.flinkkafkasource.watermarks;

import model.Metrics;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * 自定义时间戳提取器/水印发射器
 **/
public class ConsumerWaterMarkEmitter implements AssignerWithPeriodicWatermarks<Metrics> {
    private long currentTime;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentTime);
    }

    @Override
    public long extractTimestamp(Metrics metrics, long l) {
        currentTime = l;
        return currentTime;
    }
}
