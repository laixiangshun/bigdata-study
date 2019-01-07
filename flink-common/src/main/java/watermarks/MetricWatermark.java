package watermarks;

import model.Metrics;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * @Description
 * @Author hasee
 * @Date 2019/1/4
 **/
public class MetricWatermark implements AssignerWithPeriodicWatermarks<Metrics> {
    private long currentTime = Long.MAX_VALUE;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentTime == Long.MAX_VALUE ? Long.MAX_VALUE : currentTime - 1);
    }

    @Override
    public long extractTimestamp(Metrics metrics, long l) {
        long time = metrics.getTimestamp() / (1000 * 1000);
        this.currentTime = time;
        return currentTime;
    }
}
