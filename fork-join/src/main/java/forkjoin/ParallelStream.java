package forkjoin;

import java.time.Duration;
import java.time.Instant;
import java.util.stream.LongStream;

/**
 * 并行流计算
 * 底层为fork join的实现
 * 效率高于直接使用顺序流 fork join
 **/
public class ParallelStream {
    public static void main(String[] args) {
        Instant startTime = Instant.now();
        long result = LongStream.rangeClosed(0, 10_000_00_100L)
                .parallel()
                .reduce(0, Long::sum);
        System.out.println(result);
        Instant endTime = Instant.now();
        System.out.println("计算10亿条数据耗时：" + Duration.between(startTime, endTime).toMillis());
    }
}
