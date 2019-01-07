package forkjoin;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ForkJoinPool;

/**
 * 顺序流使用fork join
 **/
public class ForkJoinApp {
    public static void main(String[] args) {
        Instant startTime = Instant.now();
        ForkJoinPool forkJoinPool = new ForkJoinPool();
        MyForkJoinTask myForkJoinTask = new MyForkJoinTask(0L, 10_000_00_000L);
        Long result = forkJoinPool.invoke(myForkJoinTask);
        System.out.println(result);
        Instant endTime = Instant.now();
        System.out.println("计算10亿条数据耗时：" + Duration.between(startTime, endTime).toMillis());
    }
}
