package forkjoin;

import java.util.concurrent.RecursiveTask;

/**
 * fork join 处理任务
 **/
public class MyForkJoinTask extends RecursiveTask<Long> {
    /**
     * 任务拆分临界值
     */
    private static final long CRTTICAL_VALUE = 100_00;
    /**
     * 任务开启值
     */
    private Long startNum;

    /**
     * 任务结束值
     */
    private Long endNum;

    public MyForkJoinTask(Long startNum, Long endNum) {
        this.startNum = startNum;
        this.endNum = endNum;
    }

    @Override
    protected Long compute() {
        long length = endNum - startNum;
        if (length <= CRTTICAL_VALUE) {
            long num = 0;
            for (int i = 0; i < endNum; i++) {
                num += i;
            }
            return num;
        } else {
            long middleValue = (startNum + endNum) / 2;
            MyForkJoinTask leftTask = new MyForkJoinTask(startNum, middleValue);
            leftTask.fork();
            MyForkJoinTask rightTask = new MyForkJoinTask(middleValue + 1, endNum);
            rightTask.fork();
            return leftTask.join() + rightTask.join();
        }
    }
}
