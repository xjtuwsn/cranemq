package com.github.xjtuwsn.cranemq.broker.timer;

import cn.hutool.core.lang.Pair;
import com.github.xjtuwsn.cranemq.common.exception.CraneBrokerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @project:cranemq
 * @file:TimingWheel
 * @author:wsn
 * @create:2023/10/19-11:12
 */
public class TimingWheel<T extends Thread> {

    private static final Logger log = LoggerFactory.getLogger(TimingWheel.class);

    private static final int DEFAULT_LEVEL = 3;
    private static final int DEFALUT_CELL = 30;

    private int level;

    private int cells;

    private DialPlate[] dialPlates;

    private long[] levelMillis;

    private long createTime;
    private long total;

    private DelayQueue<DelayTaskList<T>> delayQueue;

    private TakeTaskService takeTaskService;

    private ExecutorService asyncTaskService;

    public TimingWheel() {
        this(DEFAULT_LEVEL, DEFALUT_CELL);
    }

    public TimingWheel(int level, int cells) {
        this.level = level;
        this.cells = cells;
        this.dialPlates = new DialPlate[level];
        this.levelMillis = new long[level];
        for (int i = 0; i < level; i++) {
            this.dialPlates[i] = new DialPlate<T>(this.cells);
            this.levelMillis[i] = TimeUnit.SECONDS.toMillis((int) Math.pow(this.cells, i + 1));
        }
        this.total = this.levelMillis[this.level - 1];
        this.createTime = System.currentTimeMillis();
        this.delayQueue = new DelayQueue<>();
        this.asyncTaskService = new ThreadPoolExecutor(8, 16, 60L,
                TimeUnit.SECONDS,
                new LinkedBlockingDeque<>(3000),
                new ThreadFactory() {
                    AtomicInteger index = new AtomicInteger(0);
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "AsyncTaskService NO." + index.getAndIncrement());
                    }
                });
        this.takeTaskService = new TakeTaskService();
        this.takeTaskService.start();
    }

    public void submit(T task, long delay, TimeUnit unit) {
        Pair<Pair<Integer, Integer>, Long> compute = compute(delay, unit);
        // System.out.println(compute);
        Pair<Integer, Integer> indexs = compute.getKey();
        if (indexs == null) {
            throw new CraneBrokerException("Compute index error");
        }
        int l = indexs.getKey(), index = indexs.getValue();
        long delaySecond = compute.getValue();
        DelayTaskList<T> submitRes = this.dialPlates[l]
                .submit(task,
                        index,
                        TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() + unit.toMillis(delay)),
                        delaySecond);
        if (submitRes != null) {
            this.delayQueue.put(submitRes);
        }
    }

    public Pair<Pair<Integer, Integer>, Long> compute(long delay, TimeUnit unit) {
        long delayMills = unit.toMillis(delay);
        long delaySecond = unit.toSeconds(delay);
        if (delayMills > this.total) {
            throw new CraneBrokerException("Delay is too long");
        }
        long diff = System.currentTimeMillis() - createTime;
        long seconds = TimeUnit.MILLISECONDS.toSeconds(diff);

        long base = this.cells;
        long prev = 1L, precell = 0;
        for (int i = 0; i < this.cells; i++) {

            int point = (int) ((seconds % base) / prev); // 28
            if (delaySecond <= base) { // 4
                int choosed = (int) ((point + delaySecond / prev + precell / prev) % base);
                long gap = (choosed * prev - (seconds % base) + base) % base;
                // 选择表盘id，表格id，在delay队列需要等多久
                return new Pair<>(new Pair<>(i, choosed), gap);
            }
            delaySecond -= (base - point - precell);
            prev = base;
            precell = base;
            base *= this.cells;
        }
        return null;
    }

    class TakeTaskService extends Thread {

        private boolean stop = false;

        @Override
        public void run() {

            while (!stop) {
                try {
                    DelayTaskList<T> taskList = delayQueue.take();
                    Iterator<DelayTaskWrapper<T>> iterator = taskList.iterator();
                    while (iterator.hasNext()) {
                        DelayTaskWrapper<T> next = iterator.next();
                        iterator.remove();
                        if (next.isExpired()) {
                            log.info("Task executed in current seconds {}", TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()));
                             asyncTaskService.execute(next.getTask());
                        } else {

                            long now = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
                            log.info("Task should be upgrade to next dial, in current seconds {}, expira is {}", now, next.getExpiratonTime());

                            submit(next.getTask(), next.getExpiratonTime() - now, TimeUnit.SECONDS);
                        }
                    }

                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

    }
}
