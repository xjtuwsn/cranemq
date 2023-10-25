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

/**
 * 时间轮，用于实现延时任务
 * @author wsn
 * @param <T>
 */
public class TimingWheel<T extends Thread> {

    private static final Logger log = LoggerFactory.getLogger(TimingWheel.class);

    // 默认时间轮轮数
    private static final int DEFAULT_LEVEL = 3;
    // 默认每个轮的格子数量
    private static final int DEFAULT_CELL = 30;
    // 层级数
    private final int level;
    // 格子数
    private final int cells;
    // 表盘类实体
    private final DialPlate<T>[] dialPlates;

    private final long[] levelMillis;
    // 时间轮开始运行时的时间
    private final long createTime;
    // 能表示的总时间
    private final long total;
    // 用于推进时间轮的延时队列
    private final DelayQueue<DelayTaskList<T>> delayQueue;
    // 从延时队列中拿去任务的线程
    private final TakeTaskService takeTaskService;
    // 执行延时任务的线程池
    private final ExecutorService asyncTaskService;

    public TimingWheel() {
        this(DEFAULT_LEVEL, DEFAULT_CELL);
    }

    public TimingWheel(int level, int cells) {
        this.level = level;
        this.cells = cells;
        this.dialPlates = new DialPlate[level];
        this.levelMillis = new long[level];
        // 初始化表盘，计算能表示的时间
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

    /**
     * 向时间轮提交一个任务
     * @param task 任务
     * @param delay 延时时间
     * @param unit 延时单位
     */
    public void submit(T task, long delay, TimeUnit unit) {
        // 首先计算出当前这个任务放到哪个表盘的哪个格子，和指针的延时时间
        Pair<Pair<Integer, Integer>, Long> compute = compute(delay, unit);
        Pair<Integer, Integer> indexs = compute.getKey();
        if (indexs == null) {
            throw new CraneBrokerException("Compute index error");
        }
        int l = indexs.getKey(), index = indexs.getValue();
        long delaySecond = compute.getValue();
        // 然后向表盘格子中插入任务，如果这个格子之前没有任务，则需要将这个格子加入到延时队列用于推进时间
        DelayTaskList<T> submitRes = this.dialPlates[l]
                .submit(task,
                        index,
                        TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() + unit.toMillis(delay)),
                        delaySecond);
        if (submitRes != null) {
            this.delayQueue.put(submitRes);
        }
    }

    /**
     * 计算当前延时时间的任务应该放在哪里，并且指针要推进多久才能到这个格子
     * @param delay 延时时间
     * @param unit 延时单位
     * @return 表盘号，格子id，指针延时多久
     */
    public Pair<Pair<Integer, Integer>, Long> compute(long delay, TimeUnit unit) {
        // 延时时间对应的毫秒和秒
        long delayMills = unit.toMillis(delay);
        long delaySecond = unit.toSeconds(delay);
        if (delayMills > this.total) {
            throw new CraneBrokerException("Delay is too long");
        }
        // 计算时间轮到目前为止已经运行了多久，用于确定指针位置
        long diff = System.currentTimeMillis() - createTime;
        long seconds = TimeUnit.MILLISECONDS.toSeconds(diff);

        // 当前格子数
        long base = this.cells;
        // 上一层的系数与格子数
        long prev = 1L, precell = 0;
        // 从最底层开始遍历格子
        for (int i = 0; i < this.cells; i++) {

            // 计算当前表盘上的指针位置
            int point = (int) ((seconds % base) / prev); // 28
            // 如果剩余时间比当前表盘能表示的时间少，那就证明任务该落到这个表盘
            if (delaySecond <= base) { // 4
                // 根据当前指针和延时时间记录放到哪个表格中
                int chosen = (int) ((point + delaySecond / prev + precell / prev) % base);
                // 然后计算从当前表盘指针转到chosen要多久
                // 也就表明了多久之后这个任务需要执行(在第一个表盘)或者需要降级到下一个表盘
                long gap = (chosen * prev - (seconds % base) + base) % base;
                // 选择表盘id，表格id，在delay队列需要等多久
                return new Pair<>(new Pair<>(i, chosen), gap);
            }
            // 当前表盘放不了，更新剩余时间，到下一个表盘计算
            delaySecond -= (base - point - precell);
            // 更新之前的数据
            prev = base;
            precell = base;
            // 能表示时间范围扩大
            base *= this.cells;
        }
        return null;
    }

    /**
     * 从延时队列中拿任务
     */
    class TakeTaskService extends Thread {

        private boolean stop = false;

        @Override
        public void run() {

            while (!stop) {
                try {
                    // 那曲到当前格子中的所有任务，并进行迭代
                    DelayTaskList<T> taskList = delayQueue.take();
                    Iterator<DelayTaskWrapper<T>> iterator = taskList.iterator();
                    while (iterator.hasNext()) {
                        // 真正的任务包装
                        DelayTaskWrapper<T> next = iterator.next();
                        // 从当前队列删除
                        iterator.remove();
                        // 如果这个任务正好过期，那就证明是最底层的表盘任务，立即执行
                        if (next.isExpired()) {
                            log.info("Task executed in current seconds {}", TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()));
                             asyncTaskService.execute(next.getTask());
                        } else {

                            // 任务没过期就证明当前表盘走完了，该调整时间放到下一层表盘
                            long now = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
                            log.info("Task should be upgrade to next dial, in current seconds {}, expira is {}", now, next.getExpirationTime());

                            submit(next.getTask(), next.getExpirationTime() - now, TimeUnit.SECONDS);
                        }
                    }

                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        public void setStop() {
            this.stop = true;
        }

    }
}
