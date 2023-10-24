package com.github.xjtuwsn.cranemq.broker.timer;

import com.github.xjtuwsn.cranemq.common.exception.CraneBrokerException;

import java.util.concurrent.TimeUnit;

/**
 * @project:cranemq
 * @file:DialPlate
 * @author:wsn
 * @create:2023/10/19-14:16
 */

/**
 * 时间轮中的表盘类
 * @param <T>
 */
public class DialPlate <T extends Thread> {

    // 表盘中格子的个数
    private final int number;
    // 每个格子就存放任务的列表
    private final DelayTaskList<T>[] bucket;

    public DialPlate(int number) {
        this.number = number;
        this.bucket = new DelayTaskList[number];
    }

    /** 向表盘中提交任务
     * @param task 任务
     * @param index 表格索引
     * @param totalDelay 总的过期时间
     * @param queueDelay 对应任务列表，应该延时多久
     * @return 是否是新建的或者空的list
     */
    public DelayTaskList<T> submit(T task, int index, long totalDelay, long queueDelay) {
        if (index >= number) {
            throw new CraneBrokerException("Illegal index");
        }
        // 如果这个表格原来没有任务列表，就新建
        if (bucket[index] == null) {
            synchronized (this) {
                if (bucket[index] == null) {
                    bucket[index] = new DelayTaskList<>(queueDelay);
                }
            }
        }
        // 如果是空的，就插入任务，并且充值延时时间
        if (bucket[index].isEmpty()) {
            synchronized (this) {
                if (bucket[index].isEmpty()) {
                    bucket[index].addTask(task, totalDelay);
                    bucket[index].setExpiration(queueDelay);
                    return bucket[index];
                }
            }
        } else {
            // 插入任务
            bucket[index].addTask(task, totalDelay);
        }
        return null;
    }
}
