package com.github.xjtuwsn.cranemq.broker.timer;

import com.github.xjtuwsn.cranemq.common.exception.CraneBrokerException;

import java.util.concurrent.TimeUnit;

/**
 * @project:cranemq
 * @file:DialPlate
 * @author:wsn
 * @create:2023/10/19-14:16
 */
public class DialPlate <T extends Thread> {

    private int number;

    private DelayTaskList[] bucket;

    public DialPlate(int number) {
        this.number = number;
        this.bucket = new DelayTaskList[number];
    }

    public DelayTaskList<T> submit(T task, int index, long totalDelay, long queueDelay) {
        if (index >= number) {
            throw new CraneBrokerException("Illegal index");
        }
        if (bucket[index] == null) {
            synchronized (this) {
                if (bucket[index] == null) {
                    bucket[index] = new DelayTaskList<>(queueDelay);
                }
            }
        }
        if (bucket[index].isEmpty()) {
            synchronized (this) {
                if (bucket[index].isEmpty()) {
                    bucket[index].addTask(task, totalDelay);
                    bucket[index].setExpiration(queueDelay);
                    return bucket[index];
                }
            }
        } else {
            bucket[index].addTask(task, totalDelay);
        }
        return null;
    }
}
