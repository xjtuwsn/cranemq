package com.github.xjtuwsn.cranemq.broker.timer;

import com.github.xjtuwsn.cranemq.broker.BrokerController;

/**
 * @project:cranemq
 * @file:DelayTask
 * @author:wsn
 * @create:2023/10/19-11:21
 */

/**
 * 延时任务，继承Thread
 */
public abstract class DelayTask extends Thread {
    public static final int DELAY_MESSAGE = 0;

    protected BrokerController brokerController;

    protected DelayTask(BrokerController brokerController) {
        this.brokerController = brokerController;
    }
    @Override
    public abstract void run();
    protected abstract int getTaskType();
}
