package com.github.xjtuwsn.cranemq.broker.store.cmtlog;

/**
 * @project:cranemq
 * @file:DelayMessageCommitListener
 * @author:wsn
 * @create:2023/10/20-10:03
 */
public interface DelayMessageCommitListener {

    void onCommit(long commitLogOffset, long queueOffset, String topic, int queueId, long delay);
}
