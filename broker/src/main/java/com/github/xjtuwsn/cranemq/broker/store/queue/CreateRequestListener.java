package com.github.xjtuwsn.cranemq.broker.store.queue;

import com.github.xjtuwsn.cranemq.broker.store.MappedFile;
import com.github.xjtuwsn.cranemq.broker.store.comm.AsyncRequest;

/**
 * @project:cranemq
 * @file:CreateRequestListener
 * @author:wsn
 * @create:2023/10/05-15:11
 */
public interface CreateRequestListener {

    MappedFile onRequireCreate(String topic, int queueId, int index);
}
