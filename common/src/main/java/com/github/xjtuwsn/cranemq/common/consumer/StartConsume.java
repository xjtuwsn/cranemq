package com.github.xjtuwsn.cranemq.common.consumer;

import java.io.Serializable;

/**
 * @project:cranemq
 * @file:StartConsume
 * @author:wsn
 * @create:2023/10/08-10:43
 */
public enum StartConsume implements Serializable {
    FROM_FIRST_OFFSET,
    FROM_LAST_OFFSET
}
