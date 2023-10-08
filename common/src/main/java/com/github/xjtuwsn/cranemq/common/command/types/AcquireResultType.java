package com.github.xjtuwsn.cranemq.common.command.types;

import java.io.Serializable;

/**
 * @project:cranemq
 * @file:PullResultType
 * @author:wsn
 * @create:2023/10/07-18:39
 */
public enum AcquireResultType implements Serializable {
    DONE,
    NO_MESSAGE,
    OFFSET_INVALID,
    ERROR

}
