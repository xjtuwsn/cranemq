package com.github.xjtuwsn.cranemq.common.command.types;

import java.io.Serializable;

/**
 * @project:cranemq
 * @file:RpcType
 * @author:wsn
 * @create:2023/09/27-10:24
 */
public enum RpcType implements Serializable {

    ONE_WAY,
    ASYNC,
    SYNC
}
