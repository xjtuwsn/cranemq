package com.github.xjtuwsn.cranemq.common.exception;

/**
 * @project:cranemq
 * @file:CraneBrokerException
 * @author:wsn
 * @create:2023/10/02-10:42
 */
public class CraneBrokerException extends RuntimeException {
    public CraneBrokerException(String message) {
        super(message);
    }

    public CraneBrokerException(Throwable cause) {
        super(cause);
    }
}
