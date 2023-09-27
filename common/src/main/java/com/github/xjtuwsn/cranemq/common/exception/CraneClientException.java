package com.github.xjtuwsn.cranemq.common.exception;

/**
 * @project:cranemq
 * @file:CraneClientException
 * @author:wsn
 * @create:2023/09/26-21:17
 */
public class CraneClientException extends RuntimeException {

    public CraneClientException(String msg) {
        super(msg);
    }
    public CraneClientException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
