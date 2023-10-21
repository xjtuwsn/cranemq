package com.github.xjtuwsn.cranemq.common.command.types;

/**
 * @project:cranemq
 * @file:CommandType
 * @author:wsn
 * @create:2023/09/27-10:20
 */
public enum RequestType implements Type {
    // 生产消息请求
    MESSAGE_PRODUCE_REQUEST,
    MESSAGE_BATCH_PRODUCE_REAUEST,
    DELAY_MESSAGE_PRODUCE_REQUEST,
    HEARTBEAT,
    // 向注册中心更新topic请求
    QUERY_TOPIC_REQUEST,
    // 拉取消息请求
    MESSAGE_PULL_REQUEST,
    CREATE_TOPIC_REQUEST,
    SIMPLE_PULL_MESSAGE_REQUEST,
    PULL_MESSAGE,
    QUERY_INFO,
    RECORD_OFFSET,
    LOCK_REQUEST,
    UPDATE_TOPIC_REQUEST,
    SEND_MESSAGE_BACK

}
