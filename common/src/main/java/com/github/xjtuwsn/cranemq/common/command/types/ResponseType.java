package com.github.xjtuwsn.cranemq.common.command.types;

/**
 * @project:cranemq
 * @file:ResponseType
 * @author:wsn
 * @create:2023/09/27-10:32
 */
public enum ResponseType implements Type {
    PRODUCE_MESSAGE_RESPONSE,
    QUERY_TOPIC_RESPONSE,
    CREATE_TOPIC_RESPONSE,

    QUERY_BROKER_RESPONSE,
    RESPONSE_FAILED,
    SIMPLE_PULL_RESPONSE,
    NOTIFY_CHAGED_RESPONSE,
    PULL_RESPONSE,
    LOCK_RESPONSE
}
