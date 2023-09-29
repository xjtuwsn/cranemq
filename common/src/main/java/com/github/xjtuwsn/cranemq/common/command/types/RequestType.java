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
    // 向注册中心更新topic请求
    UPDATE_TOPIC_REQUEST,
    // 拉取消息请求
    MESSAGE_PULL_REQUEST,

}
