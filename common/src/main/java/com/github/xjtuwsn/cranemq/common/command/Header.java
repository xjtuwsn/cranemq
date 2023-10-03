package com.github.xjtuwsn.cranemq.common.command;

import com.github.xjtuwsn.cranemq.common.command.types.ResponseCode;
import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;
import lombok.*;
import com.github.xjtuwsn.cranemq.common.command.types.RpcType;
import com.github.xjtuwsn.cranemq.common.command.types.Type;

import java.io.Serializable;

/**
 * @project:cranemq
 * @file:Header
 * @author:wsn
 * @create:2023/09/27-10:18
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class Header implements Serializable {
    // 远程请求类型，各种请求、响应与状态
    private Type commandType;
    // rpc类型，单向、同步或异步
    private RpcType rpcType;
    // 消息唯一标识id
    private String correlationId;
    // 响应状态码
    private int status = ResponseCode.SUCCESS;

    private int version = 1;

    public Header(Type commandType, RpcType rpcType, String correlationId) {
        this.commandType = commandType;
        this.rpcType = rpcType;
        this.correlationId = correlationId;
    }
    public void onFailure(int code) {
        if (code == ResponseCode.SUCCESS) {
            code = ResponseCode.DEFAULT_ERROR;
        }
        this.status = code;
    }
}
