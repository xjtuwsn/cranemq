package xjtu.wsn.cranemq.common.net.client;

import xjtu.wsn.cranemq.common.request.BaseRequest;

/**
 * @project:cranemq
 * @file:Client
 * @author:wsn
 * @create:2023/09/26-20:42
 */
public interface Client {
    void start();

    void close();
    void send(BaseRequest request);
}
