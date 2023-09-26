package xjtu.wsn.cranemq.test;

import xjtu.wsn.cranemq.broker.core.MqBroker;
import xjtu.wsn.cranemq.common.net.client.Client;
import xjtu.wsn.cranemq.common.net.client.ConnectionClient;
import xjtu.wsn.cranemq.common.net.client.RemoteAddress;
import xjtu.wsn.cranemq.common.request.BaseRequest;
import xjtu.wsn.cranemq.common.request.MessageProduceRequest;
import xjtu.wsn.cranemq.common.request.RequestType;

/**
 * @project:cranemq
 * @file:TestMain
 * @author:wsn
 * @create:2023/09/26-21:35
 */
public class TestMain {

    public static void main(String[] args) {
        MqBroker broker = new MqBroker();
        broker.start();

        Thread t = new Thread(() -> {
            BaseRequest request = new BaseRequest();
            request.setRequestID("123");
            request.setDataType(RequestType.MESSAGE_PRODUCE);
            request.setData(new MessageProduceRequest());

            RemoteAddress remoteAddress = new RemoteAddress("127.0.0.1", 9999);
            Client client = new ConnectionClient(remoteAddress);
            client.start();
            client.send(request);
        });
        t.start();
    }
}
