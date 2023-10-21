package com.github.xjtuwsn.cranemq.client.consumer.push;

import com.github.xjtuwsn.cranemq.client.WrapperFutureCommand;
import com.github.xjtuwsn.cranemq.client.consumer.impl.DefaultPushConsumerImpl;
import com.github.xjtuwsn.cranemq.common.command.FutureCommand;
import com.github.xjtuwsn.cranemq.common.command.Header;
import com.github.xjtuwsn.cranemq.common.command.PayLoad;
import com.github.xjtuwsn.cranemq.common.command.RemoteCommand;
import com.github.xjtuwsn.cranemq.common.command.payloads.req.MQSendBackRequest;
import com.github.xjtuwsn.cranemq.common.command.types.RequestType;
import com.github.xjtuwsn.cranemq.common.command.types.RpcType;
import com.github.xjtuwsn.cranemq.common.entity.ReadyMessage;
import com.github.xjtuwsn.cranemq.common.utils.TopicUtil;

import java.util.List;

/**
 * @project:cranemq
 * @file:AbstractReputMessageService
 * @author:wsn
 * @create:2023/10/21-11:24
 */
public abstract class AbstractReputMessageService implements ConsumeMessageService {

    // 失败消息返回
    protected DefaultPushConsumerImpl defaultPushConsumer;

    protected AbstractReputMessageService(DefaultPushConsumerImpl defaultPushConsumer) {
        this.defaultPushConsumer = defaultPushConsumer;
    }

    protected void sendMessageBackToBroker(List<ReadyMessage> readyMessages, boolean isOrdered) {
        Header header = new Header(RequestType.SEND_MESSAGE_BACK, RpcType.ASYNC, TopicUtil.generateUniqueID());
        String topic = "";
        for (ReadyMessage readyMessage : readyMessages) {
            topic = readyMessage.getTopic();
            if (isOrdered) {
                readyMessage.setRetry(3);
            } else {
                readyMessage.setRetry(readyMessage.getRetry() + 1);
            }
        }
        PayLoad payLoad = new MQSendBackRequest(readyMessages, defaultPushConsumer.getDefaultPushConsumer().getConsumerGroup());
        RemoteCommand remoteCommand = new RemoteCommand(header, payLoad);
        FutureCommand futureCommand = new FutureCommand(remoteCommand);
        WrapperFutureCommand wrappered = new WrapperFutureCommand(futureCommand, topic);
        this.defaultPushConsumer.getClientInstance().sendMessageAsync(wrappered);

    }

}
