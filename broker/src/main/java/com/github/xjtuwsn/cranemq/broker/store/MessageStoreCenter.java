package com.github.xjtuwsn.cranemq.broker.store;

import com.github.xjtuwsn.cranemq.broker.BrokerController;
import com.github.xjtuwsn.cranemq.common.command.RemoteCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @project:cranemq
 * @file:StoreService
 * @author:wsn
 * @create:2023/10/03-16:35
 */
public class MessageStoreCenter {
    private static final Logger log = LoggerFactory.getLogger(MessageStoreCenter.class);
    private BrokerController brokerController;
    private CommitLog commitLog;

    public MessageStoreCenter(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.commitLog = new CommitLog(this.brokerController);
    }
    public void putMessage(RemoteCommand remoteCommand) {
        if (remoteCommand == null) {
            log.error("Null put message request");
        }
        this.commitLog.writeMessage(remoteCommand);
    }
    public void start() {
        this.commitLog.start();
    }
}