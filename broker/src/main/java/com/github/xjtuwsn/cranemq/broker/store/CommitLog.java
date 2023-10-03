package com.github.xjtuwsn.cranemq.broker.store;

import com.github.xjtuwsn.cranemq.broker.BrokerController;
import com.github.xjtuwsn.cranemq.common.command.RemoteCommand;
import com.github.xjtuwsn.cranemq.common.command.payloads.MQProduceRequest;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @project:cranemq
 * @file:CommitLog
 * @author:wsn
 * @create:2023/10/03-10:19
 */
public class CommitLog {
    private BrokerController brokerController;
    private MappedFile head, tail;
    private int headIndex = -1;
    private int tailIndex = -2;

    private Map<Integer, MappedFile> mappedTable = new ConcurrentHashMap<>();
    private ReentrantLock tailLock = new ReentrantLock(false);

    public CommitLog(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.head = new MappedFile(this.headIndex);
        this.tail = new MappedFile(this.tailIndex);
        this.head.next = this.tail;
        this.tail.prev = this.head;
        this.mappedTable.put(this.headIndex, this.head);
        this.mappedTable.put(this.tailIndex, this.tail);
    }

    public void start() {
        File rootDir = new File(brokerController.getPersistentConfig().getCommitLogPath());
        int index = 0;
        for (File file : rootDir.listFiles()) {
            MappedFile mappedFile = new MappedFile(index, file.getName(), this.brokerController.getPersistentConfig());
            this.insertBeforeTail(mappedFile);
            this.mappedTable.put(index, mappedFile);
            index++;
        }
        MappedFile temp = head.next;
        while (temp != tail) {
            System.out.println(temp);
            temp = temp.next;
        }
    }

    public void writeMessage(RemoteCommand remoteCommand) {
        this.tailLock.lock();
        MappedFile last = tail.prev;
        if (last == head) {
            // 创建新的
        }
        MQProduceRequest produceRequest = (MQProduceRequest) remoteCommand.getPayLoad();
        StoreInnerMessage innerMessage = new StoreInnerMessage(produceRequest.getMessage(),
                produceRequest.getWriteQueue());
        last.putMessage(innerMessage);

        this.tailLock.unlock();
    }
    public void writeBatchMessage(RemoteCommand remoteCommand) {

    }
    private void insertBeforeTail(MappedFile mappedFile) {
        this.tailLock.lock();
        this.tail.prev.next = mappedFile;
        mappedFile.prev = this.tail.prev;
        mappedFile.next = this.tail;
        this.tail.prev = mappedFile;
        this.tailLock.unlock();
    }
}
