package com.github.xjtuwsn.cranemq.broker.store;

import com.github.xjtuwsn.cranemq.broker.BrokerController;
import com.github.xjtuwsn.cranemq.broker.store.comm.AsyncRequest;
import com.github.xjtuwsn.cranemq.broker.store.comm.StoreRequestType;
import com.github.xjtuwsn.cranemq.broker.store.pool.OutOfHeapMemoryPool;
import com.github.xjtuwsn.cranemq.common.command.RemoteCommand;
import com.github.xjtuwsn.cranemq.common.command.payloads.MQProduceRequest;
import com.github.xjtuwsn.cranemq.common.utils.BrokerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
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
    private OutOfHeapMemoryPool memoryPool;

    private CreateMappedFileService createMappedFileService;

    public CommitLog(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.head = new MappedFile(this.headIndex);
        this.tail = new MappedFile(this.tailIndex);
        this.head.next = this.tail;
        this.tail.prev = this.head;
        this.mappedTable.put(this.headIndex, this.head);
        this.mappedTable.put(this.tailIndex, this.tail);
        if (brokerController.getPersistentConfig().isEnableOutOfMemory()) {
            this.memoryPool = new OutOfHeapMemoryPool(brokerController.getPersistentConfig());
        }

        this.createMappedFileService = new CreateMappedFileService();
    }

    public void start() {
        if (this.memoryPool != null) {
            this.memoryPool.init();
        }
        File rootDir = new File(brokerController.getPersistentConfig().getCommitLogPath());
        int index = 0;
        File[] files = rootDir.listFiles();
        Arrays.sort(files, Comparator.comparing(File::getName));
        for (File file : files) {
            MappedFile mappedFile = null;
            if (this.brokerController.getPersistentConfig().isEnableOutOfMemory()) {
                mappedFile = new MappedFile(index, file.getName(),
                        this.brokerController.getPersistentConfig(), this.memoryPool);
            } else {
                mappedFile = new MappedFile(index, file.getName(),
                        this.brokerController.getPersistentConfig());
            }

            this.insertBeforeTail(mappedFile);
            this.mappedTable.put(index, mappedFile);
            index++;
        }
        MappedFile temp = head.next;
        while (temp != tail) {
            System.out.println(temp);
            temp = temp.next;
        }
        this.createMappedFileService.start();
    }

    public void writeMessage(RemoteCommand remoteCommand) {

        MappedFile last = tail.prev;
        if (last == head) {
            // 创建新的
            last = this.createMappedFileService.putCreateRequest(0);
        }

        MQProduceRequest produceRequest = (MQProduceRequest) remoteCommand.getPayLoad();
        StoreInnerMessage innerMessage = new StoreInnerMessage(produceRequest.getMessage(),
                produceRequest.getWriteQueue());
        this.tailLock.lock();
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

    class CreateMappedFileService extends Thread {
        private final Logger log = LoggerFactory.getLogger(CreateMappedFileService.class);
        private boolean isStop = false;
        private LinkedBlockingQueue<AsyncRequest> requestQueue;
        private ConcurrentHashMap<String, AsyncRequest> requestTable = new ConcurrentHashMap<>();
        public CreateMappedFileService() {
            this.requestQueue = new LinkedBlockingQueue<>(500);
        }
        @Override
        public void run() {
            while (!isStop && createLoop()) {

            }
        }

        private boolean createLoop() {
            try {
                AsyncRequest request = this.requestQueue.poll(3000, TimeUnit.SECONDS);
                if (request == null) {
                    log.error("Receive null request from queue");
                    return true;
                }
                if (request.getRequestType() == StoreRequestType.CREATE_MAPPED_FILE) {
                    String fileName = request.getFileName();
                    AsyncRequest expect = this.requestTable.get(fileName);

                    if (expect == null) {
                        log.error("Request has timeouted");
                        return true;
                    }
                    if (expect != request) {
                        log.warn("Expected to be the same request");
                        return true;
                    }
                    System.out.println(request);
                    MappedFile mappedFile = null;
                    if (CommitLog.this.brokerController.getPersistentConfig().isEnableOutOfMemory()) {
                        mappedFile = new MappedFile(request.getIndex(), request.getFileName(),
                                CommitLog.this.brokerController.getPersistentConfig(),
                                CommitLog.this.memoryPool);
                        log.info("Create with memory pool");
                    } else {
                        mappedFile = new MappedFile(request.getIndex(), request.getFileName(),
                                CommitLog.this.brokerController.getPersistentConfig());
                        log.info("Create without memory pool");
                    }

                    tailLock.lock();
                    MappedFile prev = tail.prev;
                    if (prev != head && prev.canWrite()) {
                        mappedFile.markPre();
                    }
                    mappedTable.put(request.getIndex(), mappedFile);
                    insertBeforeTail(mappedFile);
                    tailLock.unlock();
                    log.info("Now there are {} mappedfile.", mappedTable.size());
                }
            } catch (InterruptedException e) {
                log.error("InterruptedException");
                return false;
            }
            return true;
        }

        public MappedFile putCreateRequest(int index) {
            int preCreate = 2;
            int fileSize = CommitLog.this.brokerController.getPersistentConfig().getCommitLogMaxSize();

            CountDownLatch count = new CountDownLatch(preCreate);
            String firstFileName = BrokerUtil.makeFileName(index, fileSize);
            AsyncRequest firstFile = new AsyncRequest(index, firstFileName, fileSize);

            this.requestTable.put(firstFileName, firstFile);
            if (CommitLog.this.brokerController.getPersistentConfig().isEnableOutOfMemory()
                    && CommitLog.this.memoryPool != null) {
                if (CommitLog.this.memoryPool.remainSize() >= 1) {
                    firstFile.setCount(count);
                    this.requestQueue.offer(firstFile);
                    preCreate--;
                } else {
                    this.requestTable.remove(firstFileName);
                    log.error("No out of memeory left");
                    return null;
                }
            }

            String secondFileName = BrokerUtil.makeFileName(index + 1, fileSize);
            AsyncRequest secondFile = new AsyncRequest(index + 1, secondFileName, fileSize);
            this.requestTable.put(secondFileName, secondFile);

            if (CommitLog.this.brokerController.getPersistentConfig().isEnableOutOfMemory()
                    && CommitLog.this.memoryPool != null) {
                if (CommitLog.this.memoryPool.remainSize() >= 1) {
                    this.requestQueue.offer(secondFile);
                    secondFile.setCount(count);
                    preCreate--;
                    log.info("Pre Create request send success");
                } else {
                    this.requestTable.remove(secondFileName);
                    count.countDown();
                    log.warn("No out of memory left, so stop pre create");
                }
            }

            try {
                count.await();
                CommitLog.this.tailLock.lock();
                MappedFile mappedFile = CommitLog.this.tail.prev;
                if (mappedFile.isPre()) {
                    mappedFile = mappedFile.prev;
                    this.requestTable.remove(secondFileName);
                }
                this.requestTable.remove(firstFileName);
                CommitLog.this.tailLock.unlock();
                return mappedFile;
            } catch (InterruptedException e) {
                log.error("Interrupted error");
            }

            return null;
        }
        public void setStop() {
            this.isStop = true;
        }
    }
}
