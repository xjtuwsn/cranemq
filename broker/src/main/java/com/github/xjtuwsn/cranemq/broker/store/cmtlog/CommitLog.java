package com.github.xjtuwsn.cranemq.broker.store.cmtlog;

import com.github.xjtuwsn.cranemq.broker.BrokerController;
import com.github.xjtuwsn.cranemq.broker.store.comm.*;
import com.github.xjtuwsn.cranemq.broker.store.*;
import com.github.xjtuwsn.cranemq.broker.store.pool.OutOfHeapMemoryPool;
import com.github.xjtuwsn.cranemq.common.command.RemoteCommand;
import com.github.xjtuwsn.cranemq.common.utils.BrokerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @project:cranemq
 * @file:CommitLog
 * @author:wsn
 * @create:2023/10/03-10:19
 * commitLog主体实现类
 */
public class CommitLog extends AbstractLinkedListOrganize implements GeneralStoreService {
    private static final Logger log = LoggerFactory.getLogger(CommitLog.class);
    private BrokerController brokerController;

    private OutOfHeapMemoryPool memoryPool;

    // 创建子文件线程
    private CreateMappedFileService createMappedFileService;
    // 提交线程
    private CommitService commitService;
    private ScheduledExecutorService commitScheduleService;
    private ScheduledExecutorService scanDirectMemoryService;
    private MessageStoreCenter messageStoreCenter;
    private long recordOffset;
    private int recordSize;
    private long lastCommit;

    public CommitLog(BrokerController brokerController, MessageStoreCenter messageStoreCenter) {
        this.messageStoreCenter = messageStoreCenter;
        this.brokerController = brokerController;
        init();
        if (brokerController.getPersistentConfig().isEnableOutOfMemory()) {
            this.memoryPool = new OutOfHeapMemoryPool(brokerController.getPersistentConfig());
            this.scanDirectMemoryService = new ScheduledThreadPoolExecutor(1);
            this.commitScheduleService = new ScheduledThreadPoolExecutor(2);
        }
        this.createMappedFileService = new CreateMappedFileService();
        this.commitService = new CommitService();
    }
    @Override
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
            // 只给最后两个文件对外内存，并开线程定期扫描
            if (this.brokerController.getPersistentConfig().isEnableOutOfMemory() && index >= files.length - 2) {
                mappedFile = new MappedFile(index, brokerController.getPersistentConfig().getCommitLogMaxSize(),
                        file.getName(),
                        this.brokerController.getPersistentConfig(), this.memoryPool);
            } else {
                mappedFile = new MappedFile(index, brokerController.getPersistentConfig().getCommitLogMaxSize(),
                        file.getName(),
                        this.brokerController.getPersistentConfig());
            }

            this.insertBeforeTail(mappedFile);
            index++;
        }
        // 重置之前保存的最大offset所对应文件的指针
        if (files.length > 0) {
            int find = BrokerUtil.findMappedIndex(recordOffset, files[0].getName(),
                    brokerController.getPersistentConfig().getCommitLogMaxSize());
            int nextPos = BrokerUtil.offsetInPage(recordOffset,
                    brokerController.getPersistentConfig().getCommitLogMaxSize()) + recordSize;
            MappedFile mappedFile = mappedTable.get(find);
            if (mappedFile != null) {
                log.info("CommitLog file [index {}, name {}] recovery from {}", find, mappedFile.getFileName() ,nextPos);
                mappedFile.setWritePointer(nextPos);
                mappedFile.setCommitPointer(nextPos);
                mappedFile.setFlushPointer(nextPos);
                // 把当前位置之后的文件标为空
                MappedFile cur = mappedFile.next;
                while (cur != tail) {
                    cur.setWritePointer(0);
                    cur.setCommitPointer(0);
                    cur.setFlushPointer(0);
                    cur = cur.next;
                }
            }


        }
        // 定时扫描进行提交和查询是否需要归还堆外内存
        if (brokerController.getPersistentConfig().isEnableOutOfMemory()) {
            this.commitService.start();
            this.commitScheduleService.scheduleAtFixedRate(() -> {
                commit(true);
            }, 100, brokerController.getPersistentConfig().getAsyncCommitInterval(), TimeUnit.MILLISECONDS);
            this.scanDirectMemoryService.scheduleAtFixedRate(() -> {
                scanDirectMemory();
            }, 1000, 10 * 1000, TimeUnit.MILLISECONDS);
            log.info("CommitService, CommitScheduleService and ScanDirectMemoryService start successfully");
        }
        this.createMappedFileService.start();
    }

    /**
     * 不需要堆外内存就归还，例如文件已经写满
     */
    private void scanDirectMemory() {
        Iterator<MappedFile> iterator = this.iterator();
        while (iterator.hasNext()) {
            MappedFile mappedFile = iterator.next();
            if (!mappedFile.canWrite() && mappedFile.ownDirectMemory()) {
                mappedFile.returnMemory();
                log.info("MappedFile {} return 1 direct memeoy", mappedFile.getFileName());
            }
        }
    }

    /**
     * 根据消费队列中记录的最大offset，设置对应文件的写指针
     * @param offset
     * @param size
     */
    public void recoveryFromQueue(long offset, int size) {
        if (offset >= this.recordOffset) {
            this.recordOffset = offset;
            this.recordSize = size;
        }
    }
    private MappedFile getLastFile() {
        MappedFile last = tail.prev;
        if (last == head) {
            last = this.createMappedFileService.putCreateRequest(this.nextIndex());
        } else if (last.prev != head && last.prev.canWrite()) {
            last = last.prev;
        }
        last.markWrite();
        return last;
    }
    @Override
    public void close() {
        this.commit(true);
        MappedFile lastFile = getLastFile();
        lastFile.doFlush();
        lastFile.returnMemory();
    }

    /**
     * 向mappedfile写入一条消息
     * @param innerMessage
     * @return
     */
    public PutMessageResponse writeMessage(StoreInnerMessage innerMessage) {

        MappedFile last = this.getLastFile();

        // 先尝试进行写入
        PutMessageResponse response = last.putMessage(innerMessage);
        // 如果待写入的文件已经满了
        while (response.getResponseType() == StoreResponseType.NO_ENOUGH_SPACE) {

            // 提交当前文件
            commit(true);

            // 进行刷盘
            last.doFlush();

            // 标记为满
            last.markFull();

            // 归还堆外内存
            last.returnMemory();

            // 如果预创建了一个文件，则直接进行切换
            if (last.next != tail) {
                last = last.next;
            }
            else {
                // 否则产生创建文件请求
                last = this.createMappedFileService.putCreateRequest(this.nextIndex());
            }
            // 对新文件写入
            response = last.putMessage(innerMessage);
        }
        log.info("Put message to commit, response is {}", response);
        return response;
    }

    public boolean tryLock() {
        this.tailLock.lock();
        return true;
    }

    /**
     * 进行提交
     * @param force
     */
    public void commit(boolean force) {
        MappedFile mappedFile = getLastFile();
        // 提交指定文件
        List<CommitEntry> list = mappedFile.doCommit(force);
        // 将提交的信息加入到提交队列中
        if (list != null) {
            this.messageStoreCenter.putEntries(list);
        }
    }
    public void release() {
        this.tailLock.unlock();
    }

    /**
     * 提交线程
     */
    class CommitService extends Thread {
        private final Logger log = LoggerFactory.getLogger(CommitLog.class);
        private boolean isStop = false;
        @Override
        public void run() {
            while (!isStop) {
                commit(false);
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    log.warn("Commit service has been Interrupted");
                }
            }
        }



        public void setStop() {
            this.isStop = true;
        }
    }

    /**
     * 创建commitLog 的子文件的线程
     */
    class CreateMappedFileService extends CreateServiceThread {
        private final Logger log = LoggerFactory.getLogger(CreateMappedFileService.class);
        private AtomicLong lastCreateOffset = new AtomicLong(-1);
        public CreateMappedFileService() {
            super();
        }
        @Override
        protected boolean createLoop() {
            try {

                // 不断从队列中取出创建请求
                AsyncRequest request = this.requestQueue.poll(3000, TimeUnit.MILLISECONDS);
                if (request == null) {
                    return true;
                }
                if (request.getRequestType() == StoreRequestType.CREATE_MAPPED_FILE) {
                    String fileName = request.getFileName();
                    // 判断是否是原始请求
                    AsyncRequest expect = this.requestTable.get(fileName);

                    if (expect == null) {
                        log.error("Request has timeouted");
                        return true;
                    }
                    if (expect != request) {
                        log.warn("Expected to be the same request");
                        return true;
                    }
                    MappedFile mappedFile = null;
                    // 根据是否允许堆外内存进行创建
                    if (CommitLog.this.brokerController.getPersistentConfig().isEnableOutOfMemory()) {
                        mappedFile = new MappedFile(request.getIndex(), request.getFileSize(), request.getFileName(),
                                CommitLog.this.brokerController.getPersistentConfig(),
                                CommitLog.this.memoryPool);
                        log.info("Create with memory pool");
                    } else {
                        mappedFile = new MappedFile(request.getIndex(), request.getFileSize(), request.getFileName(),
                                CommitLog.this.brokerController.getPersistentConfig());
                        log.info("Create without memory pool");
                    }
                    // 重置新文件的指针
                    mappedFile.setWritePointer(0);
                    mappedFile.setCommitPointer(0);
                    mappedFile.setFlushPointer(0);
                    tailLock.lock();
                    // 插入，并对预创建的文件进行标记
                    MappedFile prev = tail.prev;
                    if (prev != head && prev.canWrite()) {
                        mappedFile.markPre();
                    }
                    insertBeforeTail(mappedFile);
                    request.getCount().countDown();
                    // 更新最新创建的文件
                    lastCreateOffset.getAndSet(request.getIndex() *
                            (long) brokerController.getPersistentConfig().getCommitLogMaxSize());
                    tailLock.unlock();
                    log.info("Now there are {} mappedfile.", mappedTable.size());
                }
            } catch (InterruptedException e) {
                log.error("InterruptedException");
                return false;
            }
            return true;
        }

        /**
         * 根据要创建的索引放入请求
         * @param index
         * @return
         */
        @Override
        protected MappedFile putCreateRequest(int index) {
            if (index * (long) brokerController.getPersistentConfig().getCommitLogMaxSize() <= lastCreateOffset.get()) {
                log.info("{} has been created", index);
                return tail.prev;
            }
            log.info("Receive index {} for create", index);
            int preCreate = 2;
            int fileSize = CommitLog.this.brokerController.getPersistentConfig().getCommitLogMaxSize();

            // 第一个文件
            CountDownLatch count = new CountDownLatch(preCreate);
            String firstFileName = BrokerUtil.makeFileName(index, fileSize);
            AsyncRequest firstFile = new AsyncRequest(index, firstFileName, fileSize);

            this.requestTable.put(firstFileName, firstFile);
            int remainSize = Integer.MAX_VALUE;
            // 如果允许借用内存
            if (CommitLog.this.brokerController.getPersistentConfig().isEnableOutOfMemory()
                    && CommitLog.this.memoryPool != null) {
                remainSize = CommitLog.this.memoryPool.remainSize();
                if (remainSize >= 1) {
                    firstFile.setCount(count);
                    this.requestQueue.offer(firstFile);
                    remainSize--;
                    preCreate--;
                } else {
                    this.requestTable.remove(firstFileName);
                    log.error("No out of memeory left");
                    return null;
                }
            }

            // 第二个预创建的文件
            String secondFileName = BrokerUtil.makeFileName(index + 1, fileSize);
            AsyncRequest secondFile = new AsyncRequest(index + 1, secondFileName, fileSize);
            this.requestTable.put(secondFileName, secondFile);

            // 如果没有堆外内存，就不创建了
            if (CommitLog.this.brokerController.getPersistentConfig().isEnableOutOfMemory()
                    && CommitLog.this.memoryPool != null) {
                if (remainSize >= 1) {
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
                // 等待创建完成
                count.await();
                CommitLog.this.tailLock.lock();

                MappedFile mappedFile = CommitLog.this.tail.prev;
                // 返回不为预创建的最后一个文件
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

        @Override
        protected MappedFile putCreateRequest(int index, String topic, int queueId) {
            return null;
        }
    }

    public BrokerController getBrokerController() {
        return brokerController;
    }
}
