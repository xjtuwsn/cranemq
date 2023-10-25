package com.github.xjtuwsn.cranemq.broker.store;

import cn.hutool.core.lang.Pair;
import com.github.xjtuwsn.cranemq.broker.store.cmtlog.CommitEntry;
import com.github.xjtuwsn.cranemq.broker.store.comm.PutMessageResponse;
import com.github.xjtuwsn.cranemq.broker.store.comm.StoreInnerMessage;
import com.github.xjtuwsn.cranemq.broker.store.comm.StoreResponseType;
import com.github.xjtuwsn.cranemq.broker.store.pool.OutOfHeapMemoryPool;
import com.github.xjtuwsn.cranemq.common.constant.MQConstant;
import com.github.xjtuwsn.cranemq.common.entity.Message;
import com.github.xjtuwsn.cranemq.common.utils.BrokerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @project:cranemq
 * @file:MappedFile
 * @author:wsn
 * @create:2023/10/03-10:20
 */

/**
 * 一个commitLog分为很多子文件，一个消费队列存储文件也分为很多子文件
 * mappedFile作为这些子文件的底层实现，实现对文件的读写
 * @author wsn
 */
public class MappedFile {
    private static final Logger log = LoggerFactory.getLogger(MappedFile.class);
    // 页面大小
    private int OS_PAGE = 4 * 1024; // 4KB

    // 以链表组织，前驱和后继
    public MappedFile next, prev;
    // 文件索引
    private int index;
    private int fileSize;
    private File file;
    private String fullPath;
    private FileChannel fileChannel;
    private String fileName;
    private PersistentConfig persistentConfig;
    // mmap buffer
    private MappedByteBuffer mappedByteBuffer;
    // 可读的范围：
    // 如果没有开启直接内存：是读指针
    // 如果开启了：是提交指针
    private AtomicInteger writePointer; // 当前写指针
    private AtomicInteger commitPointer; // directBuffer中已经提交，即写到filechannel的指针
    private AtomicInteger flushPointer; // 当前刷盘指针
    // 堆外内存
    private ByteBuffer directBuffer;
    // 堆外内存池
    private OutOfHeapMemoryPool memoryPool;
    // 写锁
    private ReentrantLock writeLock = new ReentrantLock();
    // 暂存所有未提交的消息
    private UnCommitEntryList unCommitEntryList;
    private Status status;

    enum Status {
        FULL,
        CAN_WRITE,
        PRE_CREATE
    }

    public MappedFile(int index) {
        this(index, 0, "", "", null, null);
    }
    public MappedFile(int index, int fileSize, String fileName, PersistentConfig persistentConfig) {
        this(index, fileSize, fileName, persistentConfig.getCommitLogPath() + fileName,
                persistentConfig, null);
    }
    public MappedFile(int index, int fileSize, String fileName, PersistentConfig persistentConfig,
                      OutOfHeapMemoryPool memoryPool) {
        this(index, fileSize, fileName, persistentConfig.getCommitLogPath() + fileName,
                persistentConfig, memoryPool);
    }
    public MappedFile(int index, int fileSize, String fileName, String fullPath, PersistentConfig persistentConfig) {
        this(index, fileSize, fileName, fullPath, persistentConfig, null);
    }

    public MappedFile(int index, int fileSize, String fileName, String fullPath, PersistentConfig persistentConfig,
                      OutOfHeapMemoryPool memoryPool) {
        this.persistentConfig = persistentConfig;
        this.index = index;
        this.fileName = fileName;
        this.memoryPool = memoryPool;
        if (this.fileName == "") {
            return;
        }
        this.fileSize = fileSize;
        this.fullPath = fullPath;

        // 读取文件，并从filechannel中映射byteMappedBuffer
        this.file = new File(fullPath);
        try {
            if (!this.file.exists()) {
                this.file.createNewFile();
            }
            this.fileChannel = new RandomAccessFile(fullPath, "rw").getChannel();
        } catch (IOException e) {
            log.error("Create file channel error");
        }
        this.writePointer = new AtomicInteger(this.fileSize);
        this.commitPointer = new AtomicInteger(this.fileSize);
        this.flushPointer = new AtomicInteger(this.fileSize);
        if (this.mappedByteBuffer == null) {
            try {
                this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE,
                        0, this.fileSize);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        if (this.fileSize == this.persistentConfig.getCommitLogMaxSize()) {
            this.status = Status.FULL;
        } else {
            this.status = Status.CAN_WRITE;
        }
        // 如果内存池存在，也就是允许堆外内存，则借用堆外内存
        if (this.memoryPool != null) {
            this.directBuffer = this.memoryPool.borrowMemmory();
        }
        this.unCommitEntryList = new UnCommitEntryList();

    }

    /**
     * 根据是否开启堆外内存，获取写的buffer
     * @return 开启的话，就是directBuffe，mappedBuffer只读，做到读写分离
     */
    public ByteBuffer getWriteBuffer() {
        return this.directBuffer != null ? this.directBuffer : this.mappedByteBuffer;
    }

    /**
     * 向commitLog中写入消息
     * @param innerMessage
     * @return
     */
    public PutMessageResponse putMessage(StoreInnerMessage innerMessage) {
        if (innerMessage == null) {
            log.error("Get error inner message null");
            return new PutMessageResponse(StoreResponseType.PARAMETER_ERROR);
        }
        log.info("Begin put message");
        writeLock.lock();
        try {
            // 总长度int + topic长度int + topic + tag长度int + tag + body长度int + body + id长度int + id + retry + 队列号int
            final byte[] idData = innerMessage.getId().getBytes(MQConstant.CHARSETNAME);
            int idLen = idData.length;

            final byte[] topicData = innerMessage.getTopic().getBytes(MQConstant.CHARSETNAME);
            int topicLen = topicData.length;

            final byte[] tagData = innerMessage.getTag().getBytes(MQConstant.CHARSETNAME);
            int tagLen = tagData.length;

            final byte[] body = innerMessage.getBody();
            int bodyLen = body.length;

            int queueSelected = innerMessage.getMessageQueue().getQueueId();

            int retry = innerMessage.getRetry();

            int total = this.calTotalLength(topicLen, tagLen, bodyLen, idLen);

            if (this.writePointer.get() + total >= this.fileSize) { // 写不下了
//                this.sweepThisFile();
                return new PutMessageResponse(StoreResponseType.NO_ENOUGH_SPACE);
            }

            ByteBuffer writeBuffer = this.getWriteBuffer().slice();
            int pos = writePointer.get();
            writeBuffer.position(pos);

            writeBuffer.putInt(total);

            writeBuffer.putInt(topicLen);
            writeBuffer.put(topicData);

            writeBuffer.putInt(tagLen);
            writeBuffer.put(tagData);

            writeBuffer.putInt(bodyLen);
            writeBuffer.put(body);

            writeBuffer.putInt(idLen);
            writeBuffer.put(idData);

            writeBuffer.putInt(retry);

            writeBuffer.putInt(queueSelected);

            writePointer.getAndAdd(total);
            long offset = BrokerUtil.calOffset(this.fileName, pos);
            this.unCommitEntryList.append(pos, total, innerMessage.getTopic(), queueSelected, innerMessage.getTag(),
                    innerMessage.getDelay());

            return new PutMessageResponse(StoreResponseType.STORE_OK, offset, total, this);

        } catch (IOException e) {
            log.error("UnsupportedEncodingException when decoding");
            return new PutMessageResponse(StoreResponseType.PARAMETER_ERROR);
        } finally {
            writeLock.unlock();
        }
    }
    /**
     * 读对应偏移位置的信息
     * @param start
     * @return
     */
    public StoreInnerMessage readSingleMessage(int start) {
        // 获取当前读指针
        int readPointer = getReadPointer();
        if (start >= readPointer) {
            return null;
        }
        // 定位开始读
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        byteBuffer.position(start);
        StoreInnerMessage message = null;

        try {
            int total = byteBuffer.getInt();

            int topicSize = byteBuffer.getInt();
            byte[] topicByte = new byte[topicSize];
            byteBuffer.get(topicByte);
            String topic = new String(topicByte, MQConstant.CHARSETNAME);

            int tagSize = byteBuffer.getInt();
            byte[] tagByte = new byte[tagSize];
            byteBuffer.get(tagByte);
            String tag = new String(tagByte, MQConstant.CHARSETNAME);

            int bodySize = byteBuffer.getInt();
            byte[] body = new byte[bodySize];
            byteBuffer.get(body);


            int idSize = byteBuffer.getInt();
            byte[] idByte = new byte[idSize];
            byteBuffer.get(idByte);
            String id = new String(idByte, MQConstant.CHARSETNAME);

            int retry = byteBuffer.getInt();
            int queueId = byteBuffer.getInt();
            message = new StoreInnerMessage(topic, tag, id, body, retry, queueId);

        } catch (UnsupportedEncodingException e) {
            log.warn("UnsupportedEncodingException in readSingleMessage");
        }
        return message;
    }

    /**
     * 将commitLog中的信息写入到消费队列中
     * @param offset log中的偏移
     * @param size 消息长度
     * @return
     */
    public PutMessageResponse putOffsetIndex(long offset, int size) {
        if (offset < 0 || size < 0) {
            return new PutMessageResponse(StoreResponseType.PARAMETER_ERROR);
        }
        int total = persistentConfig.getQueueUnit();
        if (writePointer.get() + total >= persistentConfig.getMaxQueueSize()) {
            return new PutMessageResponse(StoreResponseType.NO_ENOUGH_SPACE);
        }
        writeLock.lock();
        int pos = writePointer.get();
        ByteBuffer writeBuffer = this.mappedByteBuffer.slice();
        writeBuffer.position(pos);

        writeBuffer.putLong(offset);
        writeBuffer.putInt(size);

        // 更新写指针
        writePointer.getAndAdd(total);
        writeLock.unlock();

        return new PutMessageResponse(StoreResponseType.STORE_OK, pos, this);
    }

    /**
     * 获取读指针
     * @return 如果开启堆外内存，那么只有提交的消息才能读，没开启的话，写过的就可以读
     */
    private int getReadPointer() {
        return this.directBuffer == null ? this.writePointer.get() : this.commitPointer.get();
    }

    /**
     * 读取消费队列中的索引信息
     * @param start
     * @return
     */
    public Pair<Long, Integer> readSingleOffsetIndex(int start) {
        int readPointer = getReadPointer();
        if (start >= readPointer) {
            return null;
        }
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        byteBuffer.position(start);
        long offset = byteBuffer.getLong();
        int size = byteBuffer.getInt();
        return new Pair<>(offset, size);
    }

    /**
     * 批量读取
     * @param start
     * @param number
     * @return
     */
    public List<Pair<Long, Integer>> readOffsetIndex(int start, int number) {
        int readPointer = getReadPointer();
        if (start >= readPointer) {
            return null;
        }
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        byteBuffer.position(start);
        List<Pair<Long, Integer>> list = new ArrayList<>();

        while (number-- > 0 && start < readPointer) {
            long offset = byteBuffer.getLong();
            int size = byteBuffer.getInt();
            list.add(new Pair<>(offset, size));
            start += persistentConfig.getQueueUnit();
        }

        return list;
    }
    private int calTotalLength(int topicLen, int tagLen, int bodyLen, int idLen) {
        return 4 + 4 + topicLen + 4 + tagLen + 4 + bodyLen + 4 + idLen + 4 + 4;
        // 总长度int + topic长度int + topic + tag长度int + tag + body长度int + body + id长度int + id + retry + 队列号int
    }

    /**
     * commitLog中进行提交
     * @param force 是否强制
     * @return 提交的信息集合
     */
    public List<CommitEntry> doCommit(boolean force) {
        // 如果没开启堆外内存就不需要提交
        if (!persistentConfig.isEnableOutOfMemory() || this.directBuffer == null) {
            return null;
        }
        int last = commitPointer.get();
        int write = writePointer.get();
        if (last == write) {
            return null;
        }
        // 如果消息过少不是强制提交就不需要
        if (write - last < 4 * OS_PAGE && !force) {
            return null;
        }
        // 拿到当前写指针之前的所有未提交信息
        List<CommitEntry> commitEntries = this.unCommitEntryList.getCommitEntries(write);
        ByteBuffer buffer = directBuffer.slice();
        buffer.position(last);
        buffer.limit(write);
        try {
            this.fileChannel.position(last);
            this.fileChannel.write(buffer);
            log.info("Finish one commit, commit {} --> {}", last, write);
            // 更新提交指针
            commitPointer.set(write);
            return commitEntries;
        } catch (IOException e) {
            log.error("Move file channel posistion error");
            return null;
        }
    }

    /**
     * 执行刷盘
     */
    public void doFlush() {
        // 确定刷盘到那里为止，开启堆外内存刷到提交，否则，刷到写指针
        if (persistentConfig.isEnableOutOfMemory() && this.directBuffer != null) {
            int last = flushPointer.get();
            int commit = commitPointer.get();
            if (last == commit) {
                return;
            }
            try {
                log.info("Finish direct buffer flush, flush pointer {} ---> {}", last, commit);
                this.fileChannel.force(false);
                flushPointer.set(commit);
            } catch (IOException e) {
                log.error("Flush disk failed");
                return;
            }
        } else {
            int write = writePointer.get();
            int flush = flushPointer.get();
            if (flush == write) {
                return;
            }
            log.info("Finish direct buffer flush, flush pointer {} ---> {}", flush, write);
            this.mappedByteBuffer.force();
            flushPointer.set(write);
        }
    }

    private void sweepThisFile() {
        this.doCommit(true);
        this.doFlush();
        this.markFull();
        this.returnMemory();
    }

    public void returnMemory() {
        if (this.memoryPool != null && this.directBuffer!= null) {
            this.memoryPool.returnMemory(directBuffer);
            this.directBuffer = null;
            this.memoryPool = null;
        }
    }
    public boolean ownDirectMemory() {
        return persistentConfig.isEnableOutOfMemory() && this.memoryPool != null && this.directBuffer != null;
    }
    public void setStatus(Status status) {
        this.status = status;
    }

    public boolean isFull() {
        return this.status == Status.FULL;
    }

    public boolean isPre() {
        return this.status == Status.PRE_CREATE;
    }

    public boolean canWrite() {
        return this.writePointer.get() != this.fileSize;
    }
    public int getWrite() {
        return this.writePointer.get();
    }

    public int getFlush() {
        return this.flushPointer.get();
    }
    public void markFull() {
        this.status = Status.FULL;
        this.writePointer.set(fileSize);
        this.commitPointer.set(fileSize);
        this.flushPointer.set(fileSize);
    }
    public void markWrite() {
        this.status = Status.CAN_WRITE;
    }
    public void markPre() {
        this.status = Status.PRE_CREATE;
    }

    public int getIndex() {
        return index;
    }

    public void setWritePointer(int writePointer) {
        this.writePointer.set(writePointer);
    }

    public void setCommitPointer(int commitPointer) {
        this.commitPointer.set(commitPointer);
    }

    public void setFlushPointer(int flushPointer) {
        this.flushPointer.set(flushPointer);
    }

    public long lastModified() {
        return this.file.lastModified();
    }

    public String getFileName() {
        return fileName;
    }

    /**
     * 保存所有未提交的信息
     */
    class UnCommitEntryList {
        private LinkedBlockingQueue<CommitEntry> commitEntries = new LinkedBlockingQueue<>();

        public void append(int offset, int size, String topic, int queueId, String tag, long delay) {
            commitEntries.offer(new CommitEntry(topic, fileName, queueId, offset, size, tag, delay));
        }

        public List<CommitEntry> getCommitEntries(int limit) {
            List<CommitEntry> list = new ArrayList<>();
            while (!commitEntries.isEmpty() && commitEntries.peek().getOffsetInPage() <= limit) {
                list.add(commitEntries.poll());
            }
            return list;
        }
    }
    @Override
    public String toString() {
        return "MappedFile{" +
                "index=" + index +
                ", file=" + file +
                ", fileChannel=" + fileChannel +
                ", fileName='" + fileName + '\'' +
                ", persistentConfig=" + persistentConfig +
                ", mappedByteBuffer=" + mappedByteBuffer +
                '}';
    }
}
