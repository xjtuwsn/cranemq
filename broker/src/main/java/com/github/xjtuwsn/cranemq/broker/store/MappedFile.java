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

public class MappedFile {
    private static final Logger log = LoggerFactory.getLogger(MappedFile.class);
    private int OS_PAGE = 4 * 1024; // 4KB
    public MappedFile next, prev;
    private int index;
    private int fileSize;
    private File file;
    private String fullPath;
    private FileChannel fileChannel;
    private String fileName;
    private PersistentConfig persistentConfig;
    private MappedByteBuffer mappedByteBuffer;
    // 可读的范围：
    // 如果没有开启直接内存：是读指针
    // 如果开启了：是提交指针
    private AtomicInteger writePointer; // 当前写指针
    private AtomicInteger commitPointer; // directBuffer中已经提交，即写到filechannel的指针
    private AtomicInteger flushPointer; // 当前刷盘指针
    private ByteBuffer directBuffer;
    private OutOfHeapMemoryPool memoryPool;
    private ReentrantLock writeLock = new ReentrantLock();
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
        if (this.memoryPool != null) {
            this.directBuffer = this.memoryPool.borrowMemmory();
        }
        this.unCommitEntryList = new UnCommitEntryList();

    }

    public ByteBuffer getWriteBuffer() {
        return this.directBuffer != null ? this.directBuffer : this.mappedByteBuffer;
    }

    public PutMessageResponse putMessage(StoreInnerMessage innerMessage) {
        if (innerMessage == null) {
            log.error("Get error inner message null");
            return new PutMessageResponse(StoreResponseType.PARAMETER_ERROR);
        }
        log.info("Begin put message");
        writeLock.lock();
        try {
            final byte[] topicData = innerMessage.getTopic().getBytes(MQConstant.CHARSETNAME);
            int topicLen = topicData.length;

            final byte[] tagData = innerMessage.getTag().getBytes(MQConstant.CHARSETNAME);
            int tagLen = tagData.length;

            final byte[] body = innerMessage.getBody();
            int bodyLen = body.length;

            int queueSelected = innerMessage.getMessageQueue().getQueueId();

            int total = this.calTotalLength(topicLen, tagLen, bodyLen);

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

            writeBuffer.putInt(queueSelected);

            writePointer.getAndAdd(total);
            long offset = BrokerUtil.calOffset(this.fileName, pos);
            this.unCommitEntryList.append(pos, total, innerMessage.getTopic(), queueSelected, innerMessage.getTag());

//            writeBuffer.position(pos);
//            writeBuffer.limit(pos + total);
//            // TODO 删除同步刷盘，测试异步刷盘和提交
//            this.fileChannel.write(writeBuffer);
//            this.fileChannel.force(false);
            return new PutMessageResponse(StoreResponseType.STORE_OK, offset, total, this);


//            this.commitPointer.getAndAdd(total);
//            writeBuffer.position(0);
//            writeBuffer.limit(total);
//            this.fileChannel.position(this.commitPointer.get() - total);
//            this.fileChannel.write(writeBuffer);

            // bufferNew.position(4);

            // this.fileChannel.force(false);
        } catch (IOException e) {
            log.error("UnsupportedEncodingException when decoding");
            return new PutMessageResponse(StoreResponseType.PARAMETER_ERROR);
        } finally {
            writeLock.unlock();
        }
    }
    // 总长度int + topic长度int + topic + tag长度int + tag + body长度int + body + 队列号int
    public Message readSingleMessage(int start) {
        int readPointer = getReadPointer();
        if (start >= readPointer) {
            // log.error("Out of limit");
            return null;
        }
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        byteBuffer.position(start);
        Message message = null;

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

            message = new Message(topic, tag, body);

        } catch (UnsupportedEncodingException e) {
            log.warn("UnsupportedEncodingException in readSingleMessage");
        }
        return message;
    }

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

        writePointer.getAndAdd(total);
        writeLock.unlock();
        // this.mappedByteBuffer.force();
        return new PutMessageResponse(StoreResponseType.STORE_OK, pos, this);
    }
    private int getReadPointer() {
        return this.directBuffer == null ? this.writePointer.get() : this.commitPointer.get();
    }
    public Pair<Long, Integer> readSingleOffsetIndex(int start) {
        int readPointer = getReadPointer();
        if (start >= readPointer) {
            // log.error("Canot over the read pointer");
            return null;
        }
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        byteBuffer.position(start);
        long offset = byteBuffer.getLong();
        int size = byteBuffer.getInt();
        return new Pair<>(offset, size);
    }

    public List<Pair<Long, Integer>> readOffsetIndex(int start, int number) {
        int readPointer = getReadPointer();
        if (start >= readPointer) {
            // log.error("Canot over the read pointer");
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
    private int calTotalLength(int topicLen, int tagLen, int bodyLen) {
        return 4 + 4 + topicLen + 4 + tagLen + 4 + bodyLen + 4;
        // 总长度int + topic长度int + topic + tag长度int + tag + body长度int + body + 队列号int
    }

    public List<CommitEntry> doCommit(boolean force) {
        if (!persistentConfig.isEnableOutOfMemory() || this.directBuffer == null) {
            return null;
        }
        int last = commitPointer.get();
        int write = writePointer.get();
        if (last == write) {
            return null;
        }
        if (write - last < 4 * OS_PAGE && !force) {
            return null;
        }
        List<CommitEntry> commitEntries = this.unCommitEntryList.getCommitEntries(write);
        ByteBuffer buffer = directBuffer.slice();
        buffer.position(last);
        buffer.limit(write);
        try {
            this.fileChannel.position(last);
            this.fileChannel.write(buffer);
            log.info("Finish one commit, commit {} --> {}", last, write);
            commitPointer.set(write);
            return commitEntries;
        } catch (IOException e) {
            log.error("Move file channel posistion error");
            return null;
        }
    }
    // TODO 刷盘测试
    public void doFlush() {
        if (persistentConfig.isEnableOutOfMemory() && this.directBuffer != null) {
            int last = flushPointer.get();
            int commit = commitPointer.get();
            if (last == commit) {
                // log.info("Do not need to flush disk because commit the same");
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
                // log.info("Do not need to flush disk because write the same");
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

    public String getFileName() {
        return fileName;
    }

    class UnCommitEntryList {
        private LinkedBlockingQueue<CommitEntry> commitEntries = new LinkedBlockingQueue<>();

        public void append(int offset, int size, String topic, int queueId, String tag) {
            commitEntries.offer(new CommitEntry(topic, fileName, queueId, offset, size, tag));
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
