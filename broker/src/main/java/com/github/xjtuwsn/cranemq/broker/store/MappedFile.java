package com.github.xjtuwsn.cranemq.broker.store;

import com.github.xjtuwsn.cranemq.broker.enums.StoreResponse;
import com.github.xjtuwsn.cranemq.broker.store.pool.OutOfHeapMemoryPool;
import com.github.xjtuwsn.cranemq.common.command.RemoteCommand;
import com.github.xjtuwsn.cranemq.common.constant.MQConstant;
import io.netty.util.internal.StringUtil;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @project:cranemq
 * @file:MappedFile
 * @author:wsn
 * @create:2023/10/03-10:20
 */

public class MappedFile {
    private static final Logger log = LoggerFactory.getLogger(MappedFile.class);
    public MappedFile next, prev;
    private int index;
    private long fileSize;
    private File file;
    private FileChannel fileChannel;
    private String fileName;
    private PersistentConfig persistentConfig;
    private MappedByteBuffer mappedByteBuffer;
    private AtomicLong writePointer;
    private AtomicLong totalPointer;
    private AtomicLong refreshPointer;
    private ByteBuffer byteBuffer;
    private ByteBuffer directBuffer;
    private OutOfHeapMemoryPool memoryPool;
    private Status status;

    enum Status {
        FULL,
        CAN_WRITE,
        PRE_CREATE
    }

    public MappedFile(int index) {
        this(index, "", null, null);
    }

    public MappedFile(int index, String fileName, PersistentConfig persistentConfig) {
        this(index, fileName, persistentConfig, null);
    }
    public MappedFile(String fileName, PersistentConfig persistentConfig, OutOfHeapMemoryPool memoryPool) {
        this(Integer.parseInt(fileName.split(".")[0]) / persistentConfig.getCommitLogMaxSize(),
                fileName, persistentConfig, memoryPool);
    }
    public MappedFile(int index, String fileName, PersistentConfig persistentConfig, OutOfHeapMemoryPool memoryPool) {
        this.persistentConfig = persistentConfig;
        this.index = index;
        this.fileName = fileName;
        log.info("FileName is {}", fileName);
        this.memoryPool = memoryPool;
        if (this.fileName == "") return;
        this.byteBuffer = ByteBuffer.allocate(1024 * 1024);
        String fullPath = this.persistentConfig.getCommitLogPath() + this.fileName;

        this.file = new File(fullPath);
        try {
            if (!this.file.exists()) {
                this.file.createNewFile();
            }
            this.fileChannel = new RandomAccessFile(fullPath, "rw").getChannel();
        } catch (IOException e) {
            log.error("Create file channel error");
        }
        this.fileSize = this.file.length();
        this.writePointer = new AtomicLong(this.fileSize);
        this.totalPointer = new AtomicLong(this.fileSize);
        this.refreshPointer = new AtomicLong(this.fileSize);
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

        if (this.memoryPool != null && !this.isFull()) {
            this.directBuffer = this.memoryPool.borrowMemmory();
        }

    }

    private ByteBuffer getWriteBuffer() {
        return this.directBuffer != null ? this.directBuffer : this.mappedByteBuffer;
    }

    public StoreResponse putMessage(StoreInnerMessage innerMessage) {
        if (innerMessage == null) {
            log.error("Get error inner message null");
            return StoreResponse.PARAMETER_ERROR;
        }
        try {
            final byte[] topicData = innerMessage.getTopic().getBytes(MQConstant.CHARSETNAME);
            int topicLen = topicData.length;

            final byte[] tagData = innerMessage.getTag().getBytes(MQConstant.CHARSETNAME);
            int tagLen = tagData.length;

            final byte[] body = innerMessage.getBody();
            int bodyLen = body.length;

            int queueSelected = innerMessage.getMessageQueue().getQueueId();

            int total = this.calTotalLength(topicLen, tagLen, bodyLen);
            if (this.totalPointer.get() + total > this.persistentConfig.getCommitLogMaxSize()) {
                return StoreResponse.NO_ENOUGH_SPACE;
            }
            
            ByteBuffer writeBuffer = this.getWriteBuffer();
            System.out.println(writeBuffer);
            writeBuffer.putInt(total);
            System.out.println(writeBuffer.remaining());

            writeBuffer.putInt(topicLen);
            writeBuffer.put(topicData);
            System.out.println(writeBuffer.remaining());

            writeBuffer.putInt(tagLen);
            writeBuffer.put(tagData);
            System.out.println(writeBuffer.remaining());

            writeBuffer.putInt(bodyLen);
            writeBuffer.put(body);
            System.out.println(writeBuffer.remaining());

            writeBuffer.putInt(queueSelected);
            this.totalPointer.getAndAdd(total);
            writeBuffer.position(0);
            writeBuffer.limit((int) this.totalPointer.get());
            this.fileChannel.write(writeBuffer);
            System.out.println(this.fileChannel.size());
            this.fileChannel.force(false);
            System.out.println("----------------------");
            System.out.println(this.mappedByteBuffer);
            System.out.println("----------------------");
            writeBuffer.position((int) this.totalPointer.get());
            writeBuffer.limit(this.persistentConfig.getCommitLogMaxSize());
        } catch (IOException e) {
            log.error("UnsupportedEncodingException when decoding");
            return StoreResponse.PARAMETER_ERROR;
        }

        return StoreResponse.STORE_OK;
    }
    private int calTotalLength(int topicLen, int tagLen, int bodyLen) {
        return 4 + 4 + topicLen + 4 + tagLen + 4 + bodyLen + 4;
        // 总长度int + topic长度int + topic + tag长度int + tag + body长度int + body + 队列号int
    }
    // TODO 刷盘测试
    public void flush() {

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
        return this.status == Status.CAN_WRITE;
    }

    public void markFull() {
        this.status = Status.FULL;
    }
    public void markWrite() {
        this.status = Status.CAN_WRITE;
    }
    public void markPre() {
        this.status = Status.PRE_CREATE;
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
