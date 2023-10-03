package com.github.xjtuwsn.cranemq.broker.store;

import com.github.xjtuwsn.cranemq.broker.enums.StoreResponse;
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
    private File file;
    private FileChannel fileChannel;
    private String fileName;
    private PersistentConfig persistentConfig;
    private MappedByteBuffer mappedByteBuffer;
    private AtomicLong writePointer;
    private AtomicLong totalPointer;
    private AtomicLong refreshPointer;
    private ByteBuffer byteBuffer;

    public MappedFile(int index) {
        this(index, "", null);
    }
    public MappedFile(int index, String fileName, PersistentConfig persistentConfig) {
        this.persistentConfig = persistentConfig;
        this.index = index;
        this.fileName = fileName;
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
        this.writePointer = new AtomicLong(this.file.length());
        this.totalPointer = new AtomicLong(this.file.length());
        this.refreshPointer = new AtomicLong(this.file.length());
        if (this.mappedByteBuffer == null) {
            try {
                this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE,
                        0, this.writePointer.get());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
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

            this.byteBuffer.putInt(total);
            System.out.println(byteBuffer.remaining());
            this.byteBuffer.putInt(topicLen);
            this.byteBuffer.put(topicData);
            System.out.println(byteBuffer.remaining());
            this.byteBuffer.putInt(tagLen);
            this.byteBuffer.put(tagData);
            System.out.println(byteBuffer.remaining());
            this.byteBuffer.putInt(bodyLen);
            this.byteBuffer.put(body);
            System.out.println(byteBuffer.remaining());
            this.byteBuffer.putInt(queueSelected);
            this.totalPointer.getAndAdd(total);
        } catch (UnsupportedEncodingException e) {
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