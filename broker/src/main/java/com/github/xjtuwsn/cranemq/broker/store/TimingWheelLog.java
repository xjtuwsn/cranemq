package com.github.xjtuwsn.cranemq.broker.store;

import com.github.xjtuwsn.cranemq.broker.BrokerController;
import com.github.xjtuwsn.cranemq.broker.timer.DelayMessageTask;
import com.github.xjtuwsn.cranemq.common.command.types.Type;
import com.github.xjtuwsn.cranemq.common.constant.MQConstant;
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
import java.nio.charset.CharacterCodingException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @project:cranemq
 * @file:TimingWheelLog
 * @author:wsn
 * @create:2023/10/20-10:45
 */
public class TimingWheelLog {

    private static final Logger log = LoggerFactory.getLogger(TimingWheelLog.class);
    private static final int FILE_NUMBER = 2;

    private static final String LOGFILE_0 = "timingwheel0";
    private static final String LOGFILE_1 = "timingwheel1";

    private static final String CREATELOG = "createtime";
    private static final int EXCHANGE_GAP = 8 * 60 * 60; // s
    private static final int COMMON_LOG = 0;
    private static final int FINISH_LOG = 1;
    public int fileSize;
    public String[] fileName = new String[]{LOGFILE_0, LOGFILE_1};

    private String[] filePath;
    private File[] files;
    private long createTime;
    private int lastIndex = -1;

    private FileChannel[] fileChannels;
    private MappedByteBuffer[] mappedByteBuffers;
    private BrokerController brokerController;
    private AtomicInteger writePointer;
    private AtomicInteger flushPointer;

    private ScheduledExecutorService asyncFlushService;

    private ExecutorService recoveryService;

    public TimingWheelLog(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.filePath = new String[FILE_NUMBER];
        this.files = new File[FILE_NUMBER];
        this.fileChannels = new FileChannel[FILE_NUMBER];
        this.mappedByteBuffers = new MappedByteBuffer[FILE_NUMBER];
        this.fileSize = brokerController.getPersistentConfig().getDelayMessageLogSize();
        this.writePointer = new AtomicInteger(0);
        this.flushPointer = new AtomicInteger(0);
        this.asyncFlushService = new ScheduledThreadPoolExecutor(1);
        this.recoveryService = new ThreadPoolExecutor(6, 12, 60L,
                TimeUnit.SECONDS, new LinkedBlockingDeque<>(2000),
                new ThreadFactory() {
                    AtomicInteger index = new AtomicInteger(0);
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "RecoveryService NO." + index.get());
                    }
                });
    }

    public void start() {
        for (int i = 0; i < FILE_NUMBER; i++) {
           loadFile(i);
        }
        File createTimeFile = new File(brokerController.getPersistentConfig().getDelayLogPath() + CREATELOG);
        try {
            if (!createTimeFile.exists()) {

                createTimeFile.createNewFile();
                FileChannel fileChannel = new RandomAccessFile(createTimeFile, "rw").getChannel();
                ByteBuffer buffer = ByteBuffer.allocate(8);
                createTime = System.currentTimeMillis();
                buffer.putLong(createTime);
                buffer.position(0);
                fileChannel.write(buffer);
                fileChannel.force(false);
                fileChannel.close();
            } else {
                FileChannel fileChannel = new RandomAccessFile(createTimeFile, "rw").getChannel();
                ByteBuffer buffer = ByteBuffer.allocate(8);
                fileChannel.read(buffer);
                buffer.position(0);
                createTime = buffer.getLong();
                fileChannel.close();
            }
        } catch (IOException e) {
            log.error("Read create time log file error");
        }
        log.info("TimingWheelLog start successfully");
        this.asyncFlushService.scheduleAtFixedRate(() -> {
            flush();
        }, 100, 5 * 1000, TimeUnit.MILLISECONDS);
        recovery();
    }

    public synchronized String appendLog(String topic, long commitOffset, long delayQueueOffset, int queueId, long delay) {
        ByteBuffer buffer = this.mappedByteBuffers[index()].slice();

        try {
            String id = BrokerUtil.logId();
            byte[] idData = id.getBytes(MQConstant.CHARSETNAME);
            int idSize = idData.length;
            byte[] topicData = topic.getBytes(MQConstant.CHARSETNAME);
            int topicSize = topicData.length;
            long expira = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()) + delay;

            int type = COMMON_LOG;

            int total = 4 + 4 + 4 + idSize + 4 + topicSize + 8 + 8 + 4 + 8;
            //       total+type+idsize+id+topicsize+topic+cooff+quoff+queue+delay
            int pos = writePointer.get();
            buffer.position(pos);

            buffer.putInt(total);

            buffer.putInt(type);

            buffer.putInt(idSize);
            buffer.put(idData);

            buffer.putInt(topicSize);
            buffer.put(topicData);

            buffer.putLong(commitOffset);
            buffer.putLong(delayQueueOffset);
            buffer.putInt(queueId);

            buffer.putLong(expira);

            writePointer.getAndAdd(total);
            return id;

        } catch (UnsupportedEncodingException e) {
            log.error("UnsupportedEncodingException in append log");
        }
        return null;
    }

    public synchronized void finishLog(String id) {
        ByteBuffer buffer = this.mappedByteBuffers[index()].slice();
        try {
            byte[] idData = id.getBytes(MQConstant.CHARSETNAME);
            int idSize = idData.length;
            int type = FINISH_LOG;

            int total = 4 + 4 + 4 + idSize;
            int pos = writePointer.get();

            buffer.position(pos);

            buffer.putInt(total);

            buffer.putInt(type);

            buffer.putInt(idSize);
            buffer.put(idData);

            writePointer.getAndAdd(total);


        } catch (UnsupportedEncodingException e) {
            log.error("UnsupportedEncodingException in finish log");
        }
    }
    private void flush() {
        flush(index());
    }
    private void flush(int index) {
        int cur = writePointer.get();
        int last = flushPointer.get();
        if (cur == last) {
            return;
        }

       mappedByteBuffers[index].force();
       flushPointer.set(cur);
    }
    //  total+type+idsize+id+topicsize+topic+cooff+quoff+queue+delay
    //  total+type+idsize+id
    private void recovery() {
        Map<String, DelayInfo> infoMap = new HashMap<>();
        Set<String> commitIds = new HashSet<>();

        int choosed = index();
        try {
            for (int i = 0; i < FILE_NUMBER; i++) {
                int pos = 0;
                ByteBuffer buffer = mappedByteBuffers[i].slice();
                while (true) {
                    buffer.position(pos);
                    int size = buffer.getInt();
                    pos += size;
                    if (size == 0) {
                        if (choosed == i) {
                            this.writePointer.set(pos);
                            this.flushPointer.set(pos);
                        }
                        break;
                    }
                    int type = buffer.getInt();

                    int idSize = buffer.getInt();
                    byte[] idData = new byte[idSize];
                    buffer.get(idData);
                    String id = new String(idData, MQConstant.CHARSETNAME);

                    if (type == FINISH_LOG) {
                        commitIds.add(id);
                        continue;
                    }

                    int topicSize = buffer.getInt();
                    byte[] topicData = new byte[topicSize];
                    buffer.get(topicData);
                    String topic = new String(topicData, MQConstant.CHARSETNAME);

                    long commitOffset = buffer.getLong();
                    long queueOffset = buffer.getLong();

                    int queueId = buffer.getInt();

                    long expiration = buffer.getLong();



                    DelayInfo delayInfo = new DelayInfo(topic, id, commitOffset, queueOffset, queueId, expiration, i);
                    infoMap.put(id, delayInfo);
                }
            }
        } catch (UnsupportedEncodingException e) {
            log.error("UnsupportedEncodingException in recovery");
        }
        for (String id : commitIds) {
            infoMap.remove(id);
        }
        boolean[] mask = new boolean[FILE_NUMBER];
        for (Map.Entry<String, DelayInfo> entry : infoMap.entrySet()) {
            DelayInfo info = entry.getValue();
            long second = info.remain();
            mask[info.index] = true;
            if (second <= 0) {
                // 已过期且未提交，立即执行
                this.recoveryService.execute(new DelayMessageTask(brokerController, info.topic, info.commitLogOffset,
                        info.delayQueueOffset, info.queueId, info.id));
                log.info("This task has expiread, will execute now");
            } else {
                // 未提交，未过期，调整延时时间重新执行，且标记为不能删除，最后没有标记的文件清空

                log.info("Resubmit delay task, remain {} second to excute", second);
                this.brokerController.getMessageStoreCenter().onCommitDelayMessage(info.commitLogOffset, info.delayQueueOffset,
                        info.topic, info.queueId, second, info.id);

            }
        }
        // 清空没有待恢复任务的文件
        for (int i = 0; i < FILE_NUMBER; i++) {
            if (!mask[i]) {
                try {
                    log.info("Time wheel log file {} should be cleared", i);
                    clear(i);
                } catch (IOException e) {
                    log.error("Clear time wheel log file {} error", i);
                }
            }
        }
    }

    private void loadFile(int i) {
        filePath[i] = brokerController.getPersistentConfig().getDelayLogPath() + fileName[i];
        files[i] = new File(filePath[i]);
        try {
            if (!files[i].exists()) {

                files[i].createNewFile();
            }
            fileChannels[i] = new RandomAccessFile(files[i], "rw").getChannel();
            mappedByteBuffers[i] = fileChannels[i].map(FileChannel.MapMode.READ_WRITE, 0, this.fileSize);
        } catch (IOException e) {
            e.printStackTrace();
            log.error("Create new timing whell log file error");
        }
    }
    private int index() {
        long cur = System.currentTimeMillis();
        long gap = cur - createTime;
        int index = (int) ((TimeUnit.MILLISECONDS.toSeconds(gap) / EXCHANGE_GAP) % 2);
        if (index != lastIndex) {
            doSwap(index);
        }
        return index;

    }

    private synchronized void doSwap(int index) {
        if (lastIndex == -1) {
            lastIndex = index;
        } else {
            if (index == lastIndex) {
                return;
            }
            flush(lastIndex);
            lastIndex = index;
            try {
                clear(index);
            } catch (IOException e) {
                log.error("Clear last file error");
            }
            writePointer.set(0);
            flushPointer.set(0);
        }
    }
    private void clear(int index) throws IOException {
        fileChannels[index].close();
        files[index].delete();
        loadFile(index);
    }
    public void close() {
        flush();
        for (int i = 0; i < FILE_NUMBER; i++) {
            if (fileChannels[i] != null) {
                try {
                    fileChannels[i].close();
                } catch (IOException e) {
                    log.error("Close channel error");
                }
            }
        }
    }
    static class DelayInfo {
        private String topic;
        private String id;
        private long commitLogOffset;
        private long delayQueueOffset;
        private int queueId;
        private long expiration;
        private int index;

        public DelayInfo(String topic, String id, long commitLogOffset, long delayQueueOffset, int queueId,
                         long expiration, int index) {
            this.topic = topic;
            this.id = id;
            this.commitLogOffset = commitLogOffset;
            this.delayQueueOffset = delayQueueOffset;
            this.queueId = queueId;
            this.expiration = expiration;
        }

        public long remain() {
            return this.expiration - TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
        }

        @Override
        public String toString() {
            return "DelayInfo{" +
                    "topic='" + topic + '\'' +
                    ", id='" + id + '\'' +
                    ", commitLogOffset=" + commitLogOffset +
                    ", delayQueueOffset=" + delayQueueOffset +
                    ", queueId=" + queueId +
                    ", expiration=" + expiration +
                    ", index=" + index +
                    '}';
        }
    }
}
