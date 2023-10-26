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

/**
 * 持久化延迟消息日志，使得服务器宕机重启后能够继续延时任务
 */
public class TimingWheelLog {

    private static final Logger log = LoggerFactory.getLogger(TimingWheelLog.class);
    // 持久化日志数量
    private static final int FILE_NUMBER = 2;
    // 文件名
    private static final String LOGFILE_0 = "timingwheel0";
    private static final String LOGFILE_1 = "timingwheel1";
    // 记录整体创建时间的文件，用于实现日志文件切换
    private static final String CREATELOG = "createtime";
    // 每8小时交换一次
    private static final int EXCHANGE_GAP = 8 * 60 * 60; // s
    // 日志记录种类
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
    // 写指针
    private AtomicInteger writePointer;
    // 读指针
    private AtomicInteger flushPointer;

    // 定期刷盘
    private ScheduledExecutorService asyncFlushService;
    // 恢复时立即执行使用的线程池
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
        // 加载所有日志文件
        for (int i = 0; i < FILE_NUMBER; i++) {
           loadFile(i);
        }
        File createTimeFile = new File(brokerController.getPersistentConfig().getDelayLogPath() + CREATELOG);
        try {
            // 如果记录时间的文件不存在，就新建
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
                // 存在的化就读取记录的时间
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
        // 定期刷盘
        this.asyncFlushService.scheduleAtFixedRate(() -> {
            flush();
        }, 100, 5 * 1000, TimeUnit.MILLISECONDS);

        // 恢复延迟任务
        recovery();
    }

    /**
     * 向延迟任务日志写入任务信息
     * @param topic 当前执行延迟消息的topic，也就是delay之后投递的topic
     * @param commitOffset 该消息在commitLog中的offset
     * @param delayQueueOffset 在延迟队列中的offset
     * @param queueId 将要投递的queueId
     * @param delay 延迟时间
     * @return 返回唯一标识id
     */
    public synchronized String appendLog(String topic, long commitOffset, long delayQueueOffset, int queueId, long delay) {

        //       total+type+idsize+id+topicsize+topic+cooff+quoff+queue+delay
        // 写入的buffer
        ByteBuffer buffer = this.mappedByteBuffers[index()].slice();

        try {
            // 本次日志id
            String id = BrokerUtil.logId();
            byte[] idData = id.getBytes(MQConstant.CHARSETNAME);
            int idSize = idData.length;
            byte[] topicData = topic.getBytes(MQConstant.CHARSETNAME);
            int topicSize = topicData.length;
            long expira = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()) + delay;

            // 表明日志类型为记录任务
            int type = COMMON_LOG;

            // 总长度
            int total = 4 + 4 + 4 + idSize + 4 + topicSize + 8 + 8 + 4 + 8;

            // 当前位置
            int pos = writePointer.get();
            buffer.position(pos);

            // 写入总长度
            buffer.putInt(total);

            // 写入类型
            buffer.putInt(type);

            // 写入id长度与id数据
            buffer.putInt(idSize);
            buffer.put(idData);

            // 写入topic长度与topic数据
            buffer.putInt(topicSize);
            buffer.put(topicData);

            // 写入commitLog偏移
            buffer.putLong(commitOffset);
            // 写入延迟队列偏移
            buffer.putLong(delayQueueOffset);
            // 写入将写入队列id
            buffer.putInt(queueId);

            // 写入最后总过期时间
            buffer.putLong(expira);

            // 更新写指针
            writePointer.getAndAdd(total);
            return id;

        } catch (UnsupportedEncodingException e) {
            log.error("UnsupportedEncodingException in append log");
        }
        return null;
    }

    /**
     * 写入提交日志
     * @param id 将要提交的日志id
     */
    public synchronized void finishLog(String id) {
        ByteBuffer buffer = this.mappedByteBuffers[index()].slice();
        try {
            byte[] idData = id.getBytes(MQConstant.CHARSETNAME);
            int idSize = idData.length;
            int type = FINISH_LOG;

            int total = 4 + 4 + 4 + idSize;
            int pos = writePointer.get();

            buffer.position(pos);

            // 写入总长度
            buffer.putInt(total);

            // 写入type
            buffer.putInt(type);

            // 写入id长度和数据
            buffer.putInt(idSize);
            buffer.put(idData);

            writePointer.getAndAdd(total);


        } catch (UnsupportedEncodingException e) {
            log.error("UnsupportedEncodingException in finish log");
        }
    }

    /**
     * 刷盘
     */
    private void flush() {
        flush(index());
    }

    /**
     * 刷新指定文件
     * @param index 文件索引
     */
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

    /**
     * broker重启时从日志中恢复未完成的延迟任务
     */
    private void recovery() {
        // 任务日志列表 id: info
        Map<String, DelayInfo> infoMap = new HashMap<>();
        // 已提交日志ids
        Set<String> commitIds = new HashSet<>();

        // 当前选择的日志索引
        int choosed = index();
        try {
            // 遍历日志进行读取
            for (int i = 0; i < FILE_NUMBER; i++) {
                int pos = 0;
                ByteBuffer buffer = mappedByteBuffers[i].slice();
                while (true) {
                    buffer.position(pos);
                    // 先读总长度
                    int size = buffer.getInt();
                    pos += size;
                    // 已读完
                    if (size == 0 || size >= 300) {
                        if (choosed == i) {
                            this.writePointer.set(pos);
                            this.flushPointer.set(pos);
                        }
                        break;
                    }
                    // 读取其他信息
                    int type = buffer.getInt();
                    int idSize = buffer.getInt();
                    byte[] idData = new byte[idSize];
                    buffer.get(idData);
                    String id = new String(idData, MQConstant.CHARSETNAME);

                    // 如果是提交日志就不继续
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

                    // 封装delay信息
                    DelayInfo delayInfo = new DelayInfo(topic, id, commitOffset, queueOffset, queueId, expiration, i);
                    infoMap.put(id, delayInfo);
                }
            }
        } catch (UnsupportedEncodingException e) {
            log.error("UnsupportedEncodingException in recovery");
        }
        // 将所有已经commit的日志从map中删除
        for (String id : commitIds) {
            infoMap.remove(id);
        }
        // 记录哪个日志文件可以清空
        boolean[] mask = new boolean[FILE_NUMBER];
        for (Map.Entry<String, DelayInfo> entry : infoMap.entrySet()) {
            DelayInfo info = entry.getValue();
            // 该任务剩余时间
            long second = info.remain();
            // 标记为不能删除，最后没有标记的文件清空
            mask[info.index] = true;
            if (second <= 0) {
                // 已过期且未提交，立即执行
                this.recoveryService.execute(new DelayMessageTask(brokerController, info.topic, info.commitLogOffset,
                        info.delayQueueOffset, info.queueId, info.id));
                log.info("This task has expired, will execute now");
            } else {
                // 未提交，未过期，调整延时时间重新执行，且

                log.info("Resubmit delay task, remain {} second to execute", second);
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
                    if (i == choosed) {
                        this.writePointer.set(0);
                        this.flushPointer.set(0);
                    }
                } catch (IOException e) {
                    log.error("Clear time wheel log file {} error", i);
                }
            }
        }
    }

    /**
     * 加载指定文件
     * @param i 索引
     */
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

    /**
     * 计算当前该使用哪个文件，根据和初始时间的差来计算
     * @return 索引
     */
    private int index() {
        long cur = System.currentTimeMillis();
        long gap = cur - createTime;
        int index = (int) ((TimeUnit.MILLISECONDS.toSeconds(gap) / EXCHANGE_GAP) % 2);
        // 做文件切换
        if (index != lastIndex) {
            doSwap(index);
        }
        return index;

    }

    /**
     * 切换使用的文件
     * @param index 索引
     */
    private synchronized void doSwap(int index) {
        if (lastIndex == -1) {
            lastIndex = index;
        } else {
            if (index == lastIndex) {
                return;
            }
            // 先刷盘当前文件
            flush(lastIndex);
            lastIndex = index;
            try {
                // 清空要使用的文件
                clear(index);
            } catch (IOException e) {
                log.error("Clear last file error");
            }
            // 重置指针
            writePointer.set(0);
            flushPointer.set(0);
        }
    }

    /**
     * 清空，重新加载
     * @param index
     * @throws IOException
     */
    private void clear(int index) throws IOException {

        ByteBuffer buffer = ByteBuffer.allocate(1024);
        for (int i = 0; i < fileSize; i += 1024) {
            buffer.position(0);
            fileChannels[index].position(i);
            fileChannels[index].write(buffer);
        }
        fileChannels[index].force(false);
//        fileChannels[index].close();
//        files[index].delete();
//        loadFile(index);
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

    /**
     * 从日志中读取delay任务的包装
     */
    static class DelayInfo {
        private final String topic;
        private final String id;
        private final long commitLogOffset;
        private final long delayQueueOffset;
        private final int queueId;
        private final long expiration;
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
