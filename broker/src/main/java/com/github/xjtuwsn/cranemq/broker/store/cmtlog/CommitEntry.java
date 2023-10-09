package com.github.xjtuwsn.cranemq.broker.store.cmtlog;

/**
 * @project:cranemq
 * @file:CommitEntry
 * @author:wsn
 * @create:2023/10/09-19:47
 */
public class CommitEntry {
    private String topic;

    private int queueId;
    private long offset;
    private int offsetInPage;

    private int size;

    private String tag;

    public CommitEntry(String topic, String fileName, int queueId, int offset, int size, String tag) {
        this.topic = topic;
        this.queueId = queueId;
        this.offset = Long.valueOf(fileName) + offset;
        this.size = size;
        this.tag = tag;
        this.offsetInPage = offset;
    }

    public int getOffsetInPage() {
        return offsetInPage;
    }

    public String getTopic() {
        return topic;
    }

    public int getQueueId() {
        return queueId;
    }

    public long getOffset() {
        return offset;
    }

    public int getSize() {
        return size;
    }

    public String getTag() {
        return tag;
    }

    @Override
    public String toString() {
        return "CommitEntry{" +
                "topic='" + topic + '\'' +
                ", queueId=" + queueId +
                ", offset=" + offset +
                ", offsetInPage=" + offsetInPage +
                ", size=" + size +
                ", tag='" + tag + '\'' +
                '}';
    }
}
