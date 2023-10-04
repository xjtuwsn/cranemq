package com.github.xjtuwsn.cranemq.broker.store.queue;

import com.github.xjtuwsn.cranemq.broker.store.PersistentConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * @project:cranemq
 * @file:ConsumerQueueInner
 * @author:wsn
 * @create:2023/10/04-21:55
 */
public class ConsumerQueueInner {

    private static final Logger log = LoggerFactory.getLogger(ConsumerQueueInner.class);
    private int index;

    private String fileName;
    private String fullPath;

    private File file;
    private int fileSize;
    public ConsumerQueueInner prev, next;
    private PersistentConfig persistentConfig;

    public ConsumerQueueInner(int index) {
        this.index = index;
    }
    public ConsumerQueueInner(int index, String fileName, String fullPath, PersistentConfig persistentConfig) {
        this.index = index;
        this.fileName = fileName;
        this.persistentConfig = persistentConfig;
        this.fullPath = fullPath;
        this.file = new File(this.fullPath);
        try {
            if (!file.exists()) {
                file.createNewFile();
            }
        } catch (IOException e) {
            log.error("Create file {} error", this.fileName);
        }

    }
}
