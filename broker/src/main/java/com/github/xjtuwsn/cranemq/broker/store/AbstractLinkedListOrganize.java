package com.github.xjtuwsn.cranemq.broker.store;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @project:cranemq
 * @file:AbstractMessageStore
 * @author:wsn
 * @create:2023/10/05-10:18
 */
public abstract class AbstractLinkedListOrganize {

    protected ConcurrentHashMap<Integer, MappedFile> mappedTable = new ConcurrentHashMap<>();

    protected ReentrantLock tailLock = new ReentrantLock();
    protected MappedFile head, tail;
    protected int headIndex = -1;
    protected int tailIndex = -2;

    protected void init() {
        this.head = new MappedFile(this.headIndex);
        this.tail = new MappedFile(this.tailIndex);
        this.head.next = this.tail;
        this.tail.prev = this.head;
        this.mappedTable.put(this.headIndex, this.head);
        this.mappedTable.put(this.tailIndex, this.tail);
    }

    protected void insertBeforeTail(MappedFile mappedFile) {
        this.tailLock.lock();
        this.mappedTable.put(mappedFile.getIndex(), mappedFile);
        this.tail.prev.next = mappedFile;
        mappedFile.prev = this.tail.prev;
        mappedFile.next = this.tail;
        this.tail.prev = mappedFile;
        this.tailLock.unlock();
    }
    protected int nextIndex() {
        return this.mappedTable.size() - 2;
    }
}
