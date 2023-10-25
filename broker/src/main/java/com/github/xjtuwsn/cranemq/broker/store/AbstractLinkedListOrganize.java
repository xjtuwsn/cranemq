package com.github.xjtuwsn.cranemq.broker.store;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @project:cranemq
 * @file:AbstractMessageStore
 * @author:wsn
 * @create:2023/10/05-10:18
 */

/**
 * 表示commitLog和消费队列对mappedFile 的组织形式
 * 以链表的形式组织
 * @author wsn
 */
public abstract class AbstractLinkedListOrganize {

    // index：mappedFile
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

    /**
     * 在尾部插入新的文件
     * @param mappedFile
     */
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
    public MappedIterator iterator() {
        return new MappedIterator(head);
    }
    public MappedFile getFirstMappedFile() {
        MappedFile mappedFile = head.next;
        if (mappedFile == tail) {
            return null;
        }
        return mappedFile;
    }
    public MappedFile getMappedFileByIndex(int index) {
        return this.mappedTable.get(index);
    }

    class MappedIterator implements Iterator<MappedFile> {
        private MappedFile pointer;

        public MappedIterator(MappedFile pointer) {
            this.pointer = pointer;
        }

        @Override
        public boolean hasNext() {
            return pointer.next != tail;
        }

        @Override
        public MappedFile next() {
            pointer = pointer.next;
            return pointer;
        }
    }
}
