package com.github.xjtuwsn.cranemq.broker.timer;

import com.github.xjtuwsn.cranemq.common.exception.CraneBrokerException;

import java.util.Iterator;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * @project:cranemq
 * @file:DelayTaskList
 * @author:wsn
 * @create:2023/10/19-14:19
 */
public class DelayTaskList<T extends Thread> implements Delayed {

    private DelayTaskWrapper<T> head, tail;

    private long expiration;

    public DelayTaskList(long delay) {
        this.head = new DelayTaskWrapper<>();
        this.tail = new DelayTaskWrapper<>();
        this.head.next = this.tail;
        this.tail.prev = this.head;
        this.expiration = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(delay);
    }

    public DelayTaskList() {

    }
    public synchronized void addTask(T task, long totalDelay) {
        DelayTaskWrapper<T> wrapper = new DelayTaskWrapper<>(task, totalDelay);
        this.tail.prev.next = wrapper;
        wrapper.prev = this.tail.prev;
        wrapper.next = this.tail;
        this.tail.prev = wrapper;
    }

    public void setExpiration(long delay) {
        this.expiration = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(delay);;
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return unit.convert(expiration - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
        return (int) (this.expiration - ((DelayTaskList) o).getExpiration());
    }

    public long getExpiration() {
        return expiration;
    }

    public boolean isEmpty() {
        return this.head.next == this.tail;
    }

    public DelayTaskIterator iterator() {
        return new DelayTaskIterator(head);
    }


    class DelayTaskIterator implements Iterator<DelayTaskWrapper<T>> {
        private DelayTaskWrapper<T> pointer;

        public DelayTaskIterator(DelayTaskWrapper<T> pointer) {
            this.pointer = pointer;
        }

        @Override
        public boolean hasNext() {
            return this.pointer.next != tail;
        }

        @Override
        public DelayTaskWrapper<T> next() {
            this.pointer = this.pointer.next;
            return this.pointer;
        }

        @Override
        public void remove() {
            if (pointer == head) {
                throw new CraneBrokerException("Head node cannot be deleted");
            }
            this.pointer.prev.next = this.pointer.next;
            this.pointer.next.prev = this.pointer.prev;
        }
    }
}
