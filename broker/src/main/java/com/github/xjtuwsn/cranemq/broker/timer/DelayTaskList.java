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

/**
 * 每一个表格中存放的所有任务的列表，在这之中任务以双向链表形式组织
 * @param <T>
 */
public class DelayTaskList<T extends Thread> implements Delayed {

    // 头节点和尾节点
    private DelayTaskWrapper<T> head, tail;

    // 这个list对应的过期时间
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

    /**
     * 添加任务
     * @param task 任务
     * @param totalDelay 该任务过期的时间
     */
    public synchronized void addTask(T task, long totalDelay) {
        // 在链表尾部进行插入
        DelayTaskWrapper<T> wrapper = new DelayTaskWrapper<>(task, totalDelay);
        this.tail.prev.next = wrapper;
        wrapper.prev = this.tail.prev;
        wrapper.next = this.tail;
        this.tail.prev = wrapper;
    }

    // 重置过期时间
    public void setExpiration(long delay) {
        this.expiration = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(delay);;
    }

    /**
     * 用于延时队列进行延时时间判断
     * @param unit the time unit
     * @return 距离到期时间的差值，如果小于0，延时队列会将其弹出
     */
    @Override
    public long getDelay(TimeUnit unit) {
        return unit.convert(expiration - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * 用于延时队列进行排序
     * @param o the object to be compared.
     * @return
     */
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

    // 获取迭代器
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
