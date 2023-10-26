package com.github.xjtuwsn.cranemq.test.simpletest;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.xjtuwsn.cranemq.common.command.RemoteCommand;
import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;
import com.github.xjtuwsn.cranemq.common.remote.codec.NettyDecoder;
import com.github.xjtuwsn.cranemq.common.remote.codec.NettyEncoder;
import com.github.xjtuwsn.cranemq.common.remote.serialize.impl.Hessian1Serializer;
import com.github.xjtuwsn.cranemq.common.utils.JSONUtil;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.checkerframework.checker.index.qual.NonNegative;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @project:cranemq
 * @file:Test
 * @author:wsn
 * @create:2023/10/06-10:42
 */
public class TestSimple {

    AtomicInteger elem = new AtomicInteger(0);
    @Test
    public void test2() {
        ExecutorService service = new ThreadPoolExecutor(1, 1, 60L,
                TimeUnit.SECONDS, new LinkedBlockingDeque<>(20),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "name1");
                    }
                }, new ThreadPoolExecutor.AbortPolicy());

        service.execute(() -> {
            send(service);
        });
        while (true) {}
    }
    public void send(ExecutorService service) {
        System.out.println(elem.incrementAndGet());
        service.execute(() -> {
            send(service);
        });
    }
    @Test
    public void test3() {
        List<Integer> list1 = Arrays.asList(1, 2, 3);
        List<Integer> list2 = Arrays.asList(4, 5, 6);
        List<Integer> list3 = Arrays.asList(10, 11, 12);
        List<List<Integer>> list = Arrays.asList(list1, list2, list3);
        List<Integer> reduce = list.stream().reduce(new ArrayList<>(), (a, b) -> {
            a.addAll(b);
            return a;
        });
        System.out.println(reduce);
    }
    @Test
    public void test4() {
        List<Integer> list = Arrays.asList(1, 2, 3, 4);
        Set<String> abc = Set.of("www");
        System.out.println(allocate(list, abc, "www"));
    }
    public List<Integer> allocate(List<Integer> queues, Set<String> groupMember, String self) {
        List<String> memebers = new ArrayList<>(groupMember);
        memebers.sort(String::compareTo);
        List<Integer> result = new ArrayList<>();
        int queueSize = queues.size(), all = memebers.size();
        int div = queueSize / all, left = queueSize % all, done = 0;
        for (int i = 0; i < all; i++) {
            String cur = memebers.get(i);
            if (cur.equals(self)) {
                int limit = done + div;
                if (i < left) {
                    limit++;
                }
                for (int j = done; j < limit; j++) {
                    result.add(queues.get(j));
                }
            }
            done += (i < left ? div + 1 : div);
            System.out.println(done + ", " + i + ", " + left + ", " + div);
        }
        return result;
    }
    @Test
    public void test5() {
        ConcurrentHashMap<Integer, Object> locks = new ConcurrentHashMap<>();
        locks.put(1, new Object());
        ExecutorService service = Executors.newFixedThreadPool(2);
        for (int i = 0; i < 2; i++) {
            service.execute(() -> {
                Object lock = locks.get(1);
                synchronized (lock) {
                    System.out.println(Thread.currentThread().getName());
                    try {
                        Thread.sleep(10 * 1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }
        while (true) {

        }
    }
    @Test
    public void test6() {
        Cache<Integer, Integer> cache = Caffeine.newBuilder()
                .expireAfter(new Expiry<Integer, Integer>() {
                    @Override
                    public long expireAfterCreate(Integer integer, Integer integer2, long l) {
                        return TimeUnit.SECONDS.toNanos(7);
                    }

                    @Override
                    public long expireAfterUpdate(Integer integer, Integer integer2, long l, @NonNegative long l1) {
                        return TimeUnit.SECONDS.toNanos(5) + l1;
                    }

                    @Override
                    public long expireAfterRead(Integer integer, Integer integer2, long l, @NonNegative long l1) {
                        return l1;
                    }
                }).build();
        cache.put(1, 1);
        for (int i = 0; i < 20; i++) {
            System.out.println("第" + i + "s");
            System.out.println(cache.getIfPresent(1));
            if (i == 2) {
                cache.put(1, 1);

            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

    }
    @Test
    public void test7() {
        Bootstrap bootstrap1 = new Bootstrap();
        ChannelFuture cf = null;
        try {
            cf = bootstrap1.group(new NioEventLoopGroup())
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .handler(new ChannelInitializer<Channel>() {
                        @Override
                        protected void initChannel(Channel ch) throws Exception {
                            ch.pipeline()
                                    .addLast(new NettyEncoder(RemoteCommand.class, new Hessian1Serializer()))
                                    .addLast(new NettyDecoder(RemoteCommand.class, new Hessian1Serializer()));
                        }
                    })
                    .connect("127.0.0.1", 11111).sync();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        System.out.println(cf);
    }
    @Test
    public void test8() {
        DelayQueue<TestDelay> delayQueue = new DelayQueue<>();
        delayQueue.put(new TestDelay(2000, "hhh"));
        try {
            long start = System.currentTimeMillis();
            TestDelay take = delayQueue.take();
            long end = System.currentTimeMillis();
            System.out.println(end - start);
            System.out.println(take);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    class TestDelay implements Delayed {
        long delay;
        String name;

        public TestDelay(long delay, String name) {
            this.delay = delay + System.currentTimeMillis();
            this.name = name;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(delay - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed o) {
            return (int)(this.delay - ((TestDelay) o).getDelay());
        }

        public long getDelay() {
            return delay;
        }
    }
    @Test
    public void test9() {
        String entry = "0123456789:0123456789";
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        int number = 3000000;
        for (int i = 0; i < number; i++) {
            sb.append(entry).append(",");

        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append("}");
        double size = entry.length() * number / 1024.0 / 1024;
        System.out.println(size + "MB");
        long start = System.currentTimeMillis();
        JSONUtil.JSONStrToFile(sb.toString(), "D:\\code\\opensource\\cranemq\\test\\simple-test\\src\\main\\resources\\test.json");
        long end = System.currentTimeMillis();
        System.out.println((end - start) + " ms");
    }
    // size + type + topic_size + topic + group_size + group +   offset + expira
    // 4    +  4   +     4      + [1,128]+   4       + [1,128]+    8    +   8  =  [34, 288]B ~ avg 100B
    // 1    ----- 100B
    // 10   ----- 1KB
    // 1w   ----- 1MB
    // 100W ----- 100MB

    @Test
    public void test11() {
        MessageQueue messageQueue1 = new MessageQueue("asas1", "topic1", 2);
        MessageQueue messageQueue2 = new MessageQueue("asas", "topic1", 3);
        MessageQueue messageQueue3 = new MessageQueue("asas", "topic1", 4);
        List<MessageQueue> list = Arrays.asList(messageQueue1, messageQueue2, messageQueue3);
        Collections.sort(list);
        System.out.println(list);

    }
    @Test
    public void test12() {
        System.out.println(System.getProperty("user.home"));
    }
}
