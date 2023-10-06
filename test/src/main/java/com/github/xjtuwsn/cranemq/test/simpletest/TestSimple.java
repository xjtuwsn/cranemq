package com.github.xjtuwsn.cranemq.test.simpletest;

import com.github.xjtuwsn.cranemq.broker.store.PersistentConfig;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @project:cranemq
 * @file:Test
 * @author:wsn
 * @create:2023/10/06-10:42
 */
public class TestSimple {
    @Test
    public void test1() {
        PersistentConfig persistentConfig = new PersistentConfig();
        Method[] methods = persistentConfig.getClass().getMethods();
        for (Method method : methods) {
            Class<?>[] parameterTypes = method.getParameterTypes();
            System.out.println("-----------");
            for (Class<?> type : parameterTypes) {
                String simpleName = type.getSimpleName();
                System.out.println(simpleName);
            }

            System.out.println("-------------");
        }
    }
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
}
