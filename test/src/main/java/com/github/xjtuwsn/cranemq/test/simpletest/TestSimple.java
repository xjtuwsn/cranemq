package com.github.xjtuwsn.cranemq.test.simpletest;

import com.github.xjtuwsn.cranemq.broker.store.PersistentConfig;
import org.junit.Test;

import java.lang.reflect.Method;

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
}
