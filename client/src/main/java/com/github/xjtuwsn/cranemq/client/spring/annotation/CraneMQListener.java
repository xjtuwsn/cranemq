package com.github.xjtuwsn.cranemq.client.spring.annotation;

import java.lang.annotation.*;

/**
 * @project:cranemq
 * @file:CraneCommonListener
 * @author:wsn
 * @create:2023/10/14-16:11
 * 注解的函数是一个消息消费者，指定普通或顺序消息
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface CraneMQListener {
    String id() default "0";

    String topic() default "";

    String tag() default "*";

    boolean ordered() default false;

    Class<?> dataType();

}
