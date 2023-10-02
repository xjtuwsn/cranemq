package com.github.xjtuwsn.cranemq.common.utils;

import com.github.xjtuwsn.cranemq.common.config.FlushDisk;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Properties;

/**
 * @project:cranemq
 * @file:BrokerUtils
 * @author:wsn
 * @create:2023/10/02-11:09
 */
public class BrokerUtil {
    private static final Logger log = LoggerFactory.getLogger(BrokerUtil.class);
    public static void prarseConfigFile(Properties properties, Object object) {
        Method[] methods = object.getClass().getMethods();
        for (Method method : methods) {
            String name = method.getName();
            if (name.startsWith("set")) {
                String filedName = name.substring(3, 4).toLowerCase() + name.substring(4);
                String value = properties.getProperty(filedName);
                Class<?> parameterType = method.getParameterTypes()[0];
                if (parameterType != null) {
                    String parameterTypeName = parameterType.getSimpleName();
                    Object v = null;
                    if (parameterTypeName.equals("int") || parameterTypeName.equals("Int")) {
                        v = Integer.parseInt(value);
                    }
                    if (parameterTypeName.equals("String") || parameterTypeName.equals("string")) {
                        v = String.valueOf(value);
                    }
                    if (parameterTypeName.equals("FlushDisk")) {
                        v = FlushDisk.valueOf(value.toUpperCase());
                    }
                    if (parameterTypeName.equals("Double") || parameterTypeName.equals("double")) {
                        v = Double.valueOf(value);
                    }
                    try {
                        method.invoke(object, v);
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        log.error("Failed to parse properties to {} object", object);
                    }
                }
            }
        }
    }
}
