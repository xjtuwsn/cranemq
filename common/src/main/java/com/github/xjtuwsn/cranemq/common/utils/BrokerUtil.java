package com.github.xjtuwsn.cranemq.common.utils;

import com.github.xjtuwsn.cranemq.common.config.FlushDisk;
import com.github.xjtuwsn.cranemq.common.constant.MQConstant;
import com.github.xjtuwsn.cranemq.common.remote.enums.RegistryType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Properties;
import java.util.UUID;

/**
 * @project:cranemq
 * @file:BrokerUtils
 * @author:wsn
 * @create:2023/10/02-11:09
 */
public class BrokerUtil {
    private static final Logger log = LoggerFactory.getLogger(BrokerUtil.class);
    public static void parseConfigFile(Properties properties, Object object) {
        Method[] methods = object.getClass().getMethods();
        for (Method method : methods) {
            String name = method.getName();
            if (name.startsWith("set")) {
                String filedName = name.substring(3, 4).toLowerCase() + name.substring(4);
                String value = properties.getProperty(filedName);
                if (value == null) {
                    continue;
                }
                Class<?> parameterType = method.getParameterTypes()[0];
                if (parameterType != null) {
                    String parameterTypeName = parameterType.getSimpleName();
                    Object v = null;
                    if ("int".equals(parameterTypeName) || "Int".equals(parameterTypeName)) {
                        v = Integer.parseInt(value);
                    }
                    if ("String".equals(parameterTypeName) || "string".equals(parameterTypeName)) {
                        v = String.valueOf(value);
                    }
                    if ("FlushDisk".equals(parameterTypeName)) {
                        v = FlushDisk.valueOf(value.toUpperCase());
                    }
                    if ("Double".equals(parameterTypeName) || "double".equals(parameterTypeName)) {
                        v = Double.valueOf(value);
                    }
                    if ("Long".equals(parameterTypeName) || "long".equals(parameterTypeName)) {
                        v = Long.valueOf(value);
                    }
                    if ("Boolean".equals(parameterTypeName) || "boolean".equals(parameterTypeName)) {
                        v = Boolean.valueOf(value);
                    }
                    if ("RegistryType".equals(parameterTypeName)) {
                        if ("zk".equals(value)) {
                            v = RegistryType.ZOOKEEPER;
                        } else if ("nacos".equals(value)) {
                            v = RegistryType.NACOS;
                        } else {
                            v = RegistryType.DEFAULT;
                        }
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

    public static String makeFileName(int index, int fileSize) {
        long offset = index * (long) fileSize;
        StringBuilder sb = new StringBuilder(String.valueOf(offset));
        int left = MQConstant.COMMITLOG_FILENAME_LENGTH - sb.length();
        StringBuilder padding = new StringBuilder();
        for (int i = 0; i < left; i++) {
            padding.append("0");
        }
        return padding.append(sb).toString();
    }

    public static String getQueuePath(String prefix, String topic, int queueId, String fileName) {
        StringBuilder sb = new StringBuilder();
        sb.append(prefix).append(topic).append("\\").append(queueId).append("\\").append(fileName);
        return sb.toString();
    }

    public static long calOffset(String filename, int pos) {
        long begin = Long.valueOf(filename);
        return begin + pos;
    }

    public static int findMappedIndex(long offset, String firstFileName, int fileSize) {
        long begin = Long.parseLong(firstFileName);
        int index = (int) ((offset - begin) / fileSize);
        return index;

    }

    public static int offsetInPage(long offset, int fileSize) {

        return (int) (offset % fileSize);
    }

    public static String offsetKey(String topic, String group) {
        return topic + "@" + group;
    }

    public static String holdRequestKey(String topic, String group, String clientId) {
        return topic + "@" + group + "@" + clientId;
    }
    public static String logId() {
        return UUID.randomUUID().toString().replaceAll("-", "").substring(0, 16);
    }
}
