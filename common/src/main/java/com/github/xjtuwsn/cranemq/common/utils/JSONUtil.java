package com.github.xjtuwsn.cranemq.common.utils;

import com.alibaba.fastjson.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * @project:cranemq
 * @file:JSONUtil
 * @author:wsn
 * @create:2023/10/09-17:11
 */
public class JSONUtil {
    private static final Logger log = LoggerFactory.getLogger(JSONUtil.class);

    public static String fileToString(String path) {
        File file = new File(path);
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                log.error("Create file {} error", path);
            }
        }
        String content = "";
        try (FileInputStream fis = new FileInputStream(file)) {
            byte[] data = new byte[(int) file.length()];
            fis.read(data);
            return new String(data, "UTF-8");
        } catch (IOException e) {
            log.error("Get file input stream error");
        }
        return content;
    }

    public static <T> T JSONStrToObject(String content, Class<T> clazz) {
        return JSON.parseObject(content, clazz);
    }

    public static <T> T loadJSON(String path, Class<T> clazz) {
        String content = fileToString(path);
        return JSONStrToObject(content, clazz);
    }

    public static String objectToJSONStr(Object o) {
        return JSON.toJSONString(o, true);
    }

    public static synchronized void JSONStrToFile(Object o, String path) {
        File file = new File(path);
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                log.error("Create file error");
            }
        }
        String str = objectToJSONStr(o);
        try (OutputStream ops = new FileOutputStream(file)) {
            ops.write(str.getBytes("UTF-8"));
        } catch (IOException e) {
            log.error("Create output stream error");
        }
    }
}
