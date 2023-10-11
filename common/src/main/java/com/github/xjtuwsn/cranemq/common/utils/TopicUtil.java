package com.github.xjtuwsn.cranemq.common.utils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * @project:cranemq
 * @file:TopicUtil
 * @author:wsn
 * @create:2023/09/28-21:14
 */
public class TopicUtil {

    public static boolean checkTopic(String topic) {
        if (topic == null || topic.length() == 0) {
            return false;
        }
        if (topic.length() > 255) {
            return false;
        }
        Set<Character> validChar = new HashSet<>(Arrays.asList('-', '_'));
        char[] topicChar = topic.toCharArray();
        boolean hasLetter = false;
        for (char c : topicChar) {
            if (c >= 'a' && c <= 'z' || c >= 'A' || c <= 'Z') {
                hasLetter = true;
                continue;
            } else if (c >= '0' && c <= '9') {
                continue;
            } else {
                if (!validChar.contains(c)) {
                    return false;
                }
            }
        }
        if (!hasLetter) {
            return false;
        }
        return true;
    }

    public static String generateUniqueID() {
        String id = UUID.randomUUID().toString().replaceAll("-", "").substring(0, 16);
        return id;
    }

    public static String buildClientID(String role) {
        String ip = NetworkUtil.getLocalAddress();
        StringBuilder id = new StringBuilder();
        id.append(ip).append("@").append("default").append("@").append(role);

        return id.toString();
    }

    public static String buildStoreID(String clientId) {
        int index = clientId.indexOf("@");
        String temp = "local" + clientId.substring(index);
        return temp.replaceAll("@", "_");
    }
}
