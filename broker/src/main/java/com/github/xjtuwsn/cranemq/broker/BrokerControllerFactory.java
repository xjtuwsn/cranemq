package com.github.xjtuwsn.cranemq.broker;

import com.github.xjtuwsn.cranemq.broker.store.PersistentConfig;
import com.github.xjtuwsn.cranemq.common.config.BrokerConfig;
import com.github.xjtuwsn.cranemq.common.constant.MQConstant;
import com.github.xjtuwsn.cranemq.common.utils.BrokerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.Resource;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @project:cranemq
 * @file:BrokerControllerFactory
 * @author:wsn
 * @create:2023/10/23-18:56
 */

/**
 * Broker主启动类
 */
@Configuration
public class BrokerControllerFactory {
    private static final Logger log = LoggerFactory.getLogger(BrokerControllerFactory.class);
    // 命令行参数
    @Resource
    private ApplicationArguments arguments;

    /**
     * 构建并返回broker
     * @return
     */
    @Bean
    public BrokerController buildBrokerController() {
        // 从命令行参数中获取配置文件地址
        List<String> argList = new ArrayList<>();
        arguments.getOptionNames().forEach(optionName -> {
            List<String> optionValues = arguments.getOptionValues(optionName);
            for (String prop : optionValues) {
                argList.add("-" + optionName);
                argList.add(prop);
            }
        });
        String[] args = argList.toArray(new String[0]);
        BrokerConfig brokerConfig = new BrokerConfig();
        PersistentConfig persistentConfig = new PersistentConfig();

        String configPath = getConfPath(args);
        Properties properties = new Properties();
        File file = new File(configPath);
        if (!file.exists()) {
            file = new File(MQConstant.DEFAULT_CONF_PATH);
        }
        try {
            properties.load(new InputStreamReader(new FileInputStream(file)));
        } catch (IOException e) {
            log.error("Read config file error!");
            System.exit(1);
        }

        // 根据配置文件初始化broker配置类和持久化配置
        BrokerUtil.prarseConfigFile(properties, brokerConfig);
        BrokerUtil.prarseConfigFile(properties, persistentConfig);
        log.info("Read config file from disk: {}", brokerConfig);
        log.info("Read persist file from disk: {}", persistentConfig);

        try {
            File persistentFile = new File(persistentConfig.getRootPath());
            if (!persistentFile.exists()) {
                persistentFile.mkdir();
            }
            persistentFile = new File(persistentConfig.getCommitLogPath());
            if (!persistentFile.exists()) {
                persistentFile.mkdir();
            }
            persistentFile = new File(persistentConfig.getConsumerqueuePath());
            if (!persistentFile.exists()) {
                persistentFile.mkdir();
            }

        } catch (Exception e) {
            log.error("Create stroe directory error!");
            System.exit(1);
        }
        log.info("Finish create message store file");
        BrokerController brokerController = new BrokerController(brokerConfig, persistentConfig);
        boolean result = brokerController.initialize();
        if (!result) {
            log.error("BrokerController initialize error");
            System.exit(1);
        }

        return brokerController;
    }
    private String getConfPath(String[] args) {
        String path = MQConstant.DEFAULT_CONF_PATH;
        for (int i = 0; i < args.length; i++) {
            if ("-c".equals(args[i]) || "-C".equals(args[i]) || "--conf".equals(args[i])) {
                if (i == args.length - 1) {
                    return path;
                }
                path = args[i + 1];
            }
        }
        return path;
    }
}
