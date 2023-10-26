package com.github.xjtuwsn.cranemq.broker;

import com.github.xjtuwsn.cranemq.broker.store.PersistentConfig;
import com.github.xjtuwsn.cranemq.common.config.BrokerConfig;
import com.github.xjtuwsn.cranemq.common.constant.MQConstant;
import com.github.xjtuwsn.cranemq.common.exception.CraneBrokerException;
import com.github.xjtuwsn.cranemq.common.utils.BrokerUtil;
import com.github.xjtuwsn.cranemq.extension.impl.NacosWritableRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.io.ClassPathResource;

import javax.annotation.Resource;
import java.io.*;
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
 * @author wsn
 */
@Configuration
public class BrokerControllerFactory implements ApplicationContextAware {
    private static final Logger log = LoggerFactory.getLogger(BrokerControllerFactory.class);
    // 命令行参数
    @Resource
    private ApplicationArguments arguments;

    private ApplicationContext applicationContext;

    @Value("${cranemq.conf}")
    private String path;

    /**
     * 构建并返回broker
     * @return
     */
    @Bean("brokerController")
    @DependsOn(value = "producerMessageService")
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
        Properties properties = new Properties();
        try {
            InputStream configFileStream = getConfFile(args);
            properties.load(new InputStreamReader(configFileStream));
        } catch (IOException e) {
            log.error("Read config file error!");
            System.exit(SpringApplication.exit(applicationContext));
        }

        // 根据配置文件初始化broker配置类和持久化配置
        BrokerUtil.parseConfigFile(properties, brokerConfig);
        BrokerUtil.parseConfigFile(properties, persistentConfig);
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
            log.error("Create stroke directory error!");
            System.exit(SpringApplication.exit(applicationContext));
        }
        log.info("Finish create message store file");
        BrokerController brokerController = new BrokerController(brokerConfig, persistentConfig);

        return brokerController;
    }
    private InputStream getConfFile(String[] args) throws FileNotFoundException {
        for (int i = 0; i < args.length; i++) {
            if ("-c".equals(args[i]) || "-C".equals(args[i]) || "--conf".equals(args[i])) {
                if (i == args.length - 1) {
                    break;
                }
                File file = new File(args[i + 1]);
                if (file.exists()) {
                    return new FileInputStream(file);
                }
            }
        }
        if (path == null || path.isEmpty()) {
            log.error("Can not read config file path");
            System.exit(SpringApplication.exit(applicationContext));
        }
        if (path.startsWith(MQConstant.CLASSPATH_SUFFIX)) {
            ClassPathResource classPathResource = new ClassPathResource(
                    path.substring(MQConstant.CLASSPATH_SUFFIX.length()));
            try {
                return classPathResource.getInputStream();
            } catch (IOException e) {
                log.error("Classpath file error, path = {}", path);
                e.printStackTrace();
                System.exit(SpringApplication.exit(applicationContext));
            }
        }

        File file = new File(path);
        if (!file.exists()) {
            log.error("No such file, path = {}", path);
            System.exit(SpringApplication.exit(applicationContext));
        }

        return new FileInputStream(file);
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
