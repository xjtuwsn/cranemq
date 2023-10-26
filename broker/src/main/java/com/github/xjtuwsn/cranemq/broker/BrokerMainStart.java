package com.github.xjtuwsn.cranemq.broker;

import com.github.xjtuwsn.cranemq.broker.store.PersistentConfig;
import com.github.xjtuwsn.cranemq.common.config.BrokerConfig;
import com.github.xjtuwsn.cranemq.common.constant.MQConstant;
import com.github.xjtuwsn.cranemq.common.utils.BrokerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
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
 * @file:BrokerMain
 * @author:wsn
 * @create:2023/09/26-19:52
 */
@Component
@DependsOn(value = "brokerController")
public class BrokerMainStart implements ApplicationContextAware {
    private static final Logger log = LoggerFactory.getLogger(BrokerMainStart.class);

    private ApplicationContext applicationContext;
    @Resource
    private BrokerController brokerController;
    @PostConstruct
    public void start() {
        boolean result = brokerController.initialize();
        if (!result) {
            log.error("BrokerController initialize error");
            System.exit(SpringApplication.exit(applicationContext));
        }
        brokerController.start();
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
