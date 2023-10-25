package com.github.xjtuwsn.cranemq.client.spring.factory;

import cn.hutool.core.lang.Pair;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.github.xjtuwsn.cranemq.client.consumer.DefaultPushConsumer;
import com.github.xjtuwsn.cranemq.client.consumer.listener.CommonMessageListener;
import com.github.xjtuwsn.cranemq.client.consumer.listener.MessageListener;
import com.github.xjtuwsn.cranemq.client.consumer.listener.OrderedMessageListener;
import com.github.xjtuwsn.cranemq.client.producer.DefaultMQProducer;
import com.github.xjtuwsn.cranemq.client.spring.annotation.CraneMQListener;
import com.github.xjtuwsn.cranemq.client.spring.template.CraneMQTemplate;
import com.github.xjtuwsn.cranemq.common.consumer.MessageModel;
import com.github.xjtuwsn.cranemq.common.consumer.StartConsume;
import com.github.xjtuwsn.cranemq.common.entity.ReadyMessage;
import com.github.xjtuwsn.cranemq.common.exception.CraneClientException;
import com.github.xjtuwsn.cranemq.common.remote.RemoteHook;
import com.github.xjtuwsn.cranemq.common.remote.enums.RegistryType;
import com.github.xjtuwsn.cranemq.common.remote.serialize.Serializer;
import com.github.xjtuwsn.cranemq.common.remote.serialize.impl.Hessian1Serializer;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.config.InstantiationAwareBeanPostProcessor;
import org.springframework.beans.factory.config.YamlMapFactoryBean;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.core.io.ClassPathResource;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @project:cranemq
 * @file:CraneClientFactory
 * @author:wsn
 * @create:2023/10/13-22:19
 * 向spring容器中注入生产者和消费者
 */
public class CraneClientFactory implements FactoryBean<CraneMQTemplate>, ApplicationListener<ContextRefreshedEvent>,
        InstantiationAwareBeanPostProcessor {

    // 对消息体进行序列化
    private Serializer serializer = new Hessian1Serializer();

    // 从配置文件读取配置
    private Map<String, String> producerConf;

    private Map<String, String> consumerConf;

    private Map<String, String> registryConf;

    // 订阅信息，所有消费者组成员合一
    private List<Pair<String, String>> infos;

    // 预构建的消费者
    private List<DefaultPushConsumer.Builder> consumers;
    private String consumerGroup;
    private MessageModel messageModel;
    private StartConsume startConsume;
    private String address;
    private RegistryType registryType;

    private RemoteHook remoteHook;
    public CraneClientFactory() {
        // 读取解析配置文件
        YamlMapFactoryBean factory = new YamlMapFactoryBean();
        factory.setResources(new ClassPathResource("application.yml"));
        Map<String, Object> properties = factory.getObject();
        Object mqConf = properties.get("cranemq");
        String str = JSON.toJSONString(mqConf);
        Map<String, Map<String, String>> map = JSONObject.parseObject(str, new TypeReference<>(){});

        if (map == null) {
            throw new CraneClientException("Configuration is null");
        }
        producerConf = map.get("producer");

        consumerConf = map.get("consumer");

        registryConf = map.get("registry");

        consumers = new ArrayList<>();

        infos = new ArrayList<>();
        constructConsumer();
    }

    // 根据配置文件初始化参数
    private void constructConsumer() {
        if (consumerConf == null) {
            throw new CraneClientException("Consumer configuration is null");
        }
        if (registryConf == null || registryConf.get("address") == null) {
            throw new CraneClientException("Registry configuration is null");
        }
        consumerGroup = consumerConf.get("group");
        messageModel = "cluster".equals(consumerConf.getOrDefault("model", "cluster"))
                ? MessageModel.CLUSTER : MessageModel.BRODERCAST;
        startConsume = "last".equals(consumerConf.getOrDefault("start", "last"))
                ? StartConsume.FROM_LAST_OFFSET : StartConsume.FROM_FIRST_OFFSET;
        address = registryConf.get("address");
        String type = registryConf.get("type");
        if ("zk".equals(type)) {
            registryType = RegistryType.ZOOKEEPER;
        } else if ("nacos".equals(type)) {
            registryType = RegistryType.NACOS;
        } else {
            registryType = RegistryType.DEFAULT;
        }
    }

    /**
     * spring容器初始化完成后，也就是所有消费者都遍历到之后，进行构建和启动
     * @param event
     */
    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        for (DefaultPushConsumer.Builder consumer : consumers) {
            consumer.subscribe(infos).build().start();
        }
    }

    /**
     * 扫描带有指定注解的方法
     * @param bean
     * @param beanName
     * @return
     * @throws BeansException
     */
    @Override
    public boolean postProcessAfterInstantiation(Object bean, String beanName) throws BeansException {
        Method[] methods = bean.getClass().getMethods();
        for (Method method : methods) {
            // 为该方法构建listener
            if (method.isAnnotationPresent(CraneMQListener.class)) {
                CraneMQListener annotation = method.getAnnotation(CraneMQListener.class);
                String id = annotation.id();
                String topic = annotation.topic();
                String tag = annotation.tag();
                boolean isOrdered = annotation.ordered();
                Class<?> clazz = annotation.dataType();
                MessageListener messageListener = null;
                if (!isOrdered) {
                    // 普通消息
                    messageListener = new CommonMessageListener() {
                        @Override
                        public boolean consume(List<ReadyMessage> messages) {
                            try {
                                for (ReadyMessage readyMessage : messages) {
                                    if (!readyMessage.matchs(topic, tag)) {
                                        continue;
                                    }
                                    Object data = serializer.deserialize(readyMessage.getBody(), clazz);
                                    if (!clazz.isInstance(data)) {
                                        continue;
                                    }
                                    method.invoke(bean, data);
                                }
                                return true;
                            } catch (Exception e) {
                                e.printStackTrace();
                                return false;
                            }
                        }
                    };
                } else {
                    // 顺序消息
                    messageListener = new OrderedMessageListener() {
                        @Override
                        public boolean consume(List<ReadyMessage> messages) {

                            try {
                                for (ReadyMessage readyMessage : messages) {
                                    if (!readyMessage.matchs(topic, tag)) {
                                        continue;
                                    }
                                    Object data = serializer.deserialize(readyMessage.getBody(), clazz);
                                    if (!clazz.isInstance(data)) {
                                        continue;
                                    }
                                    method.invoke(bean, data);
                                }
                                return true;
                            } catch (Exception e) {
                                return false;
                            }
                        }
                    };
                }
                // 构建消费者
                DefaultPushConsumer.Builder consumer = DefaultPushConsumer.builder()
                        .consumerId(id)
                        .consumerGroup(consumerGroup)
                        .bindRegistry(address)
                        .messageModel(messageModel)
                        .startConsume(startConsume)
                        .messageListener(messageListener)
                        .registryType(registryType);
                this.infos.add(new Pair<>(topic, tag));
                this.consumers.add(consumer);
            }
        }
        return true;
    }

    /**
     * 工厂产生template
     * @return
     * @throws Exception
     */
    @Override
    public CraneMQTemplate getObject() throws Exception {
        if (registryConf == null || registryConf.get("address") == null) {
            throw new CraneClientException("Registry configuration is null");
        }
        String address = registryConf.get("address");
        String group = producerConf.get("group");
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer(group, remoteHook, address);
        defaultMQProducer.bindRegistry(address, registryType);
        defaultMQProducer.start();
        return new CraneMQTemplate(defaultMQProducer, serializer);
    }

    @Override
    public Class<?> getObjectType() {
        return CraneMQTemplate.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    public void registerHook(RemoteHook hook) {
        this.remoteHook = hook;
    }



}
