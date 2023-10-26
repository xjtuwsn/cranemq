## 介绍

出于对消息队列的兴趣，以及为了更好的学习[rocketmq](https://github.com/apache/rocketmq)和[kafka](https://github.com/apache/kafka)，我选择对mq的核心功能进行了实现。基于这些开源mq的架构和设计思想，我尝试用自己的方式进行了实现，该项目也主要是出于**个人学习目的**

目前已实现如下功能：

+ 生产者以同步、异步、单向方式发送消息
+ 单个消息和批量消息
+ 发送普通消息、顺序消息、延时消息
+ 超时检测和失败重试
+ 钩子函数
+ 发送队列负载均衡
+ 消费者订阅主题
+ pull和push方式获取消息
+ 历史消息消费
+ 批量拉取
+ 消费者组管理与监听
+ 消费失败重试
+ 广播消费模式和集群消费模式
+ 消息过滤
+ 本地位移管理
+ broker端持久化存储消息
+ 管理消费者位移
+ 管理生产者和消费者，心跳机制
+ 向注册中心注册
+ 提供重试队列、延时队列和死信队列
+ 多种注册中心
+ 支持灰度发布
+ 与spring框架集成

## 特性

1. 基于发布-订阅模型实现消息队列，通过生产者组和消费者组统一管理

2. 底层基于Netty构建高效通信框架，使用多种自定义协议完成通讯

3. 支持普通消息的发送和接受，同时基于shardingkey和分布式锁和本地锁实现顺序消息，消费者支持pull与push两种消费模式，并提供多种生产者队列选择的负载均衡策略

4. 实现多种通信调用方式，并提供失败重试和超时控制机制

5. 所有消息全部持久化在磁盘，保证可靠性，同时基于mmap和pagecahe机制实现高效存储和读写分离，建立消息索引文件加速读取，不依赖其它数据库

6. 通过不同模式的消费者位移持久化管理和确认机制，保证消息至少被消费一次，并且可以消费历史消息

7. 通过心跳机制定期摘除不活跃连接

8. 实现消费者组和服务端变化时的rebalance机制，实现消息重分配，内置多种分配策略
9. 内部实现时间轮来进行高效的延时任务调度，支持生产者生产任意时间延时消息，并持久化延迟任务，引入提交机制进行崩溃恢复
10. 借助延时队列实现消息失败定时重试，并将多次失败消息加入死信队列，支持死信队列的查看
11. 使用线程池提升效率，通过事件监听和请求队列，生产者和消费者模式进行解耦，并通过各种机制保证并发安全
12. 支持灰度消息分流，保证灰度发布前与发布后订阅关系与消费进度一致性
13. 实现注册中心，提供队列发现能力，方便进行集群扩展，支持更换默认注册中心为zookeeper和nacos
14. 支持水平扩展，目前支持多主集群
15. 与Spring和SpringBoot框架进行集成，使用注解和starter简化配置，方便调用



## 快速开始

### 项目结构

``` shell
- broker                               : MQ服务端实现，负责消息存储、通信、客户端管理等 
- client                               : MQ客户端实现，包括消费者和生产者
- common                               : 通用属性
- example                              : 使用示例
     - example-frameless               : 无框架使用
     - example-spring                  : 结合spring使用
- extension                            : 扩展
     - cranemq-spring-boot-starter     : springboot-starter
     - nacos-extension                 : nacos注册中心集成
     - zookeeper-extension             : zookeeper注册中心集成
- registry                             : 轻量注册中心
- test
```

### Broker

broker端采用SpringBoot编写，下载源码后，使用maven对项目进行编译，产生jar包运行

#### 配置文件

无其它依赖，需要指定配置文件位置，在`broker/resources`下有示例配置文件，配置优先级：

1. 启动时以命令行方式给处配置文件全路径
2. 在`application.yaml`中配置路径

#### 端口

默认情况下broker需要使用两个端口：

+ 与客户端通信端口`6086`
+ 队列查看页面端口`7654`
  + 启动后访问`http://localhost:7654/`查看队列信息



### Registry

自带配置中心可以直接启动，默认端口`11111`，可以使用`zookeeper`或`nacos`



### 消费者和生产者

无框架使用引入

``` java
<dependency>
    <groupId>com.github.xjtuwsn</groupId>
    <artifactId>client</artifactId>
    <version>0.0.1</version>
</dependency>
```

`SpringBoot`引入

``` java
<dependency>
    <groupId>com.github.xjtuwsn</groupId>
    <artifactId>cranemq-spring-boot-starter</artifactId>
    <version>0.0.1</version>
</dependency>
```

具体使用示例参考`example`模块



## 完善

因为个人时间和水平(比较关键)问题，所以项目还有着不少的问题，和许多可以完善的点，包括但不仅限于以下几点：

+ 完整测试，目前仅做了功能性测试，各种情况还没有考虑到
+ 性能优化，通过压测和监控解决性能瓶颈
+ 代码规范，代码写的比较仓促，耦合度高

功能方面：

+ 集群扩展，目前仅支持多主集群，实现方便而可靠性差，期望实现主从集群
+ 消费者流量控制
+ ACL
+ 事务
+ 消息压缩，存储优化
+ 等等

## 联系

+ e-mail：`wwshining@qq.com`