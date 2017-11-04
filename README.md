#ayhome-kafka kafka for php

##前言

**一个基于kafka的客户端**

KafKa是由Apache基金会维护的一个分布式订阅分发系统,KafKa它最初的目的是为了解决,统一,高效低延时,高通量(同时能传输的数据量)并且高可用一个消息平台,它是分布式消息队列,分布式日志,数据传输通道的不二之选,本扩展是基于公司自身业务二次封装的!

**支持0.9~0.10版本**

**需要安装以下扩展**

librdkafka:[https://github.com/edenhill/librdkafka](https://github.com/edenhill/librdkafka "服务底层依赖")

rdkafka PHP:[https://github.com/arnaud-lb/php-rdkafka](https://github.com/arnaud-lb/php-rdkafka "rdkafka PHP拓展地址")


##1. 安装zookeeper+KafKa

zookeeper+KafKa:[http://blog.csdn.net/lnho2015/article/details/51352966](http://blog.csdn.net/lnho2015/article/details/51352966 "zookeeper+KafKa")


```
# 安装librdkafka
git clone https://github.com/edenhill/librdkafka.git
cd librdkafka
./configure
make
make install

```

```
# 安装php-rdkafka
git clone https://github.com/arnaud-lb/php-rdkafka.git
cd php-rdkafka
phpize
./configure
make all -j 5
make install
# 在php.ini加入如下信息
vim /usr/local/php/etc/php.ini
extension=rdkafka.so  

```

这个时候使用**php -m** 可以看到拓展列表内存在 rdkafka这项证明拓展已经安装成功


### 使用 Producer

KafKa最基础的两个角色其中一个就是Producer

向KafKa中的一个Topic写入一条消息,需要写入多条可以多次使用setMassage

```
<?php
/**
 * Producer例子
 * 循环写入1w条数据15毫秒
 */

// 配置KafKa集群(默认端口9092)通过逗号分隔
$kafka = new \ayhome\kafka\KafKa("127.0.0.1,localhost");
// 设置一个Topic
$kafka->setTopic("test");
// 单次写入效率ok  写入1w条15 毫秒
$Producer = kafka->newProducer();
// 参数分别是partition,消息内容,消息key(可选)
// partition:可以设置为KAFKA_PARTITION_UA会自动分配,比如有6个分区写入时会随机选择Partition
$Producer->setMessage(0, "hello");
```

### Consumer 

对于Consumer来说支持4种从offset的获取方式分别为:

- KAFKA_OFFSET_STORED      #通过group来获取消息的offset(必须设置group)
- KAFKA_OFFSET_END			#获取尾部的offset
- KAFKA_OFFSET_BEGINNING   #获取头部的offset
- 手动指定offset开始值

####  例子1

此例子适合获取一段数据就结束的场景,每一次getMassage都会建立连接然后关闭连接,当循环使用getMassage会造成相对严重的效率问题

```
<?php
/**
 * Consumer例子1
 */

// 配置KafKa集群(默认端口9092)通过逗号分隔
$kafka = new \ayhome\kafka\KafKa("127.0.0.1,localhost");
// 设置一个Topic
$kafka->setTopic("test");
// 设置Consumer的Group分组(不使用自动offset的时候可以不设置)
$kafka->setGroup("test");
// 获取Consumer实例
$consumer = $kafka->newConsumer();

// 获取一组消息参数分别为:Partition,maxsize最大返回条数,offset(可选)默认KAFKA_OFFSET_STORED
$rs = $consumer->getMassage(0,100);
//返回结果是一个数组,数组元素类型为Kafka_Message
```

#### 例子2

例子2适合脚本队列任务

```
<?php
/**
 * Consumer例子1
 * 889 毫秒 获取1w条
 */

// 配置KafKa集群(默认端口9092)通过逗号分隔
$kafka = new \ayhome\kafka\KafKa("127.0.0.1,localhost");
// 设置一个Topic
$kafka->setTopic("test");
// 设置Consumer的Group分组(不使用自动offset的时候可以不设置)
$kafka->setGroup("test");

// 此项设置决定 在使用一个新的group时  是从 最小的一个开始 还是从最大的一个开始  默认是最大的(或尾部)
$kafka->setTopicConf('auto.offset.reset', 'smallest');
// 此项配置决定在获取数据后回自动作为一家消费 成功 无需在 一定要 stop之后才会 提交 但是也是有限制的
// 时间越小提交的时间越快,时间越大提交的间隔也就越大 当获取一条数据之后就抛出异常时 更具获取之后的时间来计算是否算作处理完成
// 时间小于这个时间时抛出异常 则不会更新offset 如果大于这个时间则会直接更新offset 建议设置为 100~1000之间
$kafka->setTopicConf('auto.commit.interval.ms', 1000);

// 获取Consumer实例
$consumer = $kafka->newConsumer();

// 开启Consumer获取,参数分别为partition(默认:0),offset(默认:KAFKA_OFFSET_STORED)
$consumer->consumerStart(0);

for ($i = 0; $i < 100; $i++) {
    // 当获取不到数据时会阻塞默认10秒可以通过$consumer->setTimeout()进行设置
    // 阻塞后由数据能够获取会立即返回,超过10秒回返回null,正常返回格式为Kafka_Message
    $message = $consumer->consume();
}

// 关闭Consumer(不关闭程序不会停止)
$consumer->consumerStop();
```

## 3. 配置文件

kafka提供两种配置文件的配置,分别传入key和value,具体配置项已经作用参看如下地址:

https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md

配置文件说明:[https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md "配置文件说明")

```
$kafka->setTopicConf();
$kafka->setKafkaConf();
```


在使用Consumer的Group(KAFKA_OFFSET_STORED)中需要注意以下配置项,否则你在使用一个新的group会从当前开始计算offset(根据场景):

```
// 此项设置决定 在使用一个新的group时  是从 最小的一个开始 还是从最大的一个开始  默认是最大的(或尾部)
$kafka->setTopicConf('auto.offset.reset', 'smallest');
```

Consumer获取之后是需要提交告诉KafKa获取成功并且更新offset,但是如果中途报错没有提交offset则下次还是会从头获取,此项配置设置一个自动提交时间,当失败后之前处理的也会吧offset提交到KafKa:

```
// 此项配置决定在获取数据后回自动作为一家消费 成功 无需在 一定要 stop之后才会 提交 但是也是有限制的
// 时间越小提交的时间越快,时间越大提交的间隔也就越大 当获取一条数据之后就抛出异常时 更具获取之后的时间来计算是否算作处理完成
// 时间小于这个时间时抛出异常 则不会更新offset 如果大于这个时间则会直接更新offset 建议设置为 100~1000之间
$kafka->setTopicConf('auto.commit.interval.ms', 1000);
```

## 4. 异常

在初始化kafka会对集群端口进行验证,如果无任何一个可用的则会抛出一个**No can use KafKa**异常,也可以主动触发ping操作检查集群是否有有可用机器











