# Data Pipeline

## 简介

基于消息队列（暂时仅支持 Kafka）的实时数据同步，包括：
1. 同步 MySQL 数据，基于 Binlog 日志，连接 MySQL 的用户最少要有 SELECT, RELOAD, LOCK TABLES, REPLICATION CLIENT, SHOW VIEW, TRIGGER 权限。
2. 同步 MongoDB 数据，基于 ChangeStream 功能，并开启 replicate。

## 系统依赖：

1. Python 版本 >= 3.6
2. MySQL 版本 >= 5.6
3. MongoDB 版本 >= 3.6
4. 操作系统安装 snappy 驱动
    ```
    APT: apt-get install snappy-dev
    RPM: yum install snappy-devel
    Brew: brew install snappy
    ```
5. 操作系统安装 librdkafka 驱动，`export RDKAFKA_INSTALL=system`
    ```
    APT: apt install librdkafka-dev
    RPM: yum install librdkafka-devel
    Brew: brew install librdkafka
    ```

## 功能

1. MySQL 全量增量数据获取写入 kafka
2. MongoDB 全量增量数据获取写入 kafka
3. 消费 kafka 写入 MySQL 或者 MongoDB
4. 基于 celery 方式进程启动
5. 基于 docker 方式进程启动


## 接下来

1. 支持 PostgreSQL，包括读写
    1. 全量与实时数据读取
    2. 写入 PostgreSQL

2. 从 kafka 消费数据，写入 ElasticSearch

3. k8s 支持
