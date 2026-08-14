# 02 - Flink 算子是什么？

## 一句话理解

**Flink 算子（Operator）就是 Flink 作业中对数据流进行处理的一个处理节点。**

可以先把它理解成：

> 数据从 Source 进入 Flink 后，每经过一步处理，这一步处理逻辑就可以看成一个算子。

例如：

```text
读取 Kafka 数据 -> 过滤无效数据 -> 转换字段 -> 按用户分组 -> 窗口统计 -> 写入数据库
```

这里面的每一步，在 Flink 中都可以对应一个或多个算子。

---

## 用 Java 后端视角理解算子

如果用普通 Java 程序来类比，算子有点像我们写业务代码时的一段处理逻辑。

例如：

```java
List<Order> orders = getOrders();

orders.stream()
      .filter(order -> order.getAmount().compareTo(BigDecimal.ZERO) > 0)
      .map(order -> convertToDTO(order))
      .collect(Collectors.toList());
```

这里的：

```text
filter
map
collect
```

都可以看成一段数据处理步骤。

Flink 中也类似，只不过 Flink 处理的不是一个已经在内存里的 List，而是一个持续不断流动的数据流。

---

## Flink 程序可以看成一条数据处理流水线

一个典型 Flink 程序可以抽象成：

```text
Source -> Transformation -> Sink
```

中文理解：

```text
数据输入 -> 数据处理 -> 数据输出
```

例如：

```text
Kafka Source -> map -> filter -> keyBy -> window -> sum -> MySQL Sink
```

这条链路里的每一个处理步骤，都可以理解为算子。

---

## 三类常见算子

Flink 里的算子可以先粗略分成三类。

### 1. Source 算子：负责读数据

Source 算子负责从外部系统读取数据。

常见 Source：

- Kafka Source
- 文件 Source
- Socket Source
- MySQL CDC Source
- 自定义 Source

例如：

```text
从 Kafka 的 order-topic 中读取订单消息
```

Source 是整个 Flink 数据流的入口。

---

### 2. Transformation 算子：负责处理数据

Transformation 算子负责对数据进行转换、过滤、分组、聚合等处理。

常见 Transformation：

- map
- flatMap
- filter
- keyBy
- reduce
- aggregate
- window
- process
- union
- connect

例如：

```text
过滤无效订单
提取订单金额
按城市分组
统计每分钟订单总金额
```

大部分业务逻辑都写在 Transformation 算子里。

---

### 3. Sink 算子：负责写数据

Sink 算子负责把计算结果写到外部系统。

常见 Sink：

- Kafka Sink
- MySQL Sink
- Redis Sink
- Doris Sink
- Elasticsearch Sink
- 文件 Sink
- 自定义 Sink

例如：

```text
把每分钟的城市订单金额写入 MySQL，供报表或大屏查询
```

Sink 是整个 Flink 数据流的出口。

---

## 常见 Transformation 算子理解

### map：一条数据变成一条数据

`map` 用于把一条数据转换成另一条数据。

例如：

```text
原始 JSON 字符串 -> Order 对象
Order 对象 -> 订单金额
```

特点：

```text
输入 1 条，输出 1 条
```

---

### filter：过滤数据

`filter` 用于保留符合条件的数据。

例如：

```text
只保留支付成功的订单
过滤掉测试订单
过滤掉金额小于等于 0 的订单
```

特点：

```text
输入 1 条，可能输出 1 条，也可能不输出
```

---

### flatMap：一条数据变成 0 条、1 条或多条数据

`flatMap` 比 `map` 更灵活。

例如：

```text
一行文本 -> 拆成多个单词
一个订单 -> 拆成多个订单明细
```

特点：

```text
输入 1 条，可能输出 0 条、1 条或多条
```

---

### keyBy：按 key 分组

`keyBy` 是 Flink 中非常重要的算子。

它的作用是：

> 按照某个字段把数据分组，相同 key 的数据会被发送到同一个并行任务中处理。

例如：

```text
按 city 分组
按 userId 分组
按 productId 分组
按 orderId 分组
```

示例：

```text
keyBy(city)
```

表示：

```text
北京订单都交给同一个 Task
上海订单都交给同一个 Task
广州订单都交给同一个 Task
```

这为后面的状态计算和窗口统计打基础。

---

### window：按时间切分数据流

`window` 用于把无限的数据流切成一段一段来计算。

例如：

```text
每 1 分钟统计一次订单金额
每 5 分钟统计一次商品点击量
每 10 秒统计一次接口 QPS
```

窗口一般会和 `keyBy` 一起使用：

```text
keyBy(city) -> window(1分钟) -> sum(amount)
```

意思是：

```text
每 1 分钟统计每个城市的订单金额
```

---

### reduce / aggregate / sum：聚合计算

这些算子用于做聚合统计。

例如：

```text
求和
计数
求最大值
求最小值
求平均值
```

示例：

```text
统计每个城市的订单总金额
统计每个商品的点击次数
统计每个用户的累计消费金额
```

---

### process：更灵活的底层处理算子

`process` 是比较强大的算子，可以访问：

- 当前数据
- 当前 key
- 时间信息
- 定时器
- 状态

它比 `map`、`filter` 更灵活，但也更复杂。

初学阶段不用急着深入，先知道：

> 当普通算子不够用时，可以使用 ProcessFunction 实现更复杂的逻辑。

---

## 一个订单统计例子

需求：

> 从 Kafka 读取订单数据，统计每 1 分钟每个城市的订单金额，然后写入 MySQL。

可以抽象成：

```text
Kafka Source
    ↓
map：JSON 转 Order 对象
    ↓
filter：只保留支付成功订单
    ↓
keyBy：按 city 分组
    ↓
window：按 1 分钟开窗口
    ↓
sum / aggregate：统计订单金额
    ↓
MySQL Sink
```

这条链路里，每个节点都可以理解为一个算子。

---

## 算子和并行度的关系

Flink 算子通常可以并行执行。

例如一个 `map` 算子可以有多个并行实例：

```text
map-1
map-2
map-3
```

这些并行实例可以同时处理不同的数据，从而提升吞吐量。

例如：

```text
Kafka Source 并行读取数据
map 并行解析 JSON
filter 并行过滤数据
keyBy 后按 key 重新分发
window 并行统计不同 key 的窗口结果
```

所以 Flink 不是单线程一条线执行，而是会把一条数据处理链路拆成多个可以并行运行的任务。

---

## 算子和 Task 的关系

入门阶段可以这样理解：

```text
算子：逻辑上的处理步骤
Task：算子真正运行时的执行实例
```

例如代码里写了一个 `map` 算子，但并行度是 3，那么运行时可能有 3 个 map task：

```text
map task 1
map task 2
map task 3
```

所以：

> 算子是逻辑概念，Task 是运行时概念。

---

## 算子链 Operator Chain

Flink 为了提升性能，可能会把多个连续的小算子合并到一个 Task 中执行。

例如：

```text
map -> filter -> map
```

这几个算子如果条件允许，可能会被 Flink 合并成一个算子链。

这样可以减少线程切换、序列化和网络传输开销。

初学阶段只需要知道：

> 代码里看到多个算子，不代表运行时一定是完全独立的多个线程或进程，Flink 可能会做优化合并。

---

## 本节总结

Flink 算子就是数据流处理过程中的一个处理节点。

可以用下面这句话记住：

> Source 负责读数据，Transformation 负责处理数据，Sink 负责写数据；这些处理步骤都可以理解为 Flink 算子。

常见算子：

```text
Source：Kafka Source、File Source、MySQL CDC Source
Transformation：map、filter、flatMap、keyBy、window、reduce、aggregate、process
Sink：Kafka Sink、MySQL Sink、Redis Sink、Doris Sink、Elasticsearch Sink
```

对于初学者，最重要的是先建立这条主线：

```text
数据从哪里来 -> 中间怎么处理 -> 最后写到哪里去
```

也就是：

```text
Source -> Transformation -> Sink
```
