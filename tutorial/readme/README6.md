# step06：用消息队列解耦投稿链路

## 这一步解决什么问题

看 step05 里投稿接口的结尾：

```python
redis_client.save_post(post_id, post)   # 同步阻塞在这里
return jsonify(...), 202
```

"存稿件"看起来天经地义，但在真实系统里，一篇稿件后面跟着一堆事：

- 内容审核（调第三方接口，可能几百毫秒）
- 全文索引（写 Elasticsearch）
- 推送通知（发 MQ / 调推送服务）
- 计数统计、风控打点……

全塞进请求里会怎样？**接口的延迟和失败率，由这一串里最慢、最不稳定的那一环决定。**
任何一个下游抖动，用户投稿就直接失败。

改成：接口只负责**投递一条消息**，谁关心这篇稿件，谁自己去订阅。

## 架构变化

```
【step05】同步
  POST -> 校验 -> 幂等 -> 限流 -> 存库 -> 202

【step06】异步
  POST -> 校验 -> 幂等 -> 限流 -> 投递到 topic -> 202(queued)
                                     |
                                     v  （异步、另一个进程）
                               consumer -> 存库 -> status=published
```

解耦之后的好处（以后加需求时才能体会到）：

> 要加"内容审核"，就新写一个消费者组订阅同一个 topic。
> **投稿接口一行代码都不用改。**

## 怎么跑

需要两个终端（Web 服务和消费者是**两个独立进程**）：

```powershell
# 终端 1：Web 服务（只投递消息）
cd tutorial\step06_kafka_decouple
..\..\.venv\Scripts\python.exe run.py

# 终端 2：消费者（真正落库）
cd tutorial\step06_kafka_decouple
..\..\.venv\Scripts\python.exe consumer.py
```

外部服务（Redis + Redpanda）的启动命令见上级 `README.md` 的"依赖的外部服务"。

### 亲手调一次

```powershell
# 1. 投稿：立刻返回，状态是 queued
Invoke-RestMethod -Uri http://127.0.0.1:8000/api/v1/posts -Method Post `
  -ContentType "application/json" `
  -Headers @{ "X-Request-ID" = "my-trace-1" } `
  -Body '{"client_msg_id":"m1","author_id":"u1","title":"标题","content":"正文"}'

# 2. 立刻查：可能还是 queued（消费者还没处理到）
Invoke-RestMethod -Uri http://127.0.0.1:8000/posts/<上一步返回的 post_id>

# 3. 过一两秒再查：变成 published，内容完整
```

重点观察两个终端的日志：**同一个 `request_id=my-trace-1`，
同时出现在 Web 服务和消费者两个进程的日志里。**

## 新增/改动了哪些文件

| 文件 | 动作 | 说明 |
|---|---|---|
| `kafka_client.py` | 新增 | 生产者/消费者封装，可靠性配置都在这里 |
| `consumer.py` | 新增 | 独立消费进程 |
| `config.py` | 改 | 加 `KAFKA_*` 和 `KEY_STATUS`、`STATUS_TTL_SECONDS` |
| `redis_client.py` | 改 | 加 `save_post_status` / `get_post_status` / `release_idempotency` |
| `api/posts.py` | 改 | `save_post` 换成投递消息；查询接口支持 queued 状态 |
| `app.py` | 改 | 生产者挂到 `app.extensions` |
| `run.py` | 改 | 启动时自检 Kafka（只告警不退出） |
| `logging_setup.py` | 改 | 把 kafka 库的内部日志压到 WARNING，避免淹没业务日志 |
| `errors.py` / `api/health.py` | 不动 | — |

## 必须讲清楚的 5 个点

### 1. 生产者的可靠性配置（默认值的坑）

```python
acks="all"                                 # 等所有副本写成功才算成功
retries=3                                  # 网络抖动自动重试
max_in_flight_requests_per_connection=1    # 重试时保证同一分区不乱序
```

默认的 `acks=1` 是"leader 写成功就返回"。leader 还没同步给副本就宕机 →
这条消息**永久丢失**，而且客户端已经收到"成功"了。

另外，`publish()` 里有一行很关键的 `future.get(timeout=10)`：
**必须等 broker 确认**。不等的话接口先返回 202，用户以为成功，消息却没进 Kafka ——
这种"静默丢失"是线上最难查的一类问题。

### 2. 重复消息无法避免 -> 消费者必须幂等

Java 官方客户端和 `confluent-kafka` 支持**幂等生产者**（`enable.idempotence=true`），
由 broker 自动去重。**但 kafka-python 系列没有实现这个能力。**

所以"同一条消息被写了两遍"是一定会发生的。消费者侧的幂等不是加分项，是**必选项**。

好消息是这一步几乎没花力气：`post_id` 直接覆盖写同一个 key，处理 N 次结果完全一样。
——这正是 step02 打下的幂等基础在这里的兑现。

### 3. 手动提交 offset

```python
enable_auto_commit=False   # 关闭自动提交
...
handler(msg.value, meta)   # 先处理业务
self._consumer.commit()    # 成功才提交
```

自动提交是"拉取后隔几秒就提交"，不管业务处理完没有。
进程在这中间挂掉，那批消息就**再也不会被消费**了。

关掉它之后，语义变成 **at-least-once（至少一次）**：
没提交 offset 的消息，重启后会被重新投递。代价就是重复处理 —— 由第 2 点兜底。

> 生产环境还要考虑"毒丸消息"：一条永远处理失败的消息会反复堵住进度。
> 常见做法是重试 N 次后扔进**死信队列（DLQ）**再提交 offset。

### 4. request_id 跨进程透传（contextvars 的兑现点）

生产者把 ID 塞进**消息头**，消费者取出来 `bind_request_id()`：

```python
# 生产者
producer.publish(event, request_id=get_request_id(), key=author_id)

# 消费者（kafka_client.py 内部）
request_id = _extract_request_id(msg.headers) or "-"
bind_request_id(request_id)
```

于是两个**独立进程**的日志能被同一个 ID 串起来：

```
{"event":"submit.accepted","post_id":"...","request_id":"trace-1959412c",...}   <- Web
{"event":"post.published", "post_id":"...","request_id":"trace-1959412c",...}   <- 消费者
```

这就是 step05 选 `contextvars` 而不是 Flask `g` 的原因：`g` 出了 HTTP 请求就没了，
而 `contextvars` 是标准库，在消费进程、定时任务里照样能用。

### 5. 异步化的代价：最终一致

投递完稿件还没落库，此时 `GET /posts/<id>` 怎么办？

| 情况 | 返回 |
|---|---|
| 消费者已落库 | `200` + 完整稿件 + `"status":"published"` |
| 刚投递，还在排队 | `200` + `{"status":"queued", "hint": ...}` |
| 完全不存在 | `404` |

⚠️ **这是本步唯一的对外行为变化**：投稿响应从 `"status":"accepted"` 变成 `"status":"queued"`，
查询接口多了一个中间态。这是异步化必然引入的，值得暴露出来而不是藏起来。

`queued` 状态**必须带 TTL**：万一消费者整体挂掉，不能永久留下脏数据。

## 一个容易漏掉的坑：失败时要释放幂等键

```python
except KafkaUnavailable:
    redis_client.release_idempotency(client_msg_id)   # 关键
    return error_response("kafka_unavailable", ..., 503)
```

占位幂等键之后业务却失败了，不释放会怎样？
用户重试时收到"已受理过"，但那篇稿件**根本没进系统** ——
幂等键被一次失败的请求占住了。这是个很隐蔽的坑。

## 客户端选型说明

| 方案 | 优点 | 缺点 |
|---|---|---|
| `kafka-python-ng`（本步采用） | 纯 Python，零编译，Windows 友好 | 不支持幂等生产者，吞吐低于 librdkafka |
| `confluent-kafka` | 官方推荐，librdkafka，功能全 | C 扩展，Windows 上偶有编译/轮子问题 |
| `aiokafka` | 原生 asyncio | 要和异步框架搭配，本项目是同步 Flask |

> 踩过的坑：`kafka-python==2.0.2` 内置的 `six` 用了 Python 3.12 已移除的
> `find_module` 钩子，导入就报 `No module named 'kafka.vendor.six.moves'`。
> `kafka-python-ng` 是它的维护分支，import 名仍是 `kafka`，代码写法完全一样。

生产环境建议换成 `confluent-kafka`：它支持幂等生产者，能从源头减少重复消息。

## 自测练习

1. 为什么 `acks` 的默认值会丢消息？
2. 自动提交 offset 会造成什么问题？关掉之后又带来什么新问题？
3. kafka-python 不支持幂等生产者，后果是什么？靠什么兜底？
4. 消费者处理消息失败时，代码里为什么不提交 offset？一直不提交会怎样？
5. `request_id` 是怎么从 HTTP 请求跑到另一个进程的日志里的？用 Flask 的 `g` 行不行？
6. 稿件投递成功但消费者挂了，此时 `GET` 返回什么？为什么 `queued` 要设 TTL？
7. 投递失败时如果不释放幂等键，用户会遇到什么诡异现象？
8. 消息的 `key` 用的是 `author_id`，换成随机值会有什么后果？（提示：分区与顺序）
9. 现在要加"内容审核"，需要改动投稿接口吗？
10. 为什么消费者要在 `finally` 里 `clear_request_id()`？不清理会怎样？

## 验证结果

24 项检查全部通过，其中关键几项：

- 投稿返回 `202` + `queued`；消费者未启动时查询返回 `queued` 而非 404
- 消费者落库后查询返回 `published` + 完整内容
- **跨进程 request_id 一致**（`submit.accepted` 与 `post.published` 同一个 ID）
- 相同 `client_msg_id` 只产生 1 条 `published`（生产者侧幂等）
- 未提交 offset 的消息会被重新投递（at-least-once）
- 同一条消息重复处理，落库结果一致（消费者侧幂等）
- Kafka 不可用时返回 503，且幂等键已释放、重试可成功
- step03/04/05 的能力（限流、蓝图、结构化日志）均未退化
