# step05：结构化日志 + request_id

## 这一步学什么

**依然不改业务逻辑**，接口行为和 step04 完全一样。

新学两件事：

1. **为什么 `print` 不够用** —— 以及结构化日志长什么样
2. **request_id 全链路透传** —— 把一次请求的所有日志串起来

## 为什么 print 不够用

假设用户反馈"我投稿失败了"，你打开控制台看到一堆输出：

```
收到投稿
写Redis成功
收到投稿
限流了
```

问题来了：**哪几行是同一个人的请求？** 分不清。而且没法按字段检索、分不出严重程度。

结构化日志解决这些：每条日志是**一个事件名 + 若干字段**，输出成 JSON：

```json
{"event": "submit.accepted", "level": "info", "request_id": "a1b2c3", "post_id": "9ed6...", "author_id": "author-A", "timestamp": "2026-10-12T08:00:00Z"}
```

有了它，日志系统（ELK / Loki / 云厂商服务）可以按字段检索：
"给我所有 `request_id = a1b2c3` 的日志"。

## request_id 是什么

一次请求的**身份证**。同一个请求产生的所有日志，都带上同一个 ID。

### 它怎么产生和传递

```
请求进来
  │
  ├─ 请求头里有 X-Request-ID？
  │     ├─ 有  → 沿用（上游服务传下来的，跨服务保持同一条链）
  │     └─ 没有 → 新生成一个
  │
  ├─ 存进 contextvars（绑定上下文）
  │
  ├─ 处理请求：这期间打的所有日志自动带上它
  │
  └─ 响应头回显 X-Request-ID
```

**回显到响应头有什么用**：用户反馈问题时，让他把这个 ID 给你，
你就能把他那次请求的所有日志一次性捞出来。

### 为什么用 contextvars 而不是 Flask 的 `g`

| | Flask 的 `g` | contextvars |
|---|---|---|
| 写法 | `g.request_id = x` | `_var.set(x)` |
| Flask 之外能用吗 | 不能 | **能** |
| 异步安全 | 一般 | 是 |

关键区别：`g` 是 Flask 专属的，只在 HTTP 请求里有效。
**step06 消费 Kafka 消息时，代码不在任何 HTTP 请求里，`g` 就失效了**，
而 contextvars 是 Python 标准库，照样能用。

> contextvars 可以理解为"每个请求/协程独享的小盒子"：
> 你把 request_id 放进去，这个请求执行期间随时能取出来，
> 而且不同请求之间互不干扰。

## 埋了哪些日志

| 位置 | 事件 | 级别 | 什么时候出现 |
|---|---|---|---|
| `app.py` | `app.ready` | info | 服务启动完成 |
| `app.py` | `http.access` | info | 每个请求结束（含耗时） |
| `posts.py` | `submit.accepted` | info | 投稿受理成功 |
| `posts.py` | `submit.duplicated` | info | 幂等命中（重复提交） |
| `posts.py` | `submit.rate_limited` | warning | 触发限流 |
| `health.py` | `health.redis_unreachable` | error | Redis 连不上 |

级别的意义：`warning`/`error` 通常要配告警，`info` 只做记录。

**不刷屏原则**：只在"事情发生变化"的地方记日志（受理成功、被拒绝、依赖异常），
不要每行代码都记，否则日志会淹没真正有用的信息。

## 跑起来看

```powershell
docker start cp-redis
cd D:\Friday\Code\Code\Project\new_project\tutorial\step05_logging_request_id
..\..\.venv\Scripts\Activate.ps1
python run.py
```

默认（开发环境）是**人类可读**格式：

```
2026-10-12T08:00:00Z [info] http.access method=POST path=/api/v1/posts duration_ms=3.21 request_id=a1b2c3
```

想看 JSON（生产格式）：

```powershell
$env:LOG_JSON="true"; python run.py
```

## 亲手验证 request_id

```powershell
# 1. 带自定义 request_id 请求
(Invoke-WebRequest -Method POST http://127.0.0.1:8000/api/v1/posts `
  -ContentType "application/json" `
  -Headers @{"X-Request-ID"="my-trace-1"} `
  -Body '{"client_msg_id":"msg-1","author_id":"author-A","title":"测试","content":"正文"}').Headers

#   看响应头里的 X-Request-ID —— 应该就是 my-trace-1
#   再看服务日志 —— 那次请求的所有日志 request_id 都是 my-trace-1

# 2. 不带请求头再发一次
#   会得到一个新的随机 ID，与上次不同
```

## 自测练习

1. 结构化日志相比 print 解决了哪三个问题？
2. request_id 为什么要回显到响应头？
3. 请求头里已经带了 X-Request-ID 时，服务应该沿用还是重新生成？为什么？
4. 为什么 request_id 用 contextvars 而不是 Flask 的 g？
5. 为什么访问日志要放在 `teardown_request` 而不是 `after_request`？
6. 请求结束后为什么要调用 `clear_request_id()`？不清理会怎样？
7. 业务代码里打日志时，需要手动传 request_id 吗？
8. 开发环境为什么用人类可读格式而不是 JSON？

<details>
<summary>答案</summary>

1. 机器可解析（能检索）、能区分级别、能关联到具体某次请求。
2. 用户反馈问题时可提供这个 ID，你就能把他那次请求的全部日志捞出来，不用在一堆日志里猜。
3. 沿用。因为跨服务调用时，上游已经生成了 ID；如果每个服务都重新生成，链路就断了，无法把一次用户操作在所有服务里的日志串起来。
4. `g` 只在 Flask 请求内有效，脱离 Web 框架（如 Kafka 消费端、定时任务）就用不了；contextvars 是标准库，任何场景都能用，且异步安全。
5. `teardown_request` 无论请求成功还是抛异常都会执行，报错的请求也能留下日志；`after_request` 在异常路径下可能不执行。
6. 不清理的话，下一个请求会复用上一个请求的 request_id，导致日志串台——排查时会把你引向完全错误的方向。
7. 不需要。`bind_request_id()` 绑定后，structlog 的 `merge_contextvars` 会自动把 request_id 加进每条日志。
8. 开发时人眼直接看，JSON 一长串反而难读；生产环境是机器采集，必须结构化。这是"谁在读日志"决定的。
</details>

## 常见问题

**日志里 request_id 一直是 `-`**
说明 `bind_request_id` 没被调用，或者请求没经过中间件（比如后台线程里的代码）。

**换了个请求，日志还是上一个的 request_id**
忘了在 `teardown_request` 里 `clear_request_id()`。

**日志太多看不清**
调 `LOG_LEVEL`：`$env:LOG_LEVEL="WARNING"` 只看警告和错误。

## 下一步

step06 会引入 **Kafka（Redpanda）做削峰解耦**：
现在投稿是同步写 Redis 的，流量一大就撑不住；改成投递到消息队列后，
由下游服务慢慢消费。到时候你会发现 contextvars 的价值——
消费者代码不在 HTTP 请求里，照样能用同样的方式打日志。
