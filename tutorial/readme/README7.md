# step07：健康检查分级 + 优雅退出

## 这一步解决什么问题

step06 留了个坑没填：

```python
@health_bp.get("/healthz")
def health():
    if not redis_client.ping():
        return jsonify({"status": "error"}), 503   # ← 问题在这里
```

**一个探针查了依赖，还用"失败"来表达。** 这在编排系统（K8s）里会被放大成事故：

> Redis 抖了 3 秒 → 所有实例的 `/healthz` 全失败 →
> 编排系统认为"进程死了" → **全部重启** →
> 重启完还要重新建连接、重新预热 → 恢复得更慢。

本来只是下游抖一下，结果演变成整个服务雪崩式重启。**故障被放大了。**

同时，服务停止时也有问题：`Ctrl+C` 或 `docker stop` 会立刻掐断进程，
正在处理到一半的请求（刚占了幂等键、还没投递消息）就被腰斩，留下不一致的状态。

## 核心概念：两个探针必须分开

| 探针 | 回答的问题 | 失败后果 | 查不查依赖 |
|---|---|---|---|
| `/healthz` liveness | 进程还活着吗？ | **重启容器** | 不查，只看自己 |
| `/readyz` readiness | 现在能接流量吗？ | **摘掉流量** | 逐个查依赖 |

一条原则记住就行：

> **liveness 只关心"我是不是卡死了"，绝不查依赖；
> readiness 才去查依赖，因为它的失败代价只是"不接流量"。**

分开之后，Redis 挂掉的正确剧本是：

```
/readyz 变 503  ->  实例被摘掉，不再接新流量
/healthz 仍 200 ->  进程不重启（重启毫无意义）
Redis 恢复      ->  /readyz 自动变回 200，流量自己回来
```

**不用重启进程，也不用人工介入。**

## 怎么跑

```powershell
# 终端 1
python run.py
# 终端 2
python consumer.py
```

启动后 `run.py` 会打印两个探针地址：

```
探针：liveness=http://127.0.0.1:8000/healthz  readiness=http://127.0.0.1:8000/readyz
```

### 亲手验证分级探活

```powershell
# 1. 正常时两个都是 200
Invoke-RestMethod http://127.0.0.1:8000/healthz
Invoke-RestMethod http://127.0.0.1:8000/readyz

# 2. 把 Redis 停掉
docker stop cp-redis

# 3. 再查：readyz 变红，healthz 依然绿  <- 这一步就是本步的全部意义
Invoke-RestMethod http://127.0.0.1:8000/healthz   # 200
Invoke-RestMethod http://127.0.0.1:8000/readyz    # 503

# 4. 恢复
docker start cp-redis
```

### 亲手验证优雅退出

Windows 没有真正的 `SIGTERM`（Python 收到它直接 `TerminateProcess`，handler 收不到），
所以代码里额外注册了 `SIGBREAK`，用 `CTRL_BREAK_EVENT` 作为等价的"请退出"信号：

```powershell
python -c "import os, signal, subprocess, time; p = subprocess.Popen([r'.\.venv\Scripts\python.exe','run.py'], creationflags=subprocess.CREATE_NEW_PROCESS_GROUP); time.sleep(4); os.kill(p.pid, signal.CTRL_BREAK_EVENT); print('exit code =', p.wait())"
```

你会看到：

```
{"event":"shutdown.signal_received","signal":"SIGBREAK"}
{"event":"shutdown.draining","grace_seconds":5}     <- 立刻变红，流量被摘走
   ... 这几秒内 /readyz 返回 503，但服务仍在应答 ...
{"event":"shutdown.complete"}                        <- flush 完毕，干净退出
exit code = 0
```

## 改了哪些文件

| 文件 | 动作 | 说明 |
|---|---|---|
| `api/health.py` | 重写 | `/healthz`（不查依赖）+ `/readyz`（逐个查依赖） |
| `app.py` | 改 | `draining` 开关、`close_resources()`、`graceful_shutdown()` |
| `run.py` | 改 | 信号接线 + 启动编排 |
| `consumer.py` | 改 | 统计处理条数；第二次信号强制退出 |
| `kafka_client.py` | 改 | `health_check()` 真实探活；`max_block_ms` 有界 |
| `config.py` | 改 | `HEALTH_CHECK_TIMEOUT_MS`、`SHUTDOWN_GRACE_SECONDS` |
| 其余 | 不动 | — |

## 三个必须讲清楚的点

### 1. 探活必须是"真"的，而且必须有界

```python
def health_check(self) -> tuple[bool, str]:
    partitions = self._producer.partitions_for(self._config.KAFKA_TOPIC)
```

为什么不能用 `is_ready()`：它只说明"生产者对象构造成功了"。
服务跑起来之后 broker 什么时候挂掉，它一无所知 —— **那不是探活，是回忆。**

为什么必须设 `max_block_ms`：`partitions_for()` 最坏会阻塞 `max_block_ms`（默认 60 秒）。
探活卡死比探活失败更糟 —— 编排系统永远等不到答复。所以我们在构造生产者时把它
压到 `HEALTH_CHECK_TIMEOUT_MS`（3 秒）：**宁可快速报"不健康"，也不要不回答。**

### 2. 优雅退出四步，一步都不能少

```
1. 标记 draining   -> /readyz 立刻 503，上游把流量摘走
2. 等待宽限期      -> 在途请求跑完，不硬掐断
3. 释放资源        -> 生产者 flush 并关闭，Redis 连接关闭
4. 退出进程        -> 退出码 0
```

**第 1 步为什么不能省**：直接退出的话，负载均衡还要等好几个探测周期才发现你没了，
这期间的流量全打到一具尸体上，全是 502/超时。主动变红是"告诉上游别再发了"，这一步是免费的。

**第 3 步为什么关键**：`KafkaProducer.close()` 会把发送缓冲区里攒着的消息全部 flush 出去。
不做这一步，`linger_ms=5` 攒批的那些消息会跟着进程一起消失。

### 3. 不能在信号处理器里 sleep（这一步踩到的真坑）

第一版我写成这样，测试直接失败：

```python
def _handle(signum, _frame):
    draining.set()
    time.sleep(config.SHUTDOWN_GRACE_SECONDS)   # ← 错
    close_resources(app)
    sys.exit(0)
```

日志看着很正常（`shutdown.draining` → 4 秒 → `shutdown.complete`），
但排水期间 `/readyz` 返回的是 **ConnectionReset**，而不是 503。

原因：**信号处理器在主线程里执行，socket 的 `accept()` 也在主线程。**
主线程一睡，服务就再也接不了新连接 —— "排水期间还能应答" 成了一句空话。

正确写法是处理器只做最轻的事，把重活丢给线程：

```python
def _handle(signum, _frame):
    def _run():
        ok = graceful_shutdown(app, config)
        os._exit(0 if ok else 1)
    threading.Thread(target=_run, daemon=True).start()
```

> 为什么用 `os._exit` 而不是 `sys.exit`：`sys.exit` 只是抛 `SystemExit`，
> 在子线程里抛只能结束那个线程，结束不了进程。此时资源已手动释放完毕、
> 日志也已 flush，直接终止是安全的。

## ⚠️ 对外行为变化

`/healthz` **不再检查 Redis**，Redis 挂掉时它仍然返回 200。
这是有意为之，也是本步的核心改动 —— 如果你的运维脚本依赖"`/healthz` 变红 = Redis 挂了"，
要改成看 `/readyz`。

## 自测练习

1. liveness 和 readiness 为什么要分开？合并成一个会造成什么后果？
2. Redis 挂了，两个探针分别返回什么？进程会被重启吗？
3. 为什么 readiness 要能"自动恢复"，而不需要重启进程？
4. 依赖探活为什么必须有超时上限？不设会怎样？
5. `is_ready()` 为什么不能当作 Kafka 的探活结果？
6. 优雅退出的第 1 步（主动变红）省掉会怎样？
7. 退出前为什么要 `close()` 生产者？不做会丢什么？
8. 为什么不能在信号处理器里 `sleep`？主线程在干什么？
9. 收到第二个退出信号时代码怎么处理？为什么要允许"强制退出"？
10. 消费者收到信号后为什么不立刻退出，而只是 `stop.set()`？

## 验证结果

21 项检查全部通过，其中关键几项：

- `/healthz` 200、`/readyz` 200 且逐个列出依赖检查结果
- **Redis 挂掉后 `/healthz` 仍 200、`/readyz` 变 503 且明确指出 `failed: ["redis"]`**
- Redis 恢复后 `/readyz` 自动变回 200（无需重启进程）
- **收到退出信号后 `/readyz` 立刻 503（draining），`/healthz` 仍 200**
- 进程退出码 0，日志完整记录 `shutdown.draining` → `shutdown.complete`
- 消费者收到信号后退出码 0，日志记录 `consumer.stopping` → `consumer.stopped`
- step06 的能力（投稿 202+queued、request_id 回显）未退化
