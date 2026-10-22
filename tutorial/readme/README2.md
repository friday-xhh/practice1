# step02：用 Redis 存数据 + 实现幂等

## 这一步学什么

解决 step01 留下的两个问题：

| step01 的问题 | step02 的解法 |
|---|---|
| 数据存在内存字典，服务重启就没了 | 改存 Redis |
| 用户重复点提交，会被当成两篇稿子 | 加幂等（相同 `client_msg_id` 只受理一次） |

新概念只有两个：**Redis 是什么**、**幂等是什么**。

## 第 0 步：先把 Redis 跑起来

这一步必须有个 Redis 才能跑。一条命令：

```powershell
docker run -d --name cp-redis -p 6379:6379 redis:7.4-alpine redis-server --appendonly no
```

确认它活着：

```powershell
docker exec cp-redis redis-cli ping      # 应该输出 PONG
docker exec cp-redis redis-cli dbsize    # 看里面有多少个键，刚起来是 0
```

> 想直接看 Redis 里存了什么：
> `docker exec cp-redis redis-cli keys "*"`

## 第 1 步：装依赖并启动服务

```powershell
cd D:\Friday\Code\Code\Project\new_project
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt        # 会装 redis

cd tutorial\step02_redis_idempotent
python app.py
```

看到 `Redis 连接正常，服务地址 http://127.0.0.1:8000` 就成功了。

如果你**故意不开 Redis** 就运行，会看到一段明确的提示并直接退出——这是代码里
`ensure_redis_alive()` 干的，专门为了不让新手面对莫名其妙的连接报错。

## 接口变化（对比 step01）

| 方法 | 路径 | step02 的变化 |
|---|---|---|
| POST | `/api/v1/posts` | 首次提交：202 + `status:accepted`<br>**重复提交：200 + `status:duplicated` + 首次的 `post_id`** |
| GET | `/posts/<post_id>` | 改为从 Redis 读 |
| GET | `/healthz` | 多检查一次 Redis，连不上返回 **503** |

## 核心讲解

### Redis 是什么

一个**跑在内存里、速度极快**的键值对仓库。它独立于你的服务进程，所以：

- 服务重启，数据还在（这是本步最直观的收获）
- 多个服务实例可以共享同一份数据（这是后面做多实例部署的前提）

可以把它想成一个超快的字典：`set(key, value)` 存，`get(key)` 取。

### 为什么幂等必须用 Redis（而不是内存变量）

幂等要记住"这个 `client_msg_id` 我处理过"。这个"记住"必须放在**服务之外**：

- 放内存字典：服务重启就忘了，而且你有 3 个实例时，另外 2 个实例根本不知道
- 放 Redis：所有实例共享，重启也不丢

### SETNX 为什么能保证幂等

```python
is_first = redis_client.set(idem_key, post_id, nx=True, ex=86400)
```

`nx=True` 就是 SETNX：**键不存在时才写入**。

关键在于它是 **Redis 的单条原子命令**——一万个请求同时进来，也只有一个能写成功，
其余全部收到"没抢到"。这就是幂等成立的根本原因。

> 反例：如果写成"先 `GET` 看看有没有，没有再 `SET`"，这两步之间有时间缝隙，
> 两个请求可能同时看到"没有"，然后都写入 —— 幂等就失效了。

### 键的设计：为什么要加前缀

```
post:<post_id>        存稿件内容
idem:<client_msg_id>  存幂等标记
```

加前缀是为了**隔离不同用途的键**。否则万一 `post_id` 和 `client_msg_id` 撞了同一个值，
数据就串了。这是 Redis 使用的基本卫生习惯。

## 亲手验证：重启后数据还在（这一步最值得做）

```powershell
# 1. 提交一篇稿件，记住返回的 post_id
curl -Method POST http://127.0.0.1:8000/api/v1/posts `
  -ContentType "application/json" `
  -Body '{"client_msg_id":"msg-0001","author_id":"author-1","title":"第一篇","content":"正文"}'

# 2. 用同样的 client_msg_id 再提交一次 —— 观察返回什么
curl -Method POST http://127.0.0.1:8000/api/v1/posts `
  -ContentType "application/json" `
  -Body '{"client_msg_id":"msg-0001","author_id":"author-1","title":"第一篇","content":"正文"}'
# 期望：post_id 和第一次完全一样，status=duplicated

# 3. 看看 Redis 里现在有什么
docker exec cp-redis redis-cli keys "*"

# 4. 【关键】把服务 Ctrl+C 停掉，再重新启动
python app.py

# 5. 用第 1 步的 post_id 查询 —— 数据还在！
curl http://127.0.0.1:8000/posts/<第1步的post_id>

# 6. 停掉 Redis 再访问 healthz
docker stop cp-redis
curl http://127.0.0.1:8000/healthz      # 期望 503
docker start cp-redis                    # 记得起回来
```

## 思考题：这套实现有个瑕疵（很重要）

重复提交时，代码做了**两次** Redis 操作：

```
SETNX 失败  →  再 GET 一次拿首次的 post_id
```

问题：

1. 多了一次网络往返（Redis 再快也是网络调用）
2. **极端情况**：如果 `SETNX` 失败后、去 `GET` 之前，这个键刚好过期被删了，
   `GET` 会返回 `None`，调用方就拿到一个空的 `post_id`

怎么解决？把"判断 + 取回已有值"合成**一个** Redis 操作一次性完成——
这就是 **Lua 脚本**的作用（后面的步骤会讲，完整项目里用的就是它）。

还有一个真实隐患留给你想：

> 如果 `SETNX` 成功了，但接下来存稿件时 Redis 挂了，会怎样？
> 这个 `client_msg_id` 还能重试成功吗？

## 自测练习

1. 为什么幂等标记不能存在服务的内存里？
2. `SETNX` 的哪个特性保证了并发下只有一个请求能成功？
3. 如果写成"先 GET 没值再 SET"，并发下会发生什么？
4. 重复提交时返回的状态码是 202 还是 200？为什么不同？
5. `decode_responses=True` 不加会怎样？
6. 为什么 `/healthz` 在 Redis 挂掉时要返回 503 而不是 200？

<details>
<summary>答案</summary>

1. 服务重启会丢失；多个实例之间也无法共享（各自内存独立）。
2. 它是 Redis 的**原子命令**——判断和写入是一次完成的，不存在中间状态。
3. 两个请求可能同时 GET 到"没有"，然后都执行 SET，导致重复处理，幂等失效。
4. 是 200。首次是 202（已受理、还没处理完）；重复提交时结果已经确定（直接返回首次结果），用 200 表示"这就是你要的结果"。
5. Redis 返回的是 `bytes`（如 `b'abc'`），你需要手动 `.decode("utf-8")`，否则到处是 `b''` 前缀。
6. 503 表示"服务暂时不可用"，运维系统（如 K8s）看到它会把流量从该实例摘走。如果返回 200，流量会继续打到一个已经不能干活的服务上。
</details>

## 常见问题

**`ConnectionError: Error 10061 connecting to localhost:6379`**
Redis 没启动。按第 0 步的命令启动它。

**重复提交返回的 post_id 是 null**
说明 `GET` 没取到值。检查是不是手动清过 Redis（`redis-cli flushall`），
或者幂等键已过期（默认保留一天）。

**改了代码没生效**
`use_reloader=False`，改完要 `Ctrl+C` 停掉再 `python app.py`。

## 下一步

step03 会引入**令牌桶限流**：现在任何人都能疯狂刷接口，
我们要给每个作者限制"每秒最多提交几次"——同样用 Redis 实现，
并且会用到 Lua 脚本（正好解决本步留下的那个瑕疵）。
