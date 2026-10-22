# step03：令牌桶限流 + 幂等改用 Lua

## 这一步学什么

解决 step02 留下的两个问题：

| step02 的问题 | step03 的解法 |
|---|---|
| 任何人都能疯狂刷接口，没有限制 | 加**令牌桶限流** |
| 幂等用 SETNX + GET 两次操作，有边界问题 | 改用 **Lua** 一次完成 |

新概念两个：**令牌桶算法**、**为什么必须用 Lua 保证原子性**。

## 第 1 步：跑起来

```powershell
# Redis 没起就先起它
docker run -d --name cp-redis -p 6379:6379 redis:7.4-alpine redis-server --appendonly no

cd D:\Friday\Code\Code\Project\new_project\tutorial\step03_rate_limit
..\..\.venv\Scripts\Activate.ps1
python app.py
```

启动时会打印当前限流参数：

```
限流参数：桶容量=10，每秒补充=2
```

## 为什么需要限流

接口是公开的。如果有人写个循环一秒发一千次请求：

- 你的服务 CPU / 内存被打满，正常用户也用不了
- Redis 被写爆，Redis 一挂，整个服务就 503 了

限流的作用就是：**给每个调用方一个额度，超了就拒绝，保护系统不被拖垮**。

## 令牌桶是什么

想象一个装令牌的桶：

```
        ┌─────────────┐
        │  ● ● ● ● ●  │  容量 10：最多装 10 个令牌
        │  ● ● ● ● ●  │
        └──────┬──────┘
               │ 每次请求取走 1 个
               ▼
          取到 = 放行
          没取到 = 拒绝(429)

    同时：每秒往桶里补充 2 个（补满了就不再加）
```

两个参数的含义：

| 参数 | 值 | 含义 |
|---|---|---|
| 容量 | 10 | 允许的最大**瞬时突发**。攒了 10 个令牌时，可以一口气发 10 次 |
| 每秒补充 | 2 | **稳态速率**。长期来看，这个作者平均每秒只能发 2 次 |

**为什么要"桶"而不是简单计数**：因为真实流量是突发的。用户可能 1 秒没动静，
然后连点 5 下——令牌桶允许这种合理突发，固定窗口计数则会误杀。

## 为什么必须用 Lua（这一步最关键）

令牌桶的逻辑是 5 步：

```
1. 读出当前令牌数
2. 按经过的时间补充令牌
3. 判断令牌够不够
4. 够就扣减
5. 写回新令牌数
```

**如果这 5 步分开执行**，并发下会出问题：

```
请求A 读到还剩 1 个令牌
请求B 也读到还剩 1 个令牌     <- 缝隙就在这里
请求A 扣减，放行
请求B 扣减，也放行            <- 限流失效，实际放行了 2 个
```

**放进一个 Lua 脚本里**，Redis 会把整个脚本当作一条命令原子执行，
中间不可能插入别的请求——这就是 Lua 在这类场景不可替代的原因。

> 类比：Lua 脚本就像 Redis 里的"事务 + 逻辑"，一次打包执行完。

### Lua 是什么

Redis 内置的一种**脚本语言**（语法有点像简化版 Pascal）。
注意代码里那两段字符串是 **Lua 语言，不是 Python**：

```lua
local current = redis.call('GET', KEYS[1])   -- Lua 注释用 --
if current then
    return {0, current}
end
```

- `redis.call('GET', KEYS[1])` = 在脚本里执行 Redis 命令
- `KEYS[1]` / `ARGV[1]` = 从 Python 传进来的键和参数
- `local x = ...` = 定义局部变量
- 返回值会原样传回 Python（表会变成列表）

## 幂等为什么也改成 Lua

step02 的做法是两次调用：

```
SETNX 失败  →  再 GET 一次拿首次的 post_id
```

问题：

1. 多一次网络往返
2. **边界问题**：万一在 SETNX 和 GET 之间，键刚好过期被删了，`GET` 返回空，调用方拿到一个空的 `post_id`

改成 Lua 之后，**一次调用**就同时拿到"是不是首次"和"已有的值"：

```lua
local current = redis.call('GET', KEYS[1])
if current then
    return {0, current}      -- 不是首次，直接把已有值带回来
end
...
return {1, ARGV[1]}
```

两个问题一起消失。这就是 step02 末尾那道思考题的答案。

## 一个容易忽略的顺序问题

代码里的顺序是：**先幂等，再限流**。

为什么不让限流排在前面？

- 重复提交是同一次业务意图的**重试**，它不该消耗令牌
- 被幂等拦下的请求根本不会走到后面，也就不会给系统增加负担

如果反过来，用户网络差点了两次提交，第二次会被限流误伤。

## 亲手验证

```powershell
# 1. 正常投稿（注意看返回的 remaining_tokens 在递减）
curl -Method POST http://127.0.0.1:8000/api/v1/posts `
  -ContentType "application/json" `
  -Body '{"client_msg_id":"msg-1","author_id":"author-A","title":"第一篇","content":"正文"}'

# 2. 重复提交（幂等，不会被限流消耗）
curl -Method POST http://127.0.0.1:8000/api/v1/posts `
  -ContentType "application/json" `
  -Body '{"client_msg_id":"msg-1","author_id":"author-A","title":"第一篇","content":"正文"}'
# 期望：status=duplicated，post_id 与第一次相同

# 3. 疯狂刷接口（换个新 client_msg_id 连续发十几次）
#    发到第 11 次左右会看到 429
curl -Method POST http://127.0.0.1:8000/api/v1/posts `
  -ContentType "application/json" `
  -Body '{"client_msg_id":"msg-2","author_id":"author-A","title":"刷接口","content":"正文"}'

# 4. 等 3 秒再发一次 —— 令牌补充了，又能成功

# 5. 换个作者 author-B，立刻就能成功（每个作者有独立的桶）
```

**直接观察 Redis 里的桶**：

```powershell
docker exec cp-redis redis-cli hgetall rl:author-A
# tokens = 剩余令牌数
# ts     = 上次访问的时间戳（毫秒）
```

## 自测练习

1. 令牌桶的"容量"和"每秒补充"分别控制什么？
2. 为什么令牌桶比固定窗口计数更适合真实流量？
3. 如果不用 Lua，5 步分开执行，并发下会发生什么？
4. 为什么幂等要放在限流之前？反过来会怎样？
5. 被限流的请求返回什么状态码？为什么用这个？
6. `register_script` 有什么好处？
7. 桶满了之后继续等待，令牌会超过容量吗？为什么？

<details>
<summary>答案</summary>

1. 容量控制最大瞬时突发量；每秒补充控制长期平均速率。
2. 真实流量是突发的（用户可能停一会儿再连点几下）。令牌桶允许攒起来的令牌一次性使用，固定窗口计数会在窗口边界误杀正常请求。
3. 多个请求可能同时读到"还剩 1 个令牌"，然后都判定放行，实际放行数量超过限额，限流失效。
4. 重复提交是重试，不该消耗令牌；反过来会让网络差的用户第二次提交被限流误伤。
5. 429（Too Many Requests），这是 HTTP 标准里专门用于"请求频率超限"的状态码，调用方看到它可以自动退避重试。
6. 预先把脚本注册到 Redis，后续调用只发脚本的哈希值，不用每次都把整段脚本文本传过去，省带宽也省解析时间。
7. 不会。脚本里有 `math.min(capacity, ...)`，补充时上限就是桶容量。
</details>

## 常见问题

**一直返回 429 怎么办**
等几秒让令牌补充，或者清掉这个桶：
`docker exec cp-redis redis-cli del rl:author-A`

**改了参数没生效**
改完要 `Ctrl+C` 停掉服务再 `python app.py`（`use_reloader=False`）。

**`remaining_tokens` 一直是 0**
说明桶空了。等待补充，或调大 `RATE_REFILL_PER_SEC`。

## 关于多实例

现在所有实例共享同一个 Redis，所以限流是"全局"的——这是好消息。
但要注意：本实现的**时间来自应用服务器**（`time.time()`），
严格来说多机时钟不完全一致。生产环境更严谨的做法是在 Lua 里用 Redis 服务器时间。
入门阶段知道有这个方向即可。

## 下一步

step04 会把代码**拆分组织**：现在所有东西都堆在一个文件里、参数硬编码在代码中，
我们会引入蓝图（Blueprint）和配置分层，让代码结构接近真实项目。
