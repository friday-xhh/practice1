# content-platform 构建教程（从零到跑通，一步步讲清楚）

> 适用对象：Python 基础薄弱、想弄懂 "微服务项目是怎么一步步搭出来" 的学习者。
> 这份教程按
>
> **真实构建顺序**
>
> 讲解：先想清楚做什么 → 准备环境 → 打地基（公共层）→ 盖楼（服务）→ 质检（测试）→ 验收（跑通）。
> 所有文件、命令、代码都来自当前项目真实内容，你可以边看边对照。



***

## 第 0 步：先想清楚 —— 这个项目到底在做什么？

### 0.1 业务故事（用大白话讲）

想象一个 UGC 平台（类似小红书 / 知乎）：每天用户会上传海量稿件（帖子 / 视频 / 图片）。

平台需要做到两件事：



1. **毫秒级受理**：用户点 "发布"，系统要立刻给回应（"收到了"）；

2. **秒级可检索**：发布后很快能被别人搜到、看到。

但稿件不能直接放出去 —— 得先**审核**（有没有违规内容），审核通过才能公开。

于是系统被拆成多个**微服务**（每个服务只干一件事）：



| 服务             | 职责                         | 本轮做没做 |
| -------------- | -------------------------- | ----- |
| ingest-service | 受理投稿（本项目的核心）               | ✅ 已完成 |
| audit-service  | 消费 Kafka，做规则 / 模型审核        | ❌ 下一轮 |
| index-service  | 写入 Elasticsearch + MongoDB | ❌ 下一轮 |
| search-service | 检索 + 缓存                    | ❌ 下一轮 |

**这一轮你只需要看懂 ingest-service 一个服务**，它的完整链路是：



```
用户 POST /api/v1/posts

&#x20;       │

&#x20;       ▼

Flask 接住请求

&#x20;  │  1. 参数校验（格式对不对）

&#x20;  │  2. 幂等（同一篇稿子不能重复受理）

&#x20;  │  3. 限流（防止有人疯狂刷接口）

&#x20;  ▼

投递到 Kafka（Redpanda）→ 以后 audit/index 服务从里面取
```

### 0.2 三个 "为什么"（面试最爱问，也是理解关键）



1. **为什么用 Redis 做幂等和限流？**

   幂等 = "同一件事做两次等于做一次"。用户网络差时可能重复点击提交，系统要能认出 "这稿子我见过了"，返回第一次的结果而不是再存一遍。

   限流 = 防止洪峰打爆系统，用 "令牌桶" 控制单位时间最多放行多少个请求。

   这两个操作都需要**原子性**（并发下不出错），所以用 Redis 的 Lua 脚本一次执行完。

2. **为什么用 Kafka（Redpanda）而不是直接把稿件存数据库？**

   因为写数据库很慢，而投稿是海量高频的。Kafka 是个 "高速缓冲管道"：先把消息快速收下，下游服务慢慢消费。这叫**削峰 + 解耦**（削掉流量高峰、解耦上下游）。

3. **为什么服务要分层（common /app/api /tests）？**

   因为 "写代码" 和 "写能维护的代码" 是两回事。分层让每一层职责单一：

* 通用能力（配置、日志、Redis、Kafka 客户端）放 `common/`，以后所有服务共用；

* 业务逻辑（校验、幂等、限流顺序）放 `app/services.py`，不掺和 HTTP 细节；

* HTTP 路由（哪个 URL 调哪个函数）放 `app/api/`，很薄；

* 测试放 `tests/`，用 "假对象" 模拟 Redis/Kafka，不依赖真实中间件。



***

## 第 1 步：准备环境（地基）

### 1.1 需要装什么



| 工具                     | 用途                      | 本机安装                    |
| ---------------------- | ----------------------- | ----------------------- |
| Python 3.11+（本机是 3.12） | 运行环境                    | 官网下载安装                  |
| Docker Desktop         | 跑 Redis / Redpanda 等中间件 | 官网下载（Windows 用 WSL2 后端） |
| Git                    | 版本管理                    | 可选                      |

### 1.2 创建虚拟环境（venv）

为什么需要？每个项目依赖的包版本可能冲突，虚拟环境 = 给这个项目单独开一间 "隔离小屋"。



```
cd D:\Friday\Code\Code\Project\new\_project

python -m venv .venv          # 创建虚拟环境目录 .venv

.\\.venv\Scripts\Activate.ps1  # 激活它（提示符前面会出现 (.venv)）
```

> 项目里已写好一键脚本 
>
> `scripts/bootstrap.ps1`
>
> ，它会自动创建 venv、装依赖、生成 .env：
>

```
Set-ExecutionPolicy -Scope Process RemoteSigned   # 允许本会话运行 ps1 脚本

.\scripts\bootstrap.ps1
```

### 1.3 三个依赖文件（为什么拆三个）



| 文件                      | 内容                                                             | 什么时候用     |
| ----------------------- | -------------------------------------------------------------- | --------- |
| `requirements.txt`      | 运行时依赖（Flask、pydantic、structlog、redis、confluent-kafka）          | 本地开发 + 容器 |
| `requirements-dev.txt`  | 开发工具（pytest、ruff、mypy），第 1 行 `-r requirements.txt` 表示 "包含上面那份" | 本地开发      |
| `requirements-prod.txt` | 生产依赖（多了 gunicorn + gevent，Linux 专用）                            | 生产部署      |

关键认知：**Windows 上不要装 requirements-prod.txt**——gunicorn 依赖 `fcntl`，Windows 没有这个模块，装了也跑不起来。项目在 Windows 上用 `python run.py`，Linux 生产环境才用 gunicorn。

### 1.4 配置文件 .env

代码里所有可变的东西（数据库地址、端口、限流参数）都不硬编码，放进 `.env`：



```
APP\_ENV=development

HOST=0.0.0.0

PORT=8000

KAFKA\_BOOTSTRAP\_SERVERS=localhost:9092

REDIS\_URL=redis://localhost:6379/0

RATE\_LIMIT\_CAPACITY=50

RATE\_LIMIT\_REFILL\_PER\_SEC=20
```

`.env.example` 是模板，复制一份改名为 `.env` 即可。`.env`**&#x20;含本机敏感信息，已写进&#x20;**`.gitignore`**，不进 Git。**

### 1.5 中间件编排文件 docker-compose.yaml

本地跑 Redis / Redpanda / Elasticsearch / MongoDB 全部靠这一个文件：



```
docker compose up -d      # 后台启动全部中间件

docker compose ps         # 查看状态，等全部 healthy
```

这个文件里藏了三个**本机真实踩过的坑**（面试讲出来很加分）：



| 现象                                   | 根因                                                 | 解决（已写进 compose）                               |
| ------------------------------------ | -------------------------------------------------- | --------------------------------------------- |
| Redpanda 启动报内存不足退出                   | `--memory=1G` 与容器上限相同，Seastar 要求足额物理内存             | 需求降到 `512M`，上限放宽到 `1536M`                     |
| Redpanda 报 `Operation not permitted` | Seastar 调用 `perf_event_open()` 被 Docker seccomp 拦截 | `cap_add: [SYS_ADMIN]` + `seccomp:unconfined` |
| MongoDB 容器 exited (14)               | WSL2 的线程 /pids 限制                                  | 调大 `ulimits`（本轮不影响 ingest，下一轮处理）              |



***

## 第 2 步：打地基 —— 公共基础层 `common/`（4 个文件）

这一层是所有服务的 "地基"，包括 4 个模块。逐个讲。

### 2.1 `common/config.py` —— 分层配置

**解决的问题**：开发环境（本机）和生产环境（线上）的配置不一样，怎么管理？

**方案**：三个类，一层套一层：



```
BaseAppSettings          ← 通用默认值（所有环境都一样）

&#x20; ├── DevelopmentSettings  ← 开发：DEBUG 开、日志明文、限流阈值低（方便触发）

&#x20; └── ProductionSettings   ← 生产：DEBUG 关、JSON 日志、限流阈值高、连接池大
```



```
@lru\_cache(maxsize=8)

def get\_settings(env: str | None = None) -> BaseAppSettings:

&#x20;   resolved = (env or os.getenv("APP\_ENV") or "development").strip().lower()

&#x20;   return \_SETTINGS\_MAP.get(resolved, DevelopmentSettings)()
```

三个知识点：



* `pydantic-settings` 的 `BaseSettings` 会自动从 `.env` 和环境变量读配置，**环境变量优先级最高**（K8s 注入配置就是这么来的）；

* `lru_cache` 缓存配置对象，避免每次用都重新读文件（单测里可以用 `cache_clear()` 重置）；

* `kafka_producer_config()` 把 Kafka 调优参数集中成方法，面试可讲：`linger.ms` 攒批、`compression.type=lz4` 压缩、`acks=1` 吞吐与可靠性折中。

### 2.2 `common/logging.py` —— 结构化日志 + request\_id 透传

**解决的问题**：微服务排错难。日志要能被机器采集（ELK/Loki），并且一条请求跨多个服务时能串起来。

**方案**：structlog 输出 JSON 格式日志 + `contextvars` 绑定 request\_id。



```
\_request\_id\_var: ContextVar\[str] = ContextVar("request\_id", default="-")

def bind\_request\_id(request\_id: str) -> None:

&#x20;   \_request\_id\_var.set(request\_id)

&#x20;   structlog.contextvars.bind\_contextvars(request\_id=request\_id)
```

大白话理解 `contextvars`：每个请求进来时，把 request\_id 绑定到 "当前这个请求的上下文"，之后**这个请求处理过程中**打的所有日志都会自动带上它；请求结束调用 `clear_context()` 清掉，防止串到下一个请求。

同时 Kafka 消息头也带 `request_id`（见 services.py 里的 `headers={"request_id": trace_id}`），下游服务继续透传，实现跨服务链路串联。

### 2.3 `common/redis_client.py` —— 幂等键 + 令牌桶（两块 Lua 脚本）

**解决的问题**：Redis 的 GET 和 SET 不是原子操作，并发下两个请求可能同时判定 "我是第一次"，造成重复投稿。

**方案**：把判断 + 写入合成一个 **Lua 脚本**，让 Redis 一次性原子执行。

**脚本 1：幂等**（`GET` 有值就不写入，返回已有值）



```
local current = redis.call('GET', KEYS\[1])

if current then

&#x20;   return {0, current}          -- 已有值：返回 0（不是首次）+ 旧值

end

redis.call('SET', KEYS\[1], ARGV\[1], 'PX', tonumber(ARGV\[2]))

return {1, ARGV\[1]}              -- 首次：写入并返回 1
```

**脚本 2：令牌桶限流**（按时间补充令牌，有令牌才放行）



```
\-- 简化的核心逻辑：tokens 按 (now - 上次时间) \* 补充速率 增加，最多到 capacity

\-- 如果 tokens >= cost，扣减并 allowed=1；否则 allowed=0
```

调用方式（redis-py 的 `eval`）：



```
result = self.\_client.eval(\_LUA\_IDEMPOTENT\_ACQUIRE, 1, key, value, str(ttl\_seconds \* 1000))
```

**Redis 不可用时的设计**：所有方法抛 `RedisUnavailable`，由上层决定降级 ——ingest 的选择是 "写链路直接失败返回 503"，不静默丢数据。

### 2.4 `common/kafka_client.py` —— Producer / Consumer 工厂

**解决的问题**：投递消息给 Kafka 时，不能阻塞请求线程；进程退出时不能丢消息。

**方案**：



* `produce()` 是**异步**的，立即返回，投递结果由 `delivery_callback` 回调统计（`delivered` / `failed` 两个计数器）；

* 本地队列满（`BufferError`）时先 `poll(1)` 排出积压再重试一次；

* `flush()` 在优雅退出时调用，等队列清空才关连接 —— 这就是 "优雅退出不丢消息"；

* `health()` 用 `list_topics` 探测连通性，供 `/readyz` 使用；

* Consumer 部分供后续 audit/index 服务及本机 `consume_demo.py` 使用。



***

## 第 3 步：盖楼 ——ingest-service（Flask 服务）

目录结构（只看业务代码）：



```
services/ingest-service/

├── run.py                    启动入口（python run.py）

├── gunicorn.conf.py          生产 WSGI 配置（Linux 用）

└── app/

&#x20;   ├── \_\_init\_\_.py           应用工厂 create\_app() + 中间件

&#x20;   ├── extensions.py         外部依赖（Redis/Kafka）生命周期管理

&#x20;   ├── errors.py             统一错误响应

&#x20;   ├── schemas.py            pydantic 数据模型

&#x20;   ├── services.py           投稿核心业务链路

&#x20;   └── api/

&#x20;       ├── health.py         健康检查路由

&#x20;       └── submit.py         投稿路由
```

按依赖顺序逐个讲（从最底层的 "零件" 到最上层的 "入口"）。

### 3.1 `app/errors.py` —— 统一错误响应（先定规矩）

**为什么先写它**：全项目约定：所有非 2xx 响应都是同一个格式，调用方只解析一处。



```
{"error": {"code": "rate\_limited", "message": "...", "request\_id": "...", "details": {...}}}
```

实现 = 一个 `ApiError` 基类 + 几个子类：



```
class ApiError(Exception):

&#x20;   status\_code = 500

&#x20;   code = "internal\_error"

class BadRequestError(ApiError):        status\_code = 400; code = "bad\_request"

class PayloadTooLargeError(ApiError):   status\_code = 413; code = "payload\_too\_large"

class RateLimitedError(ApiError):       status\_code = 429; code = "rate\_limited"

class DependencyUnavailableError(ApiError): status\_code = 503; code = "dependency\_unavailable"
```

再用 Flask 的 `errorhandler` 把异常统一转成 JSON。注意第 5 个 "兜底"：任何未捕获异常返回 500，**不向调用方泄漏堆栈**。

### 3.2 `app/schemas.py` —— pydantic 数据模型

**为什么不用手写 if 校验**：字段一变，校验逻辑自动跟随；还能导出 OpenAPI 文档。



```
class SubmitPostRequest(BaseModel):

&#x20;   client\_msg\_id: str = Field(min\_length=8, max\_length=64)

&#x20;   author\_id: str = Field(min\_length=1, max\_length=64)

&#x20;   title: str = Field(min\_length=1, max\_length=120)

&#x20;   content: str = Field(min\_length=1, max\_length=20000)

&#x20;   content\_type: ContentType = "article"     # Literal\["article","video","image"]

&#x20;   tags: list\[str] = Field(default\_factory=list, max\_length=10)

&#x20;   publish\_at: datetime | None = None
```

关键点：`client_msg_id`**&#x20;必须由调用方生成**（不能服务端生成），否则 "重试" 就成了 "新请求"，幂等无从谈起。它就是要被幂等拦截的锚点。

### 3.3 `app/extensions.py` —— 依赖生命周期（Redis / Kafka）

**启动策略（best-effort）**：Redis 或 Kafka 连不上，服务**仍然启动**，但标记 `degraded`。这样容器不会被 K8s 反复重启，同时 `/readyz` 返回 503 让流量被摘掉。



```
try:

&#x20;   ctx.redis = RedisClient(settings)

&#x20;   if ctx.redis.ping():

&#x20;       logger.info("dependency.ready", name="redis", ...)

&#x20;   else:

&#x20;       raise RuntimeError("ping failed")

except Exception as exc:

&#x20;   ctx.degraded.append("redis")

&#x20;   logger.error("dependency.unavailable", name="redis", error=str(exc))
```

**退出策略（优雅）**：`atexit` 注册 `shutdown_extensions` —— 先 flush Kafka 队列（不丢消息），再关 Redis 连接池。对应 K8s 的 preStop 语义。

### 3.4 `app/services.py` —— 核心业务链路（本项目灵魂）

链路顺序（面试必问 "为什么这样排"）：



```
1\. 依赖可用性检查  →  2. 幂等  →  3. 限流  →  4. 投递 Kafka
```

原因：



1. **幂等在限流之前**：重试是预期行为，不该消耗令牌；被幂等拦下的请求不会进下游，天然不放大流量；

2. **投递放最后**：只有真正需要处理的消息才进 Kafka，Kafka 是 "削峰 + 解耦"，不是 "记录所有请求"；

3. **返回 202 而非 200**：202 表达 "已受理、尚未处理完"，审核结果由下游异步产出。

核心代码（节选，去掉了日志）：



```
def submit(self, payload: SubmitPostRequest) -> SubmitResponse:

&#x20;   self.\_ensure\_dependencies()                    # 1. 依赖检查，缺 Redis/Kafka 抛 503

&#x20;   trace\_id = get\_request\_id()

&#x20;   new\_post\_id = uuid.uuid4().hex

&#x20;   first, existing = redis.acquire\_idempotency(   # 2. 幂等：抢键

&#x20;       \_idempotency\_key(payload.client\_msg\_id),

&#x20;       new\_post\_id,

&#x20;       settings.IDEMPOTENCY\_TTL\_SECONDS,

&#x20;   )

&#x20;   if not first:                                  #    已存在 → 返回旧 post\_id + duplicated

&#x20;       return SubmitResponse(..., status="duplicated", post\_id=existing, ...)

&#x20;   allowed, remaining = redis.allow\_request(      # 3. 限流：令牌桶

&#x20;       \_rate\_limit\_key(payload.author\_id), cost=settings.RATE\_LIMIT\_COST)

&#x20;   if not allowed:

&#x20;       raise RateLimitedError("too many submissions", ...)   # 429

&#x20;   event = { "post\_id": post\_id, "author\_id": payload.author\_id, ... }

&#x20;   producer.publish(event, key=payload.author\_id,             # 4. 投递

&#x20;                    headers={"request\_id": trace\_id})

&#x20;   return SubmitResponse(..., status="accepted", ...)         # 202
```

两个设计细节（都能讲）：



* 幂等键 = `ingest:idem:{client_msg_id}`，限流键 = `ingest:rl:{author_id}`，**用前缀命名空间隔离**，避免不同用途的键撞车；

* Kafka 分区键用 `author_id`：同一作者的消息落在同一分区，保证 "该作者的稿件按顺序被消费"。

### 3.5 `app/api/` —— 路由层（很薄）

`api/__init__.py` 里一句话点明分层原则：**路由层只做参数绑定与响应拼装，业务逻辑下沉到 services.py**（这样业务可以在脱离 Web 框架的情况下做单测）。

`api/health.py` —— 三个运维端点：



| 端点               | 类型        | 语义                                         |
| ---------------- | --------- | ------------------------------------------ |
| `/healthz`       | liveness  | 进程活着就 200，失败则 K8s **重启容器**                 |
| `/readyz`        | readiness | 依赖（Redis+Kafka）可用才 200，失败则 K8s **摘流量但不重启** |
| `/metrics/local` | 自观测       | Kafka 投递统计（生产换 Prometheus exporter）        |

为什么两者必须分开：Redis 抖动时重启容器毫无意义（新容器起来还是连不上），正确做法是摘流量、等恢复。

`api/submit.py` —— POST /api/v1/posts：



1. `_parse_body()`：要求 JSON、body 上限 1MB（防超大 body 打爆内存）、非法 JSON 返回 400；

2. `SubmitPostRequest(**body)` 用 pydantic 校验，失败转 400 + 字段级错误详情；

3. 调 `PostIngestService(ctx).submit(payload)`，成功返回 202。

### 3.6 `app/__init__.py` —— 应用工厂 + 中间件

**应用工厂模式**（`create_app(settings)`）：一个函数把整个应用组装起来，好处是测试时可以传入不同的 settings、生产 / 开发共用一份代码。

每个请求的固定动作（中间件）：



```
@app.before\_request

def \_assign\_request\_id():

&#x20;   request\_id = request.headers.get("X-Request-ID") or uuid.uuid4().hex

&#x20;   g.request\_id = request\_id

&#x20;   g.started\_at = time.perf\_counter()

&#x20;   bind\_request\_id(request\_id)

@app.after\_request

def \_echo\_request\_id(response):

&#x20;   response.headers\["X-Request-ID"] = getattr(g, "request\_id", "-")   # 回显给调用方

&#x20;   return response

@app.teardown\_request

def \_log\_access(exc):

&#x20;   \# 记访问日志（含耗时），然后 clear\_context() 防泄漏
```

### 3.7 `run.py` —— 启动入口



```
cd services/ingest-service

python run.py            # 本机开发（Windows 用这个）
```

做的三件事：



1. **修 sys.path**：把仓库根目录和当前服务目录加进导入路径，让 `common` 包可被导入（`parents[2]` 往上跳两级到仓库根）；

2. `build_app()`：供 gunicorn 调用的工厂函数；

3. `install_graceful_shutdown()`：注册 SIGTERM 处理器 —— K8s 滚动更新先发 SIGTERM，收到后 flush Kafka + 关连接再退出（否则会被 SIGKILL 强杀、丢消息）。

生产环境（Linux）：`gunicorn -c gunicorn.conf.py "run:build_app()"`。

`gunicorn.conf.py` 里 worker 选择：本服务大量等待 Redis/Kafka（IO 密集），用 `gevent` worker，单 worker 可并发处理成百上千连接。



***

## 第 4 步：质检 —— 测试与静态检查

### 4.1 测试的核心思路：不依赖真实中间件

单元测试要快、要能离线跑，所以用 **Fake 替身**（`tests/conftest.py`）：



```
class FakeRedis:

&#x20;   def acquire\_idempotency(self, key, value, ttl\_seconds):

&#x20;       self.acquire\_calls.append(key)

&#x20;       return (self.\_first, self.\_stored or value)   # 可控：首次/非首次

class FakeProducer:

&#x20;   def publish(self, payload, key, headers=None):

&#x20;       self.published.append({...})                  # 记录投递内容

def \_patch\_dependencies(app, \*, first=True, allowed=True, stored="", drop=()):

&#x20;   ctx = app.extensions\["context"]

&#x20;   ctx.redis = None if "redis" in drop else FakeRedis(first=first, ...)

&#x20;   ctx.producer = None if "kafka" in drop else FakeProducer()
```

测试里直接替换 `app.extensions["context"]` 里的槽位，就能模拟 "Redis 挂了""Kafka 挂了 ""幂等命中"" 限流触发 " 等各种场景。

### 4.2 14 个用例覆盖的分支



| 用例组  | 验证点                                                     |
| ---- | ------------------------------------------------------- |
| 正常受理 | 返回 202、post\_id 非空、消息已投递且分区键 = author\_id               |
| 幂等   | 重复提交返回同一 post\_id、status=duplicated、不重复投递               |
| 限流   | 返回 429、error.code=rate\_limited、不投递                     |
| 依赖缺失 | Redis/Kafka 缺失返回 503；/readyz 503；/healthz 仍然 200        |
| 参数校验 | 短 client\_msg\_id、非法 content\_type、空 title、超长 tag → 400 |

运行：



```
cd services/ingest-service

pytest tests -v        # 14 passed
```

### 4.3 静态检查



```
ruff check .           # 代码规范（All checks passed）

mypy common services/ingest-service/app --ignore-missing-imports   # 类型检查（no issues）
```



***

## 第 5 步：验收 —— 端到端跑通

按顺序执行（假设已做过第 1 步）：



```
\# 1. 启动中间件（首次拉镜像会比较久）

docker compose up -d

docker compose ps                    # 等 Redpanda/Redis 变 healthy

\# 2. 启动服务（新开一个终端）

cd services/ingest-service

python run.py                        # 看到 "server.starting" 和 "app.ready" 即可

\# 3. 冒烟测试（再开一个终端）

.\scripts\smoke\_test.ps1             # 期望 6 PASS / 0 FAIL

\# 4. 从 Kafka 实际消费，验证消息真的落地

python scripts/consume\_demo.py --max 5

\# 5. 手工体验一下

curl http://localhost:8000/healthz
```

本机实测结果（README 第 7 节）：6 PASS / 0 FAIL，幂等返回同一 post\_id，限流触发 429，`consume_demo.py` 实消费到 3 条消息（含 request\_id 消息头）。



***

## 第 6 步：常见坑速查（本机真实踩过）



| 现象                                                                  | 原因                          | 解法                                         |
| ------------------------------------------------------------------- | --------------------------- | ------------------------------------------ |
| `x509: certificate is valid for *.facebook.com, not auth.docker.io` | Docker 出口 TLS 被代理劫持         | Docker Desktop 配置 registry-mirrors（阿里云加速器） |
| PowerShell 冒烟脚本报 "意外的标记 }"                                          | 脚本无 BOM，PS 5.1 按 GBK 解析中文乱码 | 脚本保存为 UTF-8 with BOM                       |
| Redpanda 内存 / 权限报错                                                  | 见第 1.5 节                    | 已写入 compose                                |
| MongoDB exited(14)                                                  | WSL2 线程限制                   | 调大 ulimits；本轮 ingest 不依赖它，下一轮处理            |



***

## 第 7 步：接下来你可以做什么

按 README 的迭代计划，这个骨架的下一步是：



* [ ] `audit-service`：消费 Kafka，规则引擎 + 布隆过滤器去重

* [ ] `index-service`：批量写 Elasticsearch + MongoDB 审核快照

* [ ] `search-service`（FastAPI）：ES 检索 + Redis 缓存三板斧（穿透 / 击穿 / 雪崩）

* [ ] 秒杀专项：库存分段 + Lua 原子预扣

* [ ] Locust 压测报告 + K8s Helm Chart + CI 流水线

每完成一个，都是在往 "能讲出完整故事" 的面试项目上走一步。



***

## 附：学习自检清单

读完这份教程后，试着不看代码回答：



1. 幂等为什么要用 Lua 脚本？只用 GET 再 SET 有什么问题？

2. 为什么幂等要在限流之前？

3. 为什么返回 202 而不是 200？

4. liveness 和 readiness 为什么要分开？分别对应什么？

5. 为什么 Windows 用 run.py、Linux 用 gunicorn？

6. 为什么 Kafka 分区键用 author\_id？

7. 测试里怎么做到不依赖真实 Redis/Kafka？

8. request\_id 是怎么跨服务透传的？

9. `client_msg_id` 为什么必须由调用方生成？

10. 优雅退出时做了什么，为什么？

答不出来就回到对应章节再看一遍。构建 + 能讲清设计理由，才是这个项目真正的价值。