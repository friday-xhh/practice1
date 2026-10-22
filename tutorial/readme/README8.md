# step08：类型注解 + 单元测试

前七步一直在"加功能"。这一步不加任何功能，只做一件事：

> **让前面写的东西不再那么容易坏。**

## 为什么需要这一步

回头看这段代码（step07 之前的写法）：

```python
redis_client = current_app.extensions["redis_client"]
redis_client.save_post(post_id, post)      # 写错方法名？运行时才知道
```

两个真问题：

1. **`app.extensions` 是 `dict[str, Any]`**，所以 `redis_client` 是 `Any`。
   mypy 帮不上任何忙，拼错属性名（比如 `RATE_CAPCITY`）要等线上才炸。
2. **测试只能靠真停服务**。想测"Kafka 挂了返回 503"，
   要么真的把 Redpanda 停掉（测试不能并行、不能在 CI 跑），
   要么用 `mock.patch` 塞一个假返回值 —— 那测的是"有没有调用"，不是"业务对不对"。

本步用两个手段一次性解决：**Protocol（结构化类型）+ 依赖注入**。

## 怎么跑

```powershell
# 单元测试（不需要 Redis / Kafka / Docker，毫秒级）
..\..\.venv\Scripts\python.exe -m pytest -q

# 静态类型检查
..\..\.venv\Scripts\python.exe -m mypy . --config-file mypy.ini
```

当前状态：**26 passed**，**mypy: no issues found in 20 source files**。

## 新增/改动

| 文件 | 动作 | 说明 |
|---|---|---|
| `protocols.py` | 新增 | `PostStore` / `MessagePublisher` 两个 Protocol |
| `app.py` | 改 | `create_app(config, store=..., publisher=...)` 支持注入 |
| `api/posts.py` | 改 | 类型化的 `_get_config` / `_get_store` / `_get_publisher` |
| `api/health.py` | 改 | 类型化依赖检查 |
| `mypy.ini` | 新增 | 类型检查配置（**必须纯 ASCII**，原因见下） |
| `pytest.ini` | 新增 | `testpaths`、`pythonpath` |
| `tests/` | 新增 | `fakes.py` / `conftest.py` / 5 个测试文件 |
| 其余 | 改 | 补类型注解；`logging_setup` 的 handler 改为幂等 |

## 三个核心点

### 1. Protocol：描述"能力"，不要求继承

```python
class PostStore(Protocol):
    def save_post(self, post_id: str, post: dict[str, Any]) -> None: ...
    def take_token(self, author_id: str, now_ms: int) -> tuple[bool, int]: ...
    ...
```

`RedisClient` **不需要**写 `class RedisClient(PostStore)`。
只要方法签名对得上，mypy 就认。这叫结构化类型（鸭子类型，但机器能检查）。

关键在 `app.py` 这一行：

```python
real_store: PostStore = store if store is not None else RedisClient(config)
```

标注成 `PostStore` 之后，mypy 才会真的去校验 `RedisClient` 的方法签名是否匹配。
**不标注的话这一行什么都不会检查。**

> 契约描述的是**业务能力**（抢占幂等、取令牌），不是"Redis 的 get/set"。
> 所以假实现只要写 8 个业务方法，不用重新发明一个 Redis。

### 2. 依赖注入：测试不再需要真服务

```python
def create_app(config=None, store: PostStore | None = None,
               publisher: MessagePublisher | None = None) -> Flask:
```

生产代码不传（用真的），测试代码传假的：

```python
create_app(cfg, store=FakeStore(), publisher=FakePublisher(fail=True))
```

**业务逻辑一行都没改**，但"Kafka 挂了返回 503"现在能被自动化验证，
不依赖 Docker、不依赖网络、毫秒级完成。

### 3. Fake 而不是 mock

```python
class FakeStore:
    def take_token(self, author_id: str, now_ms: int) -> tuple[bool, int]:
        remaining = self.tokens.setdefault(author_id, self.capacity)
        if remaining <= 0:
            return False, 0
        ...
```

对比 `mock`：

| | mock | Fake |
|---|---|---|
| 能断言什么 | "take_token 被调用了，返回 (False, 0)" | "取 3 次后第 4 次自然返回 False" |
| 问题 | 返回值是你自己塞的，等于自己证明自己 | 真维护状态，429 是被**推出来**的 |

代价是 Fake 要写几十行，换来的是测试真的在验证行为。

## 测试逮到的真 bug（本步最大收获）

跑测试时出现一条失败：

```
FAILED tests/test_submit.py::test_rate_limit_returns_429 - assert 500 == 429
```

原因在 `api/posts.py`：

```python
"retry_after_seconds": round(1 / config.RATE_REFILL_PER_SEC, 1)
```

`RATE_REFILL_PER_SEC` 配成 0（= 令牌永不补充，相当于硬性配额）时，
这行直接 `ZeroDivisionError` —— 接口返回 **500 而不是 429**。

这类 bug 特别阴险：**限流的响应自己抛了异常，限流逻辑在最该生效的时候失效了。**
而且它在前七步的所有手工验证里都没暴露过，因为线上配置一直是 2。

修复：

```python
def _retry_after_seconds(config: Config) -> float | None:
    if config.RATE_REFILL_PER_SEC <= 0:
        return None          # 令牌永不补充，重试也没用
    return round(1 / config.RATE_REFILL_PER_SEC, 1)
```

这就是单元测试最实际的价值：**它逼你去想那些"手工测不到"的分支。**

## ⚠️ Windows 专属坑：配置文件必须纯 ASCII

第一次跑 mypy 直接崩：

```
UnicodeDecodeError: 'gbk' codec can't decode byte 0x80 in position 46
```

原因是我在 `mypy.ini` 里写了中文注释。`configparser` 在 Windows 上
用本地代码页（GBK）读 ini 文件。**和 `requirements.txt` 是同一个坑。**

解决办法：配置文件保持纯 ASCII（注释写英文）。
源码文件不受影响，因为有 `# -*- coding: utf-8 -*-`。

## 另一个小改动：日志初始化要幂等

```python
# 不能写 root.handlers = [handler]
# 那会把 pytest 的 caplog 一起干掉，测试里一条日志都抓不到
for existing in list(root.handlers):
    if getattr(existing, "content_platform_handler", False):
        root.removeHandler(existing)
root.addHandler(handler)
```

顺带修掉了"重复调用 create_app 会导致每条日志打印两遍"。

## 测试覆盖了什么

| 文件 | 覆盖内容 |
|---|---|
| `test_submit.py` | 202+queued、幂等返回同一 post_id、重复提交不消耗令牌、限流 429、参数校验、投递失败 503、**失败后释放幂等键并可重试** |
| `test_query.py` | published / queued / 404 三态、未知路径返回 JSON 404 |
| `test_health.py` | liveness 不查依赖、readiness 逐项检查、依赖挂掉变 503 且指明是谁、draining 变红 |
| `test_request_id.py` | 回显、自动生成、随消息头传给消费者、出现在访问日志里 |
| `test_consumer.py` | 落库+改状态、重复处理幂等、处理计数 |

## 自测练习

1. `app.extensions` 为什么是 `Any`？标注成 Protocol 之后多了什么保障？
2. Protocol 和抽象基类（ABC）有什么区别？为什么这里选 Protocol？
3. 为什么契约要描述"业务能力"而不是"Redis 操作"？
4. 依赖注入给 `create_app` 加了两个参数，业务逻辑改了吗？
5. 用 mock 测"令牌耗尽返回 429"有什么问题？
6. `RATE_REFILL_PER_SEC = 0` 那个 bug 为什么前七步的手工验证没发现？
7. 为什么 `json.loads()` 的结果要先存进带标注的变量再返回？
8. `warn_return_any` 这个开关防的是什么？
9. 为什么 `logging_setup` 不能写 `root.handlers = [handler]`？
10. 配置文件为什么必须纯 ASCII？

## 到这里为止

step01 ~ step08 走完，项目已经具备：

- Flask 应用工厂 + 蓝图分层
- Redis 幂等 + Lua 令牌桶限流
- 结构化日志 + request_id 全链路（跨进程）
- Kafka 解耦 + 独立消费者（at-least-once + 消费者幂等）
- 分级健康检查 + 优雅退出
- 类型注解（mypy 全绿）+ 单元测试（26 项，无需外部服务）
