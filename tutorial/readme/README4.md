# step04：代码拆分（蓝图 + 配置分层）

## 这一步学什么

**这一步不添加任何新功能**——接口行为和 step03 完全一样。

学的是**代码怎么组织**。三个新概念：

1. **蓝图（Blueprint）**：路由分组机制，解决"路由多了怎么分文件"
2. **应用工厂 `create_app()`**：解决"应用实例怎么创建、配置怎么注入"
3. **配置分层**：解决"参数散落在代码里"的问题

> 学完你会发现，真实项目的 `app/api/`、`common/config.py`、`run.py` 就是这么来的。

## 为什么要拆

step03 的 `app.py` 已经 249 行。继续加功能会有三个问题：

| 问题 | 表现 |
|---|---|
| 找一个东西要翻半天 | 改个限流参数，得在 200 多行里搜索 |
| 所有概念挤在一起 | Redis 连接、Lua 脚本、路由、校验、业务逻辑全在一个文件 |
| 配置写死 | 想换个环境得改代码然后重新打包 |

## 文件结构与对照

```
step04_blueprint_config/
├── run.py              原项目 run.py                启动入口，只做启动
├── config.py           原项目 common/config.py      所有参数集中管理
├── redis_client.py     原项目 common/redis_client.py  Redis 连接 + Lua 脚本
├── app.py              原项目 app/__init__.py       应用工厂
├── errors.py           原项目 app/errors.py         统一错误响应
└── api/
    ├── __init__.py
    ├── health.py       原项目 app/api/health.py     健康检查路由
    └── posts.py        原项目 app/api/submit.py     投稿/查询路由
```

每个文件都不超过 100 行，职责单一。对照着看原项目，你会发现结构几乎一致。

> 计划里原本是 6 个文件，实际多了 `errors.py`——因为 `error_response` 会被多个
> 蓝图用到，放在任何一个蓝图里都会造成循环 import。原项目也是这么拆的。

## 概念一：蓝图（Blueprint）

```python
# api/health.py
health_bp = Blueprint("health", __name__)

@health_bp.get("/healthz")
def health():
    ...
```

```python
# app.py
app.register_blueprint(health_bp)
app.register_blueprint(posts_bp)
```

蓝图就是**一组路由的集合**：先在各个文件里定义好，最后统一挂到主应用上。

好处：职责分离——`health.py` 只管健康检查，`posts.py` 只管投稿，互不干扰。

## 概念二：应用工厂 `create_app()`

原来的写法：

```python
app = Flask(__name__)      # import 的瞬间就固定了
```

问题：应用实例在导入时就创建完毕，没法给它传不同的配置。

改成工厂：

```python
def create_app(config=None):
    app = Flask(__name__)
    app.extensions["config"] = config
    app.extensions["redis_client"] = RedisClient(config)
    app.register_blueprint(health_bp)
    return app
```

好处有三个：

1. **配置可注入**：测试传测试配置，生产传生产配置，业务代码不用动
2. **实例隔离**：每次调用得到全新实例，互不污染
3. **依赖清晰**：要用什么资源，都在工厂里一次性挂好

### `current_app` 和 `app.extensions` 是什么

工厂模式下**没有全局 `app` 变量**了，那蓝图里怎么拿 Redis 客户端？

```python
from flask import current_app

redis_client = current_app.extensions["redis_client"]
```

- `current_app`：指代"当前正在处理请求的那个应用实例"
- `app.extensions`：Flask 官方提供的扩展挂载点，用来放共享资源

> 千万不要用全局变量存这些资源——那样又回到了"实例之间互相污染"的老问题。

## 概念三：配置分层

```python
class Config:                      # 默认（开发）
    RATE_CAPACITY = _get_int("RATE_CAPACITY", 10)

class ProductionConfig(Config):    # 生产，只写不一样的
    DEBUG = False
    RATE_CAPACITY = _get_int("RATE_CAPACITY", 200)
```

**环境变量优先**：`_get_int("RATE_CAPACITY", 10)` 的意思是
"读环境变量 `RATE_CAPACITY`，没设置就用 10"。

为什么这样设计：容器/K8s 里改配置不该重新构建镜像，
改个环境变量（ConfigMap/Secret）重启即可。

实际用起来：

```powershell
# 换配置不用改一行代码
RATE_CAPACITY=3 python run.py          # 临时压低限流阈值
APP_ENV=production python run.py       # 整套切换到生产配置
```

**一个容易踩的点**：类属性在**类定义时**求值，也就是 import 模块的时候就读环境变量了。
服务跑起来之后再改系统环境变量，本次运行不生效，要重启才行。

## 重构踩到的一个坑（很有代表性）

我一开始给 posts 蓝图加了 `url_prefix="/api/v1"`，结果：

| 接口 | step03 | 加了 url_prefix 之后 |
|---|---|---|
| 投稿 | `/api/v1/posts` | `/api/v1/posts` ✓ |
| 查询 | `/posts/<id>` | `/api/v1/posts/<id>` ✗ **变了** |

url_prefix 会把蓝图下**所有**路由都加上前缀，于是查询接口被悄悄改了路径。
这就是违背"**重构不改变外部行为**"的典型例子，最后我把它去掉了。

> 什么时候该用 `url_prefix`：当一组路由确实共享同一个前缀时。
> 本项目两个路由前缀不一致，所以不能用。代码注释里写了这段。

## 亲手验证

```powershell
docker start cp-redis        # 确保 Redis 在跑
cd D:\Friday\Code\Code\Project\new_project\tutorial\step04_blueprint_config
..\..\.venv\Scripts\Activate.ps1
python run.py
```

启动日志会告诉你当前用的是哪套配置：

```
配置类=Config，Redis 连接正常
服务地址 http://127.0.0.1:8000
限流参数：桶容量=10，每秒补充=2
```

试试切环境：

```powershell
$env:APP_ENV="production"; python run.py
# 配置类=ProductionConfig ... 桶容量=200

$env:RATE_CAPACITY="3"; $env:APP_ENV=""; python run.py
# 限流参数：桶容量=3
```

接口调用方式和 step03 完全一样：

```powershell
curl -Method POST http://127.0.0.1:8000/api/v1/posts `
  -ContentType "application/json" `
  -Body '{"client_msg_id":"msg-1","author_id":"author-A","title":"第一篇","content":"正文"}'

curl http://127.0.0.1:8000/posts/<返回的post_id>
curl http://127.0.0.1:8000/healthz
```

## 自测练习

1. 蓝图解决什么问题？不用蓝图会怎样？
2. 应用工厂相比 `app = Flask(__name__)` 有什么好处？
3. 为什么不能用全局变量存 Redis 客户端？
4. `current_app` 是什么？为什么需要它？
5. 配置为什么要支持环境变量？直接写死有什么麻烦？
6. 改了系统环境变量后，正在运行的服务会立刻生效吗？
7. 给 `posts_bp` 加 `url_prefix="/api/v1"` 会发生什么？为什么这是错的？
8. `errors.py` 为什么要单独拆出来？

<details>
<summary>答案</summary>

1. 蓝图把路由按主题分组到不同文件。不用蓝图的话，所有路由都得堆在创建 app 的那个文件里，几百上千行时没法维护。
2. 配置可以从外部注入、每次调用得到全新实例、依赖关系在创建时一次性确定；全局写法则把这些都固定死了。
3. 全局变量在多实例场景下会互相污染（比如测试环境中两个 app 共享同一个 Redis 连接），也不利于替换成替身做测试。
4. `current_app` 指代当前处理请求的应用实例。工厂模式下没有全局 app 变量，只能通过它访问挂在 app 上的资源。
5. 容器里改配置不应重新构建镜像。环境变量可以在部署时注入，重启即生效。
6. 不会。配置在 import config 模块时就读取了，也就是服务启动时。要重启才生效。
7. 查询接口路径会变成 `/api/v1/posts/<id>`，与原来的 `/posts/<id>` 不一致，等于悄悄修改了对外 API，违反"重构不改变外部行为"。
8. `error_response` 被多个蓝图共用。放在任一蓝图里会导致其他蓝图反向 import 它，形成循环依赖。
</details>

## 常见问题

**`ModuleNotFoundError: No module named 'app'`**
要在 `step04_blueprint_config` 目录下运行 `python run.py`。
Python 会把"脚本所在目录"加入搜索路径，在别的目录运行就找不到这些模块。

**改了 config.py 没生效**
关掉服务重新启动。另外注意别写成 Shell 会话里没导出的变量。

**想加新的一组接口怎么办**
在 `api/` 下新建文件（比如 `api/admin.py`），定义 `admin_bp`，
然后到 `app.py` 里 `app.register_blueprint(admin_bp)`。

## 下一步

step05 会加**结构化日志 + request_id**：
现在排查问题只能盯着控制台，没法知道"某一次请求"在整个流程里做了什么。
到时候你会发现，加日志的位置正好是每个蓝图里——如果现在没拆文件，加起来会很乱。
