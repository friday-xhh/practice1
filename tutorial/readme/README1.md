# step01：最小可用的 Flask 服务

## 这一步学什么

**目标**：搞清楚"一个 HTTP 接口到底是怎么跑起来的"。

学完你应该能回答：用户输入一个网址/发一个请求，是怎么一步步走到你的 Python 函数里的？

这一步**不碰** Redis、Kafka、日志、配置——它们都会在后面单独出现。

## 三个接口

| 方法 | 路径 | 作用 |
|---|---|---|
| POST | `/api/v1/posts` | 提交一篇稿件，返回受理号和 202 |
| GET | `/posts/<post_id>` | 用上一步拿到的 ID 把稿件查回来 |
| GET | `/healthz` | 健康检查，永远返回 `{"status":"ok"}` |

## 跑起来

```powershell
# 进入这一步的目录
cd D:\Friday\Code\Code\Project\new_project\tutorial\step01_minimal_flask

# 激活虚拟环境（在根目录创建过就不用重复创建）
..\..\.venv\Scripts\Activate.ps1

# 启动
python app.py
```

看到类似下面的输出就成功了：

```
 * Running on http://127.0.0.1:8000
```

**另开一个 PowerShell 窗口**测试（服务要一直开着）：

```powershell
# 1. 健康检查
curl http://127.0.0.1:8000/healthz

# 2. 提交稿件
curl -Method POST http://127.0.0.1:8000/api/v1/posts `
  -ContentType "application/json" `
  -Body '{"client_msg_id":"msg-0001","author_id":"author-1","title":"我的第一篇","content":"正文内容"}'

# 3. 把上一步返回的 post_id 填进去查询
curl http://127.0.0.1:8000/posts/<这里换成你的post_id>

# 4. 故意少传字段，看 400 错误长什么样
curl -Method POST http://127.0.0.1:8000/api/v1/posts `
  -ContentType "application/json" `
  -Body '{"title":"缺少其他字段"}'
```

> PowerShell 的 `curl` 其实是 `Invoke-WebRequest` 的别名。如果想看响应状态码，
> 用 `curl -Method POST ... | Select-Object StatusCode, Content`。

## 代码讲解（对应 app.py 的 9 段）

### 第 2 段：`app = Flask(__name__)`

创建应用实例。整个程序只有这一个 `app`，所有接口都挂在它上面。

### 第 4 段：`error_response()`

统一错误格式。为什么要有它？想象一下，如果有的错误返回
`{"msg":"..."}`、有的返回 `{"error":"..."}`，调用方就得写两套解析逻辑。
统一之后，调用方永远只看一处。

### 第 5 段：`@app.post("/api/v1/posts")` —— 最关键的一句

这就是 Flask 的核心机制：

```
URL 路径 + HTTP 方法  →  一个 Python 函数
/api/v1/posts + POST  →  submit_post()
```

那个 `@` 开头的东西叫**装饰器**。你现在只需要知道它的效果：
"把下面这个函数登记到某个 URL 上"。内部原理后面再深究。

函数内部做了四件事，顺序很重要：

1. **检查是不是 JSON** —— 不是就直接拒绝
2. **解析请求体** —— 拿到 Python 字典
3. **校验字段** —— 必填的有没有、长度超没超
4. **保存并返回** —— 生成 ID、存起来、返回 202

这个顺序（先验格式、再验业务、最后干活）在后面所有步骤里都会保持。

### 第 5.3 段：为什么返回 202 而不是 200？

- `200 OK` = "事情办完了"
- `202 Accepted` = "我收下了，但还没办完"

投稿这个动作，系统只保证"收到了"，真正处理（审核、建索引）是后面的事。
现在用 202，等 step06 接入 Kafka 之后语义仍然成立，不用改代码。

### 第 6 段：`<post_id>` 路径参数

`/posts/abc123` 里的 `abc123` 会被 Flask 抓出来，直接作为函数参数传进来。
这样同一个接口就能查不同的稿件，不用为每个 ID 写一个函数。

### 第 9 段：`if __name__ == "__main__"`

这是 Python 的标准写法，意思是：
"只有直接运行这个文件时才启动服务；被别人 import 时不启动"。

## 新概念速查

| 名词 | 一句话解释 |
|---|---|
| 路由 | URL 和函数的对应关系 |
| 装饰器 | `@app.post(...)` 这种写法，作用是"给函数加个登记动作" |
| HTTP 方法 | GET 是"取东西"，POST 是"交东西" |
| 状态码 | 200 成功 / 202 已受理 / 400 请求有问题 / 404 找不到 / 500 服务端崩了 |
| JSON | 前后端传数据用的文本格式，长得像 Python 字典 |
| 请求体 | POST 时附带的那段数据 |

## 自测练习（不看代码试试）

1. 用户请求 `POST /api/v1/posts` 时，Flask 是怎么知道该执行 `submit_post` 这个函数的？
2. 如果请求里 `content` 传了 30000 个字，会发生什么？返回什么状态码？
3. 为什么第 5 段用 `body.get(field)` 而不是 `body[field]`？换成后者会怎样？
4. 访问一个不存在的路径（比如 `/abc`）会返回什么？为什么不是 HTML 页面？
5. 服务重启之后，之前提交的稿件还能查到吗？为什么？

<details>
<summary>答案（先自己想，再看）</summary>

1. 因为 `@app.post("/api/v1/posts")` 装饰器把这个函数登记到了该 URL + POST 方法上。
2. 返回 400，错误信息是 "content 超过 20000 字"（第 5.1 段的长度校验）。
3. `body[field]` 在字段不存在时会抛 `KeyError` 导致 500 错误；`.get()` 返回 None，可以优雅地报 400。这是"让程序不要崩"的基本手法。
4. 返回 JSON 格式的 404（因为第 8 段用 `@app.errorhandler(404)` 改掉了默认的 HTML 响应）。
5. 查不到。因为数据存在内存字典 `posts` 里，进程一退出内存就清空了。step02 会换成 Redis 解决这个问题。
</details>

## 常见问题

**端口被占用怎么办**
`OSError: [WinError 10048]`，说明 8000 端口被别的程序占了。
改代码最后一行的 `port=8000` 为 `port=8001`，或关掉占用端口的程序。

**改了代码没生效**
本文件设置了 `use_reloader=False`，改完代码需要手动 `Ctrl+C` 停掉再 `python app.py`。
（这是为了 Windows 上更稳定，生产环境本来就不需要自动重载。）

**curl 报错说参数不对**
PowerShell 的 `curl` 是别名，参数要用 PowerShell 风格（`-Method`、`-Body`），
不是 Linux 那种 `-X`、`-d`。

## 下一步

step02 会做一件很自然的事：现在数据存在内存里，服务一重启就没了；
我们会引入 Redis，并顺带解决"用户重复点提交怎么办"这个问题（幂等）。
