# content-platform 新手教程

从"最短能跑的代码"开始，一步一步长成一个完整的 Python 微服务。

## 这个教程解决什么问题

直接看一个完整微服务项目会觉得无从下手，因为它同时用了：应用工厂、蓝图、
结构化日志、Lua 脚本、Kafka、类型注解、优雅退出……任何一个都能单独学一天。

**所以这里的做法是：每一步只加一个新东西。**

每一步都是一个能独立运行的小项目，全都跑通之后，你回头看完整项目就是"复习"，而不是"天书"。

## 怎么学（建议顺序）

1. 先读这一步目录下的 `README.md`（讲这一步要解决什么问题）
2. 再读代码（每一行关键代码都有中文注释，解释"为什么这么写"）
3. 按 README 里的命令把它跑起来，用 curl **亲手调一次**（光看不动手记不住）
4. 做一遍 README 末尾的"自测练习"，答不出来就回去再看

## 路线图

| 步骤 | 这一步新学什么 | 对应完整项目的哪一块 | 状态 |
|---|---|---|---|
| **step01** | Flask 路由 / 请求 / 响应 | `app/api/submit.py` 的雏形 | 已完成 |
| step02 | Redis + 幂等（重复提交返回同一结果） | `common/redis_client.py` | 已完成 |
| step03 | 令牌桶限流（Lua 保证原子） | `common/redis_client.py` | 已完成 |
| step04 | 蓝图 + 配置分层 | `app/__init__.py`、`common/config.py` | 已完成 |
| step05 | 结构化日志 + request_id | `common/logging.py` | 已完成 |
| step06 | Kafka 削峰解耦（生产者 + 独立消费者进程） | `common/kafka_client.py`、`consumer.py` | 已完成 |
| step07 | 健康检查分级 + 优雅退出 | `app/api/health.py`、`app/extensions.py` | 已完成 |
| step08 | 类型注解 + 单元测试 | mypy 配置、`tests/` | 已完成 |

走到 step08，代码基本就是完整的 `ingest-service` 了。

## 环境准备（只需做一次）

```powershell
cd D:\Friday\Code\Code\Project\new_project

python -m venv .venv                  # 创建虚拟环境（隔离依赖，不污染全局 Python）
.\.venv\Scripts\Activate.ps1          # 激活（命令行前面会出现 (.venv)）
pip install -r requirements.txt       # 装依赖
```

> 如果提示"无法加载脚本"，先执行：
> `Set-ExecutionPolicy -Scope Process RemoteSigned`

> **两个 Windows 专属坑**（代码里已规避，遇到时知道原因即可）：
> - `requirements.txt` 必须保持纯 ASCII —— `pip` 在 Windows 用本地代码页(GBK)读它，
>   中文注释会直接让它崩掉（`UnicodeDecodeError: 'gbk' codec`）。
> - `.ps1` 脚本必须存成 UTF-8 **with BOM** —— PowerShell 5.1 按 GBK 解析无 BOM 的脚本，
>   中文会导致语法错误（报"意外的标记 }"）。

**什么是虚拟环境**：每个项目依赖的库版本可能冲突，虚拟环境就是给这个项目
单独开一间"隔离小屋"，在里面装什么都不影响其他项目。

## 依赖的外部服务（step02 起需要 Redis，step06 起需要 Kafka）

```powershell
# Redis（step02 起）
docker run -d --name cp-redis -p 6379:6379 redis:7.4-alpine redis-server --appendonly no

# Redpanda（step06 起。它是 Kafka 协议的兼容实现，单容器即可跑，
# 不需要 Zookeeper / KRaft 那套，非常适合本机教学）
docker run -d --name cp-redpanda -p 9092:9092 `
    docker.redpanda.com/redpandadata/redpanda:v24.2.7 `
    redpanda start --overprovisioned --smp 1 --memory 1G --reserve-memory 0M `
      --node-id 0 --check=false `
      --kafka-addr 0.0.0.0:9092 --advertise-kafka-addr 127.0.0.1:9092
```

查看是否活着：`docker ps`

## 目录说明

```
tutorial/
├── README.md                    你正在看的这份总览
└── step01_minimal_flask/
    ├── README.md                这一步的讲解文档
    └── app.py                   这一步的代码（单文件，可直接运行）
```

每一步都长这个样子：一个 `README.md` + 可直接运行的代码。
