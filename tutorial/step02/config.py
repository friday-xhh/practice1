# -*- coding: utf-8 -*-
"""配置集中管理。

回忆 step03 的写法：参数散落在业务代码里

    RATE_CAPACITY = 10          # 在 app.py 中间某处
    REDIS_HOST = "localhost"    # 又在另一个地方

这有三个麻烦：
    1. 想改一个参数，得在几百行代码里翻
    2. 本机和生产用同一套值，没法区分
    3. 部署到容器后改配置要重新打包

解决方式：**所有参数搬到这一个文件**，并且允许环境变量覆盖。

为什么支持环境变量：
    容器/K8s 里改配置不该重新构建镜像。通过环境变量（ConfigMap / Secret）
    注入新值，重启容器即可生效 —— 这也是 `APP_ENV` 能切换环境的原因。
"""

import os

def _get_str(name:str,default:str)->str:
    """读字符串型配置"""
    return os.getenv(name) or default

def _get_int(name:str,default:int)->int:
    """读整数型配置。没设置用默认值；设置了但不是整数就明确报错。"""
    raw = os.getenv(name)
    if raw is None or raw =="":
        return default
    try:
        return int(raw)
    except ValueError:
        raise ValueError(f"环境变量 {name} 必须是整数，当前值是 {raw!r}") from None
def _get_float(name: str, default: float) -> float:
    """读浮点型配置。"""
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError:
        raise ValueError(f"环境变量 {name} 必须是数字，当前值是 {raw!r}") from None

# 配置类
class Config:
    """默认配置（本机开发）。
    类属性在**类定义时**求值，所以环境变量是在 import 这个模块时读取的。
    也就是说：服务启动后你再改系统环境变量，本次运行不会变，重启才生效。
    """
    # ---服务---
    HOST = _get_str("HOST","127.0.0.1")
    PORT = _get_int("PORT",8000)
    DEBUG = _get_str("DEBUG","true").lower() == "true"
    # ---redis---
    REDIS_HOST = _get_str("REDIS_HOST","localhost")
    REDIS_PORT = _get_int("REDIS_PORT",6379)
    REDIS_DB = _get_int("REDIS_DB",0)
    #---键前缀---
    KEY_POST = "post:" # 稿件内容
    KEY_IDEM = "idem:" # 幂等键
    KEY_RATE = "rl:" # 令牌桶
    KEY_STATUS = "status:"#队列中/已发布
    #---幂等---
    IDEM_TTL_SECONDS = _get_int("IDEM_TTL_SECONDS",24*3600)
    #---限流（令牌桶）---
    RATE_CAPACITY = _get_int("RATE_CAPACITY", 10)
    RATE_REFILL_PER_SEC = _get_float("RATE_REFILL_PER_SEC", 2)  # 每秒补充速率
    RATE_COST = _get_int("RATE_COST", 1)
    RATE_TTL_SECONDS = _get_int("RATE_TTL_SECONDS", 3600)
    # ---- 业务限制 ----
    MAX_CONTENT_LENGTH = _get_int("MAX_CONTENT_LENGTH", 20000)
    # ----稿件处理状态----
    STATUS_TTL_SECONDS = _get_int("STATUS_TTL_SECONDS",600)
    # ----Kafka----
    KAFKA_BOOTSTRAP = _get_str("KAFKA_BOOTSTRAP","127.0.0.1:9092")
    KAFKA_TOPIC = _get_str("KAFKA_TOPIC","post.ingested")
    # 消费者组：同一个组内的多个实例分摊分区，互不重复消费
    KAFKA_GROUP_ID = _get_str("KAFKA_GROUP_ID","post-ingest-worker")
    # ---- 健康检查 ----
    # 依赖探活的最坏耗时上限。探活**必须有界**：
    # 若下游卡住不响应，健康检查自己也卡死，编排系统永远等不到答复，
    # 反而会判定这个实例"失联"。宁可快速报"不健康"，也不要不回答。
    HEALTH_CHECK_TIMEOUT_MS = _get_int("HEALTH_CHECK_TIMEOUT_MS", 3000)

    # ---- 优雅退出 ----
    # 收到停止信号后：先标记 not ready（让负载均衡把流量摘走），
    # 再等这么久让在途请求处理完，然后才释放资源、退出进程。
    SHUTDOWN_GRACE_SECONDS = _get_int("SHUTDOWN_GRACE_SECONDS", 5)
    # ---日志---
    LOG_LEVEL = _get_str("LOG_LEVEL","INFO")
    # 开发时默认人类可读（一眼看懂），生产用JSON（给采集系统解析）
    LOG_JSON = _get_str("LOG_JSON","false").lower() == "true"

class ProductionConfig(Config):
    """生产环境配置。
    只需要写"和开发环境不一样的那几项"，其余全部继承 —— 这就是配置分层的好处。
    """

    DEBUG = False
    # 生产日志必须是 JSON，否则采集系统没法解析
    LOG_JSON = _get_str("LOG_JSON","true").lower()=="true"
    # 线上流量大，限流水位高得多
    RATE_CAPACITY = _get_int("RATE_CAPACITY", 200)
    RATE_REFILL_PER_SEC = _get_float("RATE_REFILL_PER_SEC", 50)
    # 线上幂等键保留更久
    IDEM_TTL_SECONDS = _get_int("IDEM_TTL_SECONDS", 7 * 24 * 3600)

# 环境名 -> 配置类 的映射
CONFIGS: dict[str, type[Config]] = {
    "development": Config,
    "dev": Config,
    "production": ProductionConfig,
    "prod": ProductionConfig,
}


def get_config(env: str | None = None) -> Config:
    """按 APP_ENV 取对应配置（默认 development）。"""
    resolved = (env or os.getenv("APP_ENV") or "development").strip().lower()
    return CONFIGS.get(resolved, Config)()