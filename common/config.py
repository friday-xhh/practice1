"""
设计要点：
    1. 所有配置项集中在一处，按 APP_ENV 切换不同 Profile；
    2. 环境变量优先级最高（便于 K8s ConfigMap / Secret 注入）；
    3. `get_settings()` 用 lru_cache 缓存，单测里可用 `cache_clear()` 重置。

层级：
    BaseAppSettings        通用默认值与键名定义
      |- DevelopmentSettings   本机：DEBUG 日志、控制台输出、低限流阈值便于触发
      '- ProductionSettings    生产：强制 JSON、大连接池、高限流阈值
"""
from __future__ import annotations
import os
from functools import lru_cache
from pydantic_settings import BaseSettings,SettingsConfigDict
# 继承BaseSettings的类，会自动读取环境变量里的值，SettingsConfigDict来配置一些读取设置
class BaseAppSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file = ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )
    # ------------------------------------------------------------ 应用
    APP_ENV: str = "development"
    APP_NAME: str = "content-platform"
    DEBUG: bool = False
    HOST: str = "0.0.0.0"
    PORT: int = 8000
    LOG_LEVEL: str = "INFO"
    LOG_JSON: bool = True
    #--------------------------------------------kafka/Redpanda
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_TOPIC_POST_INGESTED: str = "post.ingested"
    KAFKA_CLIENT_ID: str = "content-platform"
    # acks=1 而非 all：写入吞吐与可靠性的折中，业务允许极小概率重投（消费端幂等兜底）
    KAFKA_ACKS: str = "1"
    KAFKA_COMPRESSION_TYPE: str = "lz4"
    KAFKA_LINGER_MS: int = 20
    KAFKA_BATCH_NUM_MESSAGES: int = 1000
    KAFKA_MESSAGE_TIMEOUT_MS: int = 30000
    KAFKA_FLUSH_TIMEOUT: float = 10.0
    KAFKA_HEALTH_TIMEOUT: float = 3.0

    # ------------------------------------------------------------ Redis
    REDIS_URL: str = "redis://localhost:6379/0"
    REDIS_MAX_CONNECTIONS: int = 64
    REDIS_SOCKET_TIMEOUT: float = 1.0
    # 令牌桶：容量决定瞬时突发上限，refill 决定稳态速率
    RATE_LIMIT_CAPACITY: int = 50
    RATE_LIMIT_REFILL_PER_SEC: float = 20.0
    RATE_LIMIT_COST: int = 1
    RATE_LIMIT_KEY_TTL_SECONDS: int = 3600
    IDEMPOTENCY_TTL_SECONDS: int = 86400

    # ------------------------------- 检索层（本轮仅占位，后续 index/search 使用）
    ELASTICSEARCH_URL: str = "http://localhost:9200"
    MONGO_URI: str = "mongodb://localhost:27017"
    MONGO_DB: str = "content_platform"

def kafka_producer_config(self)->dict[str,object]:
    """
    生成 confluent-kafka Producer 配置
    面试可讲的三个调优点：批量攒批、压缩、acks权衡
    """
    return {
        "bootstrap.servers": self.KAFKA_BOOTSTRAP_SERVERS,
        "client.id": self.KAFKA_CLIENT_ID,
        "acks": self.KAFKA_ACKS,
        "compression.type": self.KAFKA_COMPRESSION_TYPE,
        "linger.ms": self.KAFKA_LINGER_MS,
        "batch.num.messages": self.KAFKA_BATCH_NUM_MESSAGES,
        "message.timeout.ms": self.KAFKA_MESSAGE_TIMEOUT_MS,
        # 队列积压到阈值立即发送，避免 linger 期间内存膨胀
        "queue.buffering.max.messages": 100000,
    }
class DevelopmentSettings(BaseSettings):
    """本机开发环境"""

    APP_ENV: str = "development"
    DEBUG: bool = True
    LOG_LEVEL: str = "DEBUG"
    LOG_JSON: bool = False

class ProductionSettings(BaseSettings):
    """生产环境：更大的连接池和更高的限流水位"""
    APP_ENV: str = "production"
    DEBUG: bool = False
    LOG_LEVEL: str = "INFO"
    LOG_JSON: bool = True
    REDIS_MAX_CONNECTIONS: int = 256
    RATE_LIMIT_CAPACITY: int = 2000
    RATE_LIMIT_REFILL_PER_SEC: float = 800.0

_SETTING_MAP:dict[str, type[BaseAppSettings]] = {
    "development": DevelopmentSettings,
    "dev": DevelopmentSettings,
    "production": ProductionSettings,
    "prod": ProductionSettings,
}

@lru_cache(maxsize=8)
def get_settings(env: str | None = None)->BaseAppSettings:
    """按APP_ENV返回对应配置实例（带缓存）"""
    resolved: str = (env or os.getenv("APP_ENV") or "development").strip().lower()
    return _SETTING_MAP.get(resolved,DevelopmentSettings)()