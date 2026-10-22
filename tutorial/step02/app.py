# -*- coding: utf-8 -*-
"""应用工厂：把配置、Redis 客户端、各个蓝图组装成一个完整应用。

对比 step03 的写法：

    app = Flask(__name__)          # 模块一 import 就固定了，没法换配置
    @app.post("/api/v1/posts")     # 所有路由堆在一个文件里

用工厂函数 create_app(config) 之后：

    - 配置从外面传进来：测试用一套、生产用另一套，业务代码不用改
    - 每次调用返回一个全新实例，互不干扰
    - 路由分散到 api/ 下的各个蓝图里

这就是"应用工厂模式"，真实项目几乎都这么写。
"""
import threading
from flask import Flask, g, request
import uuid
import time


from api.health import health_bp
from api.posts  import posts_bp
from config     import get_config
from errors     import register_error_handlers
from redis_client import RedisClient
from logging_setup import (
    REQUEST_ID_HEADER,
    bind_request_id,
    clear_request_id,
    configure_logging,
    get_logger,
)
from kafka_client import KafkaProducerClient

logger = get_logger("app")

def register_middleware(app)->None:
    @app.before_request
    def assign_request_id():
        request_id = request.headers.get(REQUEST_ID_HEADER) or uuid.uuid4().hex
        g.request_id = request_id
        g.started_at = time.perf_counter()
        bind_request_id(request_id)
    @app.after_request
    def echo_request_id(response):
        response.headers[REQUEST_ID_HEADER] = getattr(g,"request_id","-")
        return response
    @app.teardown_request
    def log_access(exc):
        started = getattr(g,"started_at",None)
        duration_ms = round((time.perf_counter() - started) * 1000, 2) if started else -1
        logger.info(
            "http.access",
            method = request.method,
            path = request.path,
            status = exc.__class__.__name__ if exc else "ok",
            duration_ms = duration_ms,
        )
        clear_request_id()

def close_resources(app)->None:
    # 先关生产者 再关redis
    producer = app.extensions.get("Kafka_producer")
    if producer is not None:
        producer.close()
    redis_client = app.extensions.get("redis_client")
    if redis_client is not None:
        redis_client.close()

def graceful_shutdown(app, config)->bool:
    """执行优雅退出前三步（标记 -> 等待 -> 释放资源）。

    **不负责终止进程**，由调用方决定怎么收尾 ——
    这样它既能被信号处理调用，也能被单独测试。

    :return: True = 走完了正常流程；False = 已经在退出中（收到第二个信号）

    四步：
        1. 标记 draining  -> /readyz 立刻返回 503，上游把流量摘走
        2. 等待宽限期     -> 让已经进来的请求处理完，不要硬掐断
        3. 释放资源       -> 生产者 flush 并关闭、Redis 连接关闭
        4. 退出进程

    为什么必须有第 1 步：
        直接退出的话，负载均衡那边还要等好几个探测周期才发现你没了，
        这期间的流量全部打到一具尸体上，全是 502 / 超时。
        主动变红是"告诉上游别再发了我马上走"，这一步是免费的。

    为什么必须有第 2 步：
        一个请求可能正跑到一半（刚占了幂等键、还没投递消息）。
        硬杀会留下不一致的状态，给它几秒钟跑完。

    为什么单独抽成函数而不是写在 signal handler 里：
        这样它能被单独调用和验证，不用真的去发操作系统信号。
    """
    draining = app.extensions["draining"]

    if draining.is_set():
        # 已经在退出了，又来一个信号 = "等不及了"
        logger.warning("shutdown.forced")
        return False

    draining.set()
    logger.info(
        "shutdown.draining",
        grace_seconds=config.SHUTDOWN_GRACE_SECONDS,
    )

    # 宽限期：让在途请求跑完
    time.sleep(config.SHUTDOWN_GRACE_SECONDS)

    close_resources(app)
    logger.info("shutdown.complete")
    return True

def create_app(config=None) -> Flask:
    """组装并返回一个 Flask 应用实例。

    :param config: 配置对象；不传则按 APP_ENV 自动取
    """
    if config is None:
        config = get_config()
    configure_logging(level=config.LOG_LEVEL,json_output=config.LOG_JSON)

    app = Flask(__name__)

    # app.extensions 是 Flask 官方推荐的"扩展挂载点"。
    # 把共享资源放这里，蓝图里就能通过 current_app.extensions[...] 拿到，
    # 不需要用全局变量（全局变量是工厂模式下的大忌）。
    app.extensions["config"] = config
    app.extensions["redis_client"] = RedisClient(config)
    app.extensions["kafka_producer"] = KafkaProducerClient(config)
    # 优雅退出开关：置位后 /readyz 立刻返回 503，让上游把流量摘走。
    # 放在 extensions 而不是全局变量，是为了和 config / redis_client 保持一致，
    # 也避免多个 app 实例（比如测试里）互相干扰。
    app.extensions["draining"] = threading.Event()

    register_error_handlers(app)
    register_middleware(app)

    # 注册蓝图：每个蓝图贡献一组路由，主应用负责把它们拼起来
    app.register_blueprint(health_bp)
    app.register_blueprint(posts_bp)

    producer = app.extensions["kafka_producer"]
    if not producer.is_ready():
        # 只告警不退出：Kafka 挂了服务仍然能提供查询，投稿会返回 503
        logger.error("app.kafka_unavailable", error=producer.error)

    logger.info("app.ready",config=config.__class__.__name__,topic=config.KAFKA_TOPIC,kafka_ready=producer.is_ready(),)
    return app
