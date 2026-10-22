# -*- coding: utf-8 -*-
"""服务入口。职责只有一件事：把应用跑起来。

对比 step03：那时候 `app.run(...)` 和几百行业务代码写在一个文件里。
现在这里只做 4 步：取配置 -> 建应用 -> 自检 -> 启动。

运行：
    python run.py

    # 用不同环境/参数启动（不用改代码）
    APP_ENV=production python run.py
    RATE_CAPACITY=3 python run.py
"""
import os
import signal 
import threading
import sys

from app import create_app, graceful_shutdown
from config import get_config
from logging_setup import get_logger

logger = get_logger("run")

def _redis_hint(config):
    return (
        f"连不上 Redis（{config.REDIS_HOST}:{config.REDIS_PORT}）\n"
        "请先启动 Redis：\n"
        "  docker run -d --name cp-redis -p 6379:6379 \\\n"
        "      redis:7.4-alpine redis-server --appendonly no"
    )

def _kafka_hint(config) -> str:
    return (
        f"连不上 Kafka（{config.KAFKA_BOOTSTRAP}）\n"
        "投稿接口会返回 503。请启动 Redpanda：\n"
        "  docker run -d --name cp-redpanda -p 9092:9092 \\\n"
        "      docker.redpanda.com/redpandadata/redpanda:v24.2.7 \\\n"
        "      redpanda start --overprovisioned --smp 1 --memory 1G \\\n"
        "        --reserve-memory 0M --node-id 0 --check=false \\\n"
        "        --kafka-addr 0.0.0.0:9092 --advertise-kafka-addr 127.0.0.1:9092"
    )

def install_signal_handlers(app, config)->None:
    """把操作系统信号接到 graceful_shutdown 上。
    真正的退出逻辑在 app.graceful_shutdown() 里，这里只做"接线"。
    """

    def _handle(signum, _frame):
        logger.info("shutdown.signal_received", signal=signal.Signals(signum).name)

        # 关键：**不能在信号处理器里 sleep**。
        # 处理器是在主线程里执行的，而 socket 的 accept() 也在主线程。
        # 主线程一睡，服务就再也接不了新连接 ——
        # 那"排水期间还能应答 /readyz（告诉上游别再发流量）"就成了一句空话。
        # 所以这里只做最轻的事：开一个线程去跑真正的退出流程。
        def _run():
            ok = graceful_shutdown(app, config)
            # 为什么用 os._exit 而不是 sys.exit：
            # sys.exit 只是抛 SystemExit，在子线程里抛只能结束这个线程，
            # 结束不了进程。而此时资源已经按顺序手动释放完毕、日志也已 flush，
            # 直接终止进程是安全的。
            os._exit(0 if ok else 1)

        threading.Thread(target=_run, daemon=True).start()

    signal.signal(signal.SIGINT, _handle)    # Ctrl+C
    signal.signal(signal.SIGTERM, _handle)   # docker stop / k8s 终止

    # Windows 上 SIGTERM 是假的：Python 收到它直接 TerminateProcess，
    # handler 根本没机会跑。用 CTRL_BREAK_EVENT 作为等价的"请退出"信号，
    # 这样在本机（Windows）也能验证优雅退出。
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, _handle)


def main() -> None:
    config = get_config()
    app = create_app(config)

    # 启动自检：Redis 连不上就早点说清楚，
    # 别让服务"看起来起来了"但每个请求都在报看不懂的错
    redis_client = app.extensions["redis_client"]
    if not redis_client.ping():
        print("=" * 60)
        print(_redis_hint(config))
        print("=" * 60)
        sys.exit(1)

    # Kafka 只告警不退出：它挂了之后查询还能用，投稿返回 503
    producer = app.extensions["kafka_producer"]
    if not producer.is_ready():
        print("=" * 60)
        print(_kafka_hint(config))
        print("=" * 60)

    print(f"配置类={config.__class__.__name__}，Redis 连接正常")
    print(f"服务地址 http://{config.HOST}:{config.PORT}")
    print(f"限流参数：桶容量={config.RATE_CAPACITY}，每秒补充={config.RATE_REFILL_PER_SEC}")
    print(f"Kafka：{config.KAFKA_BOOTSTRAP}，topic={config.KAFKA_TOPIC}")
    print(f"探针：liveness=http://{config.HOST}:{config.PORT}/healthz"
          f"  readiness=http://{config.HOST}:{config.PORT}/readyz")
    install_signal_handlers(app, config)
    app.run(host=config.HOST, port=config.PORT, debug=config.DEBUG, use_reloader=False)


if __name__ == "__main__":
    main()