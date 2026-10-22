"""
消费者进程：把Kafka里的投稿消息落库。
消费语义：at-least-once(至少一次)
    Kafka只保证“消息不会丢”，不保证没有重复的，比如消费者重启、网络重试，都有可能会产生重复数据，所以消费者要自己幂等，本项目用post_id直接覆盖写入同一个key，处理N次结果也是完全一样
"""
from __future__ import annotations
import signal
import sys
import threading 
from typing import Any

from config import get_config
from kafka_client import KafkaConsumerClient,ensure_topic
from logging_setup import configure_logging,get_logger
from redis_client import RedisClient

logger = get_logger("consumer")

def build_handler(redis_client:RedisClient,stats: dict|None=None)->Any:
    """构造消息处理函数
    为什么用闭包传redis_client而不是全局变量：
    这样handler只依赖传进来的东西，测试时赛一个假的实现就能单测
    """
    counter = stats if stats is not None else {"n":0}
    def handle(event:dict,meta:dict):
        counter["n"] += 1
        post_id = event["post_id"]
        logger.info(
            "post.consumed",
            post_id = post_id,
            client_msg_id=event.get("client_msg_id"),
            partition=meta["partition"],
            offset=meta["offset"],
        )
        # 落库：覆盖写
        redis_client.save_post(post_id,event)
        # 终态：不设TTL，稿件永久保留
        redis_client.save_post_status(post_id,"published")
        logger.info(
            "post.published",
            post_id=post_id,
            author_id=event.get("author_id"),
            seq=counter["n"],
        )
    return handle

def main() -> None:
    config = get_config()
    configure_logging(level=config.LOG_LEVEL, json_output=config.LOG_JSON)

    redis_client = RedisClient(config)
    if not redis_client.ping():
        logger.error(
            "consumer.redis_unreachable",
            host=config.REDIS_HOST,
            port=config.REDIS_PORT,
        )
        print(f"连不上 Redis（{config.REDIS_HOST}:{config.REDIS_PORT}），消费者退出")
        sys.exit(1)

    try:
        ensure_topic(config)
    except Exception as exc:  # noqa: BLE001 - 启动期任何异常都该说清楚再退出
        logger.error("consumer.topic_failed", error=str(exc))
        print(f"准备 topic 失败：{exc}")
        sys.exit(1)

    consumer = KafkaConsumerClient(config)
    stats = {"n":0}
    handler = build_handler(redis_client,stats)
    logger.info(
        "consumer.ready",
        topic=config.KAFKA_TOPIC,
        group_id=config.KAFKA_GROUP_ID,
        bootstrap=config.KAFKA_BOOTSTRAP,
    )

    # 优雅退出：收到 Ctrl+C / kill 时置位，等当前这批消息处理完再退出。
    # 直接退出会丢掉"已处理但未提交 offset"的消息（其实不会丢，会被重投，
    # 但会造成重复处理），更重要的是可能写了一半。
    stop = threading.Event()

    def _request_stop(signum, _frame):
        name = signal.Signals(signum).name
        if stop.is_set():
            logger.warning("consumer.stopping.forced",signal=name)
            sys.exit(1)
        logger.info("consumer.stopping", signal=name)
        stop.set()

    signal.signal(signal.SIGINT, _request_stop)
    signal.signal(signal.SIGTERM, _request_stop)

    if hasattr(signal,"SIGBREAK"):
        signal.signal(signal.SIGBREAK, _request_stop)
    try:
        consumer.consume(handler, stop_event=stop)
    finally:
        consumer.close()
        redis_client.close()
        logger.info("consumer.stopped",processed=stats["n"])


if __name__ == "__main__":
    main()

