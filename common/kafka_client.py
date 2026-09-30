"""Kafka Producer / Consumer 工厂。

使用 confluent-kafka（librdkafka 封装），对 Redpanda 与原生 Kafka 均适用，
客户端层完全兼容，切换时只需改 bootstrap.servers。

面试要点：
    - 异步 send + delivery callback，不阻塞请求线程；
    - 退避失败不入 MQ 而是记录 KafkaError，由上层兜底重试；
    - flush 用于优雅退出，避免进程退出丢消息。
"""
from __future__ import annotations
