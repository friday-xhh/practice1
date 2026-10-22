# 1、本项目如何处理各个并发的场景？

**并发这件事项目里不是没有，而是全部外包给了 Redis 和 Kafka。**

| 并发问题 | 谁在解决 | 在哪 |
|---|---|---|
| 多请求同时到达 | Flask dev server 多线程（`threaded=True` 默认） | 隐式，没人提过 |
| 并发下令牌超发 | Redis Lua 原子执行 | `redis_client.py` |
| 并发下幂等被击穿 | Redis Lua 原子执行 | `redis_client.py` |
| 并发下 request_id 串台 | `contextvars` 线程/协程隔离 | `logging_setup.py` |
| 并发下顺序乱掉 | `max_in_flight_requests_per_connection=1` | `kafka_client.py` |
| 消费并行度 | 分区数（当前 **1**） | 配置 |
