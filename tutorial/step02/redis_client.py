# -*- coding: utf-8 -*-
"""Redis 连接与 Lua 脚本封装。

为什么把它从业务代码里抽出来：
    Redis 属于**基础设施**，不该和"投稿"这种业务逻辑混在一个文件里。
    抽出来之后，别的模块要用 Redis 直接 import 这里，不用复制一遍连接代码。

另一个好处：路由层调用的是 `take_token()`、`acquire_idempotency()` 这种
**有业务含义的方法**，而不是一堆 `eval(...)` 细节，读起来清楚得多。
"""

import json
from typing import Any

import redis


# ---------------------------------------------------------------------------
# Lua 脚本一：幂等（判断 + 返回已有值，一次完成）
# ---------------------------------------------------------------------------
# 注意：这是 Lua 语言，不是 Python。Redis 原子执行整段脚本。
LUA_IDEMPOTENT = """
-- KEYS[1] = 幂等键
-- ARGV[1] = 本次的 post_id
-- ARGV[2] = 过期时间（毫秒）
-- 返回 {是否首次(1/0), post_id}
local current = redis.call('GET', KEYS[1])
if current then
    return {0, current}
end
redis.call('SET', KEYS[1], ARGV[1], 'PX', tonumber(ARGV[2]))
return {1, ARGV[1]}
"""

# ---------------------------------------------------------------------------
# Lua 脚本二：令牌桶限流
# ---------------------------------------------------------------------------
# "读令牌 -> 补令牌 -> 判断 -> 扣减 -> 写回"必须原子，否则并发下会超发。
LUA_TOKEN_BUCKET = """
-- KEYS[1] = 桶键
-- ARGV = 容量, 每秒补充, 当前时间(毫秒), 本次消耗, 过期时间(毫秒)
-- 返回 {是否放行(1/0), 剩余令牌}
local capacity = tonumber(ARGV[1])
local refill   = tonumber(ARGV[2])
local now      = tonumber(ARGV[3])
local cost     = tonumber(ARGV[4])
local ttl      = tonumber(ARGV[5])

local bucket = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(bucket[1])
local ts     = tonumber(bucket[2])

if tokens == nil or ts == nil then
    tokens = capacity
    ts = now
end

local elapsed = math.max(0, now - ts) / 1000.0
tokens = math.min(capacity, tokens + elapsed * refill)

local allowed = 0
if tokens >= cost then
    tokens = tokens - cost
    allowed = 1
end

redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', KEYS[1], ttl)
return {allowed, math.floor(tokens)}
"""


class RedisClient:
    """把所有 Redis 操作收拢到一个类里。"""

    def __init__(self, config: Any) -> None:
        self._config = config
        # 建立 Redis 连接。注意参数全部来自 config，这里不写死任何值
        self._client = redis.Redis(
            host=config.REDIS_HOST,
            port=config.REDIS_PORT,
            db=config.REDIS_DB,
            decode_responses=True,
            socket_connect_timeout=1,
        )
        # register_script：预注册到 Redis，之后调用只发送脚本哈希，
        # 不用每次都传整段脚本文本
        self._idempotent = self._client.register_script(LUA_IDEMPOTENT)
        self._token_bucket = self._client.register_script(LUA_TOKEN_BUCKET)

    # ------------------------------------------------------------------ 探活
    def ping(self) -> bool:
        """Redis 是否可用。这里吞掉异常，由调用方决定怎么处理。"""
        try:
            return bool(self._client.ping())
        except redis.exceptions.RedisError:
            return False

    # ------------------------------------------------------------------ 稿件
    def save_post(self, post_id: str, post: dict) -> None:
        raw = json.dumps(post, ensure_ascii=False)
        self._client.set(self._config.KEY_POST + post_id, raw)

    def get_post(self, post_id: str) -> dict | None:
        raw = self._client.get(self._config.KEY_POST + post_id)
        if raw is None:
            return None
        return json.loads(raw)

    # ------------------------------------------------------------------ 状态
    def save_post_status(self,post_id:str,status:str,ttl_seconds:int|None=None):
        self._client.set(self._config.KEY_STATUS + post_id,status,ex=ttl_seconds)
    def get_post_status(self,post_id:str):
        return self._client.get(self._config.KEY_STATUS+post_id)

    # ------------------------------------------------------------------ 幂等
    def acquire_idempotency(self, client_msg_id: str, post_id: str) -> tuple[bool, str]:
        """抢占幂等键。

        :return: (是否首次, post_id)。非首次时返回首次生成的 post_id。
        """
        first, existing = self._idempotent(
            keys=[self._config.KEY_IDEM + client_msg_id],
            args=[post_id, str(self._config.IDEM_TTL_SECONDS * 1000)],
        )
        return int(first) == 1, str(existing)
    def release_idempotency(self,client_msg_id:str):
        self._client.delete(self._config.KEY_IDEM+client_msg_id)

    # ------------------------------------------------------------------ 限流
    def take_token(self, author_id: str, now_ms: int) -> tuple[bool, int]:
        """从令牌桶取一个令牌。

        :return: (是否放行, 剩余令牌数)
        """
        allowed, remaining = self._token_bucket(
            keys=[self._config.KEY_RATE + author_id],
            args=[
                str(self._config.RATE_CAPACITY),
                str(self._config.RATE_REFILL_PER_SEC),
                str(now_ms),
                str(self._config.RATE_COST),
                str(self._config.RATE_TTL_SECONDS * 1000),
            ],
        )
        return int(allowed) == 1, int(remaining)

    # ------------------------------------------------------------------ 收尾
    def close(self) -> None:
        try:
            self._client.close()
        except redis.exceptions.RedisError:
            pass
