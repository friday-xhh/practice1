from flask import Flask,jsonify,request
import uuid
import sys
import json
import redis
import time

app=Flask(__name__)
# 链接redis
# decode_responses=True：让返回值是 str 而不是 bytes，省得每次自己 .decode()
redis_client = redis.Redis(
    host="localhost",
    port=6379,
    db=0,
    decode_responses=True,
    socket_connect_timeout=1,
)

# 键的前缀：不同用途的键用前缀隔开，避免撞车，排查时也能一眼看出是什么
KEY_POST = "post:"      # 稿件内容： post:<post_id>       -> JSON 字符串
KEY_IDEM = "idem:"      # 幂等键：   idem:<client_msg_id> -> post_id
KEY_RATE = "rl:"        # 限流桶：   rl:<author_id>       -> Hash(tokens, ts)
IDEM_TTL_SECONDS = 24*3600 # 幂等键保留一天，避免无限堆积

#令牌桶参数
# 令牌桶参数（故意调低，方便你手动点几下就能触发限流）
RATE_CAPACITY = 2              # 桶容量：最多能攒 10 个令牌（允许的瞬时突发量）
RATE_REFILL_PER_SEC = 0.02         # 每秒补充 0.2 个令牌（稳态速率）
RATE_COST = 1                   # 每次投稿消耗 1 个令牌
RATE_TTL_SECONDS = 3600         # 长时间没人访问就清掉这个桶，省内存

# ============ 3. Lua 脚本一：幂等============
# 作为原子操作进行幂等操作，解决了传统方法的边界问题
LUA_IDEMPOTENT = """
-- KEYS[1] = 幂等键
-- ARGV[1] = 本次生成的 post_id
-- ARGV[2] = 过期时间（毫秒）
-- 返回 {是否首次(1/0), post_id}
local current = redis.call('GET',KEYS[1])
if current then
    return {0, current}
end
redis.call('SET', KEYS[1], ARGV[1], 'PX', tonumber(ARGV[2]))
return {1, ARGV[1]}
"""

# ================ 4. Lua 脚本二：令牌桶限流（必须是原子的）================
LUA_TOKEN_BUCKET = """
-- KEYS[1] = 桶的键
-- ARGV = 容量, 每秒补充, 当前时间(毫秒), 本次消耗, 过期时间(毫秒)
-- 返回 {是否放行(1/0), 剩余令牌数}
local capacity = tonumber(ARGV[1])
local refill   = tonumber(ARGV[2])
local now      = tonumber(ARGV[3])
local cost     = tonumber(ARGV[4])
local ttl      = tonumber(ARGV[5])

-- 取出桶里的令牌数和上次访问时间
local bucket = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(bucket[1])
local ts     = tonumber(bucket[2])

-- 第一次访问：给一个满桶
if tokens == nil or ts ==nil then
    tokens = capacity
    ts = now
end

-- 按经过的时间补充令牌，但最多补到桶容量（这就是"令牌桶"允许突发的原理）
local elapsed = math.max(0, now-ts)/1000.0
tokens = math.min(capacity, tokens+elapsed*refill)

local allowed = 0
if tokens >=cost then
    tokens = tokens-cost
    allowed = 1
end

redis.call('HSET',KEYS[1],'tokens',tokens,'ts',now)
redis.call('PEXPIRE',KEYS[1],ttl)
return {allowed,math.floor(tokens)}
"""
# register_script：把脚本预先注册到 Redis，之后调用只发脚本的哈希值，
# 不用每次都把整段脚本传过去（生产环境必须这么做）
idempotent_script = redis_client.register_script(LUA_IDEMPOTENT)
token_bucket_script = redis_client.register_script(LUA_TOKEN_BUCKET)


# 统一错误响应信息的格式
def error_response(code,message,status,details=None):
    return jsonify({
        "error":{
            "code":code,
            "message":message,
            "details":details if details else ""
        }
    }),status

# redis的自检
def ensure_redis_alive():
    try:
        redis_client.ping()
    except redis.exceptions.RedisError as exc:
        print(f"连接不上Redis,原因{type(exc).__name__}:{exc}")
        sys.exit(1) # 非零退出码 = 启动失败，明确告诉调用方

@app.post("/api/v1/posts")
def submit_post():
    if not request.is_json:
        return error_response("bad_request","请求体必须是JSON格式",400)
    # get_json(silent=True)：把 JSON 请求体解析成字典；
    # silent=True 表示解析失败不抛异常，而是返回 None，方便我们自己报 400
    body = request.get_json(silent=True)
    if body is None:
        return error_response("bad_request","JSON 格式错误",400)
    # 检查必填字段是不是填上了
    for field in ["client_msg_id", "author_id", "title", "content"]:
        if not body.get(field):
            return error_response("bad_request", f"缺少必填字段: {field}", 400)
    if len(body["content"])>20000:
        return error_response("bad_request", "content 超过 20000 字", 400)
    client_msg_id = body["client_msg_id"]
    author_id = body["author_id"]
    post_id = uuid.uuid4().hex
    # --- 7.1 幂等（Lua 版）---
    # 顺序说明：幂等放在限流**之前**。
    # 因为重复提交是同一次业务意图的重试，它不该消耗令牌；
    # 而且被幂等拦下的请求不会进入后面的流程，天然不会放大流量。
    first, existing_post_id = idempotent_script(
        keys = [KEY_IDEM+client_msg_id],
        args = [post_id,str(IDEM_TTL_SECONDS *1000)]
    )
    if int(first)==0:
        return jsonify({
            "post_id": existing_post_id,
            "status": "duplicated",
            "hint": "相同 client_msg_id 已受理过，返回首次结果",
        }),200
    now_ms = int(time.time() * 1000)
    allowed,remaining = token_bucket_script(
        keys=[KEY_IDEM + author_id],
        args=[
            str(RATE_CAPACITY),
            str(RATE_REFILL_PER_SEC),
            str(now_ms),
            str(RATE_COST),
            str(RATE_TTL_SECONDS * 1000),
        ]
    )
    if int(allowed)==0:
        return error_response(
            "rate_limited",
            "提交过于频繁，请稍后再试",
            429,      # 429 = Too Many Requests，限流的标准状态码
            details={
                "remaining_tokens": int(remaining),
                "retry_after_seconds": round(1 / RATE_REFILL_PER_SEC, 1),
            },
        )
    
    post = {
        "post_id": post_id,
        "client_msg_id": client_msg_id,
        "author_id": author_id,
        "title": body["title"],
        "content": body["content"],
    }
    redis_client.set(KEY_POST+post_id,json.dumps(post,ensure_ascii=False))
    return jsonify({"post_id": post_id, "status": "accepted","remaining_tokens": int(remaining),}), 202

@app.get("/posts/<post_id>")
def get_post(post_id):
    post = redis_client.get(KEY_POST+post_id)
    if post is None:
        return error_response("not_found", "稿件不存在", 404)
    return jsonify(json.loads(post)),200

@app.get("/healthz")
def health():
    try:
        redis_client.ping()
    except redis.exceptions.RedisError:
        return jsonify({"status": "error", "redis": "redis没活了"}), 503
    return jsonify({"status":"ok","redis": "ok"}),200

@app.errorhandler(404)
def handle_404(error):
    return error_response("not_found","接口不存在",404)

if __name__ == "__main__":
    ensure_redis_alive()
    print("Redis 连接正常，服务地址 http://127.0.0.1:8000")
    print(f"限流参数：桶容量={RATE_CAPACITY}，每秒补充={RATE_REFILL_PER_SEC}")
    app.run(host="127.0.0.1",port=8000,debug=True,use_reloader=False)
 