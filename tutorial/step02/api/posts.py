# -*- coding: utf-8 -*-
"""投稿与查询蓝图。

这一步**业务逻辑一行没改**，只是从 app.py 搬到了这里。
这也验证了重构的意义：结构变了，行为没变。
"""
import time
import uuid
from flask import Blueprint, current_app, jsonify,request
from errors import error_response
from logging_setup import get_logger,get_request_id
from kafka_client import kafkaUnavailable

logger = get_logger("posts")
posts_bp = Blueprint("posts",__name__)

def _get_config():
    """取出挂在应用上的配置"""
    return current_app.extensions["config"]
def _get_redis():
    return current_app.extensions["redis_client"]
@posts_bp.post("/api/v1/posts")
def submit_post():
    """提交一篇稿件"""
    config = _get_config()
    redis_client = _get_redis()
    # ----------------------------------------- 校验数据集格式
    if not request.is_json:
        return error_response("bad_request", "请求体必须是 JSON", 400)

    body = request.get_json(silent=True)
    if body is None:
        return error_response("bad_request", "JSON 格式错误", 400)

    for field in ["client_msg_id", "author_id", "title", "content"]:
        if not body.get(field):
            return error_response("bad_request", f"缺少必填字段: {field}", 400)

    if len(body["content"]) > config.MAX_CONTENT_LENGTH:
        return error_response(
            "bad_request",
            f"content 超过 {config.MAX_CONTENT_LENGTH} 字",
            400,
        )

    client_msg_id = body["client_msg_id"]
    author_id = body["author_id"]
    post_id = uuid.uuid4().hex
    # --------------------------幂等--------------------------
    first,existing_post_id = redis_client.acquire_idempotency(client_msg_id,post_id)
    if not first:
        logger.info(
            "submit.duplicated",
            client_msg_id=client_msg_id,
            post_id=existing_post_id,
        )
        return jsonify({
            "post_id": existing_post_id,
            "status": "duplicated",
            "hint": "相同 client_msg_id 已受理过，返回首次结果",
        }),200
    # --------------------------限流---------------------------
    now_ms = int(time.time()*1000)
    allowed,remaining = redis_client.take_token(author_id, now_ms)
    if not allowed:
        logger.info(
            "submit.rate_limited",
            author_id=author_id,
            remaining_tokens=remaining,
        )
        return error_response(
            "rate_limited",
            "提交过于频繁，请稍后再试",
            429,
            details={
                "remaining_tokens": remaining,
                "retry_after_seconds": round(1 / config.RATE_REFILL_PER_SEC, 1),
            },
        )
    # --------------------------投递---------------------------
    event = {
        "post_id": post_id,
        "client_msg_id": client_msg_id,
        "author_id": author_id,
        "title": body["title"],
        "content": body["content"],
        "submitted_at": int(time.time() * 1000),
    }
    producer = current_app.extensions["kafka_producer"]
    try:
        producer.publish(event,request_id = get_request_id(),key = author_id)
    except kafkaUnavailable as exc:
        redis_client.release_idempotency(client_msg_id)# 释放幂等键
        logger.error(
            "submit.publish_failed",
            post_id=post_id,
            client_msg_id=client_msg_id,
            error=str(exc),
        )
        return error_response("kafka_unavailable", "投稿服务暂时不可用，请稍后重试", 503)
    redis_client.save_post_status(post_id, "queued", config.STATUS_TTL_SECONDS)
    logger.info(
        "submit.accepted",
        post_id=post_id,
        author_id=author_id,
        remaining_tokens=remaining,
        status="queued",
    )

    # 202 Accepted：已经收下，但还没处理完
    return jsonify({
        "post_id": post_id,
        "status": "queued",
        "remaining_tokens": remaining,
    }), 202
    # # ---------------------存稿件----------------------
    # post = {
    #     "post_id": post_id,
    #     "client_msg_id": client_msg_id,
    #     "author_id": author_id,
    #     "title": body["title"],
    #     "content": body["content"],
    # }
    # redis_client.save_post(post_id,post)
    
    # logger.info(
    #     "submit.accepted",
    #     post_id=post_id,
    #     author_id=author_id,
    #     remaining_tokens=remaining,
    # )

    # return jsonify({
    #     "post_id": post_id,
    #     "status": "accepted",
    #     "remaining_tokens": remaining,
    # }),202

@posts_bp.get("/posts/<post_id>")
def get_post(post_id):
    """按ID查询稿件"""
    redis_client = _get_redis()
    post = redis_client.get_post(post_id)
    if post is not None:
        # 带上 status，调用方不用靠"有没有 content 字段"去猜
        return jsonify({**post, "status": "published"}), 200
    status = redis_client.get_post_status(post_id)
    if status == "queued":
        return jsonify({
            "post_id": post_id,
            "status": "queued",
            "hint": "稿件已受理，正在处理中，请稍后重试",
        }), 200

    return error_response("not_found", "稿件不存在", 404)