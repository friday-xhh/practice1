# -*- coding: utf-8 -*-
"""健康检查蓝图。

Blueprint（蓝图）是什么：
    一组相关路由的集合。先在这个文件里定义好，最后在 app.py 里
    `app.register_blueprint(health_bp)` 挂到主应用上。

    好处是路由可以分散到多个文件 —— 想象一下如果 step03 那 249 行
    继续膨胀到 1000 行，找一个路由会有多痛苦。
"""
from flask import Blueprint,current_app,jsonify
from logging_setup import get_logger
logger = get_logger("health")
# 参数1 = 蓝图名（内部标识，url_for 会用到）
# 参数2 = 模块名（Flask 用它定位模板/静态文件）
health_bp = Blueprint("health",__name__)

# 依赖检查
def check_redis(app)->tuple[bool,str]:
    try:
        ok = app.extensions["redis_client"].ping()
    except Exception as exc:
        return False,f"{type(exc).__name__}:{exc}"
    return (True,"ok") if ok else (False, "ping 返回失败")

def check_kafka(app)->tuple[bool, str]:
    return app.extensions["Kafka_producer"].health_check()

def run_dependency_checks(app)->dict[str,tuple[bool, str]]:
    return {
        "redis": check_redis(app),
        "kafka": check_kafka(app),
    }

# 路由
# def health():
#     """健康检查：顺便确认redis还活着"""
#     # current_app 指代"当前正在处理请求的那个应用实例"。
#     # 工厂模式下没有全局 app 变量，只能通过它访问挂在 app 上的资源。
#     redis_client = current_app.extensions["redis_client"]
#     # error 级别：这类日志通常要配告警，不能只是安静地失败
#     logger.error("health.redis_unreachable")
#     if not redis_client.ping():
#         return jsonify({"status": "error", "redis": "unreachable"}),503
#     return jsonify({"status":"ok","redis":"ok"}),200

@health_bp.get("/healthz")
def liveness():
    # 查是否可以顺利的http响应
    return jsonify({"status":"ok", "alive":True}),200

@health_bp.get("/readyz")
def readiness():
    """就绪探针：逐个检查依赖，并且在优雅退出期间主动变红。"""
    draining = current_app.extensions.get("draining")
    if draining is not None and draining.is_set():
        logger.info("health.draining")
        return jsonify({"status":"draining","ready":False}),503
    results = run_dependency_checks(current_app)
    details = {name: {"ok": ok, "detail": msg} for name, (ok, msg) in results.items()}

    failed = [name for name, (ok, _msg) in results.items() if not ok]
    if failed:
        for name in failed:
            # error 级别：这类日志通常要配告警，不能只是安静地失败
            logger.error(
                "health.dependency_unavailable",
                dependency=name,
                detail=results[name][1],
            )
        return jsonify({
            "status": "error",
            "ready": False,
            "failed": failed,
            "dependencies": details,
        }),503
    return jsonify({"status": "ok", "ready": True, "dependencies": details}), 200


