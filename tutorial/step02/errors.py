# -*- coding: utf-8 -*-
"""统一错误响应格式。

约定：所有非 2xx 响应都长这样

    {"error": {"code": "...", "message": "...", "details": {...}}}

这样调用方只需要写一处解析逻辑，不用猜这次返回会是什么形状。

抽成独立文件的原因：error_response 会被多个蓝图用到，
放在任何一个蓝图里都会造成"别的蓝图要反向 import 它"的循环依赖。
"""
from flask import jsonify

def error_response(code: str,message: str,status: int,details: dict| None=None):
    """构造统一格式的错误响应"""
    payload = {"error":{"code":code,"message":message}}
    if details:
        payload["error"]["details"] = details
    return jsonify(payload),status
def register_error_handlers(app)->None:
    """把 Flask 自带的错误页也改成 JSON。
    默认访问不存在的路径会返回一段 HTML，但 API 的调用方期望 JSON。
    """
    @app.errorhandler(404)
    def handle_404(error): # noqa: ANN001 - Flask 回调签名固定
        return error_response("not_found","接口不存在",404)