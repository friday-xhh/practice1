"""
结构化日志 + request_id 全链路透传。

两个关键设计：
    1. JSON 输出，方便直接被 ELK / Loki 采集，无需再写解析规则；
    2. request_id 通过 contextvars 绑定到每条日志，并通过 Kafka 消息头
       传给下游服务，实现跨服务的链路串联（Jaeger 之前的最小可用方案）。
"""
from __future__ import annotations
import logging
import sys
from contextvars import ContextVar

import structlog
from structlog.typing import Processor

REQUEST_ID_HEADER = "X-Request-ID"

_request_id_var: ContextVar[str] = ContextVar[str]("request_id",default="-")

def get_request_id()->str:
    """获取当前请求的 request_id，非请求上下文中返回占位符"""
    return _request_id_var.get()

def build_request_id(request_id: str)->None:
    """把request_id 绑定到上下文，之后该协程/线程内所有日志自动携带"""
    _request_id_var.set(request_id)
    structlog.contextvars.bind_contextvars(request_id=request_id)

def clear_context()->None:
    """请求结束时清理，防止上下文泄漏到下一个请求"""
    structlog.contextvars.clear_contextvars()
    _request_id_var.set("-")

def _level_to_int(level: str|int)->int:
    """把任意形式输入的日志级别名level，转换成 `logging` 模块标准的**整数级别值**"""
    if isinstance(level,int):
        return level
    return int(getattr(logging,str(level).upper(),logging.INFO))

def configure_logging(level: str = "INFO",json_output:bool=True)->None:
    level_int: int=_level_to_int(level)
    shared_processors: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.UnicodeDecoder(),
        structlog.processors.format_exc_info,
    ]
    # Windows 控制台对ANSI颜色支持不稳定，开发模式下直接关掉
    renderer: Processor = (
        structlog.processors.JSONRenderer()
        if json_output
        else structlog.dev.ConsoleRenderer(colors=False)
    )
    structlog.configure(
        processors=[*shared_processors,renderer],
        wrapper_class=structlog.make_filtering_bound_logger(min_level=level_int),
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=False,
    )
    # structlog 渲染后的字符串交给stdlib handler 输出
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(fmt=logging.Formatter("%(message)s"))
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level_int)

    # 压制第三方库的噪声日志
    logging.getLogger("werkzeug").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)  

def get_logger(name: str = "app")->structlog.stdlib.BoundLogger:
    """获取结构化 logger"""
    return structlog.get_logger(name)
