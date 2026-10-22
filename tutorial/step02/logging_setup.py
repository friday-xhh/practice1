# -*- coding: utf-8 -*-
"""结构化日志 + request_id 全链路透传。

为什么要换掉 print：
    1. print 就是一行文本，机器没法解析，没法检索
    2. 分不出级别（哪些是普通信息、哪些是警告、哪些是错误）
    3. **看不出哪些日志属于同一次请求**  <- 这条最要命

结构化日志：每条日志 = 一个事件名 + 若干字段，输出成 JSON。
系统（ELK / Loki / 云厂商日志服务）可以直接采集和按字段检索。

request_id 为什么用 contextvars 而不是 Flask 的 g：
    g 只在 Flask 请求内部有效。而 contextvars 是 Python 标准库，
    脱离 Web 框架照样能用 —— 以后在 Kafka 消费端、定时任务里
    也能用同样的方式打日志。这是它最大的价值。
"""
from __future__ import annotations
import logging 
import sys
from contextvars import ContextVar
import structlog
from structlog.typing import Processor

# 请求/响应头里传递request_id用的名字
REQUEST_ID_HEADER = "X-Request-ID"
# ContextVar:每个请求（或每个协程）持有各自独立的值，互不干扰。
# 默认值“-”表示“当前不在任何请求的上下文中”。
_request_id_var:ContextVar[str] = ContextVar("request_id",default="-")
def get_request_id()->str:
    """取出当前上下文的request_id"""
    return _request_id_var.get()
def bind_request_id(request_id:str):
    """把request_id 绑定到当前上下文。
    绑定之后，这个请求处理过程中打的所有日志都会自动带上它，
    不需要每次手动传参——这是contextvars最方便的地方。
    """
    _request_id_var.set(request_id)
    structlog.contextvars.bind_contextvars(request_id=request_id)
def clear_request_id()->None:
    """请求结束时清理，防止串到下一个请求"""
    structlog.contextvars.clear_contextvars()
    _request_id_var.set("-")
def configure_logging(level: str = "INFO",json_output: bool = True)->None:
    """初始化日志。服务启动时调用一次即可"""
    level_int = int(getattr(logging,str(level).upper(),logging.INFO))
    processors: list[Processor] = [
        # 把 bind_request_id塞进去的字段合并到每条日志里
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso",utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]
    # JSON 给机器读（生产），console给人看
    renderer:Processor=(
        structlog.processors.JSONRenderer()
        if json_output
        else structlog.dev.ConsoleRenderer(colors=False)
    )
    structlog.configure(
        processors=[*processors,renderer],
        # 按级别过滤：低于level的日志直接丢弃
        wrapper_class=structlog.make_filtering_bound_logger(level_int),
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=False,
    )
    # structlog 渲染好的字符串交给标准库handler输出
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(message)s"))
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level_int)
    # 第三方库的内部日志往往非常啰嗦（kafka 会打印连接探测、版本协商……），
    # 全放出来会把业务日志淹掉。统一压到 WARNING：出问题时才看得见。
    for noisy in ("kafka", "kafka.conn", "kafka.client", "kafka.producer"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

def get_logger(name:str = "app"):
    """"获取一个logger。用法：logger.info("事件名",字段=值)"""
    return structlog.get_logger(name)