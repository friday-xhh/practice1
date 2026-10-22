from __future__ import annotations
import json
from typing import Any,Callable

from kafka import KafkaConsumer,KafkaProducer
from kafka.admin import KafkaAdminClient,NewTopic
from kafka.errors import KafkaError,TopicAlreadyExistsError

from logging_setup import (
    REQUEST_ID_HEADER,
    bind_request_id,
    clear_request_id,
    get_logger,
)
logger = get_logger("kafka")

REQUEST_ID_MESSAGE_HEADER = REQUEST_ID_HEADER

class kafkaUnavailable(Exception):
    """Kafka不可用。业务层捕获它并返回503。"""

def _encode(value:dict)->bytes:
    """业务穿dict，这里统一装成JSON字节"""
    return json.dumps(value,ensure_ascii=False).encode("utf-8")

def _decode(raw:bytes)->dict:
    return json.loads(raw.decode("utf-8"))

def ensure_topic(config:Any,num_partitions: int = 3):
    """确保topic存在。不存在就创建，已存在则跳过。"""
    admin = KafkaAdminClient(
        bootstrap_servers=config.KAFKA_BOOTSTRAP.split(","),
        client_id="content-platform-admin",
        request_timeout_ms=15000,
    )
    try:
        admin.create_topics([
            NewTopic(
                name=config.KAFKA_TOPIC,
                num_partitions=num_partitions,# 分区数
                replication_factor=1, # 副本数
            )
        ])
        logger.info("topic.created",topic=config.KAFKA_TOPIC,partitions=num_partitions)
    except TopicAlreadyExistsError:
        logger.info("topic.exists", topic=config.KAFKA_TOPIC)
    finally:
        admin.close()

# 生产者
class KafkaProducerClient:
    """投递消息。"""
    def __init__(self,config:Any):
        self._config = config
        self._error:str | None = None
        try:
            self._producer = KafkaProducer(
                bootstrap_servers=config.KAFKA_BOOTSTRAP.split(","),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                value_serializer=_encode,
                #----可靠性三件套----
                acks="all",# 所有副本写成功才算成功
                retries=3,# 网络抖动自动重试
                # 重试时保证同一分区内不乱序（见文件头第 2 点）
                max_in_flight_requests_per_connection=1,
                # ---- 性能 ----
                linger_ms=5,              # 攒 5ms 再发，小批打包提高吞吐，延迟几乎无感
                request_timeout_ms=15000,
                # 元数据不可用 / 缓冲区满时，send() 最多阻塞这么久就报错。
                # 默认 60 秒太久了：下游挂掉时请求会全部堆在这里。
                # 这个值同时决定了 health_check() 的最坏耗时（见下方方法）
                max_block_ms=config.HEALTH_CHECK_TIMEOUT_MS,
            )
        except KafkaError as exc:
            self._producer = None
            self._error = str(exc)
            logger.error("producer.init_failed", error=self._error)
    def is_ready(self)->bool:
        return self._producer is not None

    @property
    def error(self)->str|None:
        return self._error

    def health_check(self)->tuple[bool, str]:
        if self._producer is None:
            return False, self._error or "producer 未初始化"
        try:
            partitions = self._producer.partitions_for(self._config.KAFKA_TOPIC)
        except KafkaError as exc:
            return False,str(exc)
        if not partitions:
            return False,f"拿不到{self._config.KAFKA_TOPIC}的分区信息"
        return True, "ok"    
    
    def publish(self,event:dict,request_id:str,key:str|None = None):
        if self._producer is None:
            raise kafkaUnavailable(self._error or "prioducer 未初始化")
        future = self._producer.send(
            self._config.KAFKA_TOPIC,
            key = key,
            value = event,
            headers=[(REQUEST_ID_MESSAGE_HEADER, request_id.encode("utf-8"))]
        )
        try:
            future.get(timeout=10)
        except KafkaError as exc:
            logger.error("producer.publish_failed",error=str(exc))
            raise kafkaUnavailable(str(exc)) from exc
    def close(self):
        if self._producer is not None:
            self._producer.close(timeout=5)

# 消费者
def _extract_request_id(headers)->str|None:
    """从消息头里取出来上游传下来的request_id"""
    if not headers:
        return None
    for key,value in headers:
        if key==REQUEST_ID_MESSAGE_HEADER:
            return value.decode("utf-8") if isinstance(value,bytes) else str(value)
    return None

class KafkaConsumerClient:
    """消费消息。核心约定：业务处理成功后才提交offset"""
    def __init__(self, config:Any):
        self._config = config
        self._consumer = KafkaConsumer(
            config.KAFKA_TOPIC,
            bootstrap_servers=config.KAFKA_BOOTSTRAP.split(","),
            group_id=config.KAFKA_GROUP_ID,
            auto_offset_reset="earliest",

            # 手动提交offset
            enable_auto_commit=False,
            value_deserializer=_decode,
            key_deserializer=lambda k:k.decode("utf-8") if k else None,
            # 一次性最多拉去这么多条数据
            max_poll_records=50,
        )
    def consume(
            self,
            handler:Callable[[dict,dict],None],
            stop_event: Any = None,
            timeout_ms: int =1000,
    )->None:
        """循环拉取并处理消息
        :param handler:处理函数，签名(event:dict,meta:dict)->None
        :param stop_event:threading.Event,置位后优雅退出
        """
        # 用poll()而不是‘for msg in consumer’:
        # poll每返回一批，空手而归时也会按时返回，方便检查退出信号
        while stop_event is None or not stop_event.is_set():
            batches = self._consumer.poll(timeout_ms=timeout_ms)
            for messages in batches.values():
                for msg in messages:
                    self._handle_one(msg,handler)
    def _handle_one(self,msg,handler:Callable[[dict,dict],None]):
        request_id = _extract_request_id(msg.headers) or "-"
        bind_request_id(request_id)
        meta = {
            "topic":msg.topic,
            "partition":msg.partition,
            "offset":msg.offset,
            "request_id":request_id,
        }
        try:
            handler(msg.value,meta)
        except Exception:
            logger.exception("message.handle_failed",**meta)
            return 
        finally:
            clear_request_id()

        self._consumer.commit()
        logger.debug("offset.committed",**meta)
    def close(self):
        self._consumer.close()
        