import redis
import json
import logging
import time
from typing import Optional, Dict
from .config import Config

logger = logging.getLogger(__name__)

class RedisService:
    _instance = None
    _max_retries = 3
    _retry_delay = 1  # seconds

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._connect_with_retry()
        return cls._instance

    def _connect_with_retry(self):
        """Kết nối Redis với cơ chế retry"""
        for attempt in range(self._max_retries):
            try:
                self.client = redis.Redis(
                    host=Config.REDIS_HOST,
                    port=Config.REDIS_PORT,
                    db=Config.REDIS_DB,
                    decode_responses=True,
                    socket_timeout=5,
                    socket_connect_timeout=5
                )
                # Test kết nối
                self.client.ping()
                logger.info("Kết nối Redis thành công")
                return
            except redis.ConnectionError as e:
                if attempt < self._max_retries - 1:
                    logger.warning(f"Lỗi kết nối Redis (lần {attempt + 1}/{self._max_retries}): {e}")
                    time.sleep(self._retry_delay)
                else:
                    logger.error(f"Không thể kết nối Redis sau {self._max_retries} lần thử: {e}")
                    raise

    def _execute_with_retry(self, operation, *args, **kwargs):
        """Thực hiện operation với cơ chế retry"""
        for attempt in range(self._max_retries):
            try:
                return operation(*args, **kwargs)
            except redis.RedisError as e:
                if attempt < self._max_retries - 1:
                    logger.warning(f"Lỗi Redis (lần {attempt + 1}/{self._max_retries}): {e}")
                    time.sleep(self._retry_delay)
                    self._connect_with_retry()  # Thử kết nối lại
                else:
                    logger.error(f"Lỗi Redis sau {self._max_retries} lần thử: {e}")
                    raise

    def push_to_queue(self, queue_name: str, data: dict) -> bool:
        try:
            return self._execute_with_retry(
                self.client.rpush,
                queue_name,
                json.dumps(data)
            )
        except Exception as e:
            logger.error(f"Lỗi khi push vào queue {queue_name}: {e}")
            return False

    def pop_from_queue(self, queue_name: str) -> Optional[dict]:
        try:
            data = self._execute_with_retry(
                self.client.blpop,
                queue_name,
                timeout=1
            )
            if data:
                return json.loads(data[1])
            return None
        except Exception as e:
            logger.error(f"Lỗi khi pop từ queue {queue_name}: {e}")
            return None

    def set_status(self, key: str, data: dict, expire: int = 3600) -> bool:
        try:
            return self._execute_with_retry(
                self.client.set,
                key,
                json.dumps(data),
                ex=expire
            )
        except Exception as e:
            logger.error(f"Lỗi khi set status cho key {key}: {e}")
            return False

    def get_status(self, key: str) -> Optional[dict]:
        try:
            data = self._execute_with_retry(
                self.client.get,
                key
            )
            return json.loads(data) if data else None
        except Exception as e:
            logger.error(f"Lỗi khi get status cho key {key}: {e}")
            return None 