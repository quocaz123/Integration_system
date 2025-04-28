import time
import logging
from services.config import Config

logger = logging.getLogger(__name__)

class CircuitBreaker:
    def __init__(self):
        self.failure_count = 0
        self.success_count = 0
        self.failure_threshold = Config.FAILURE_THRESHOLD
        self.success_threshold = 3  # Số lần thành công cần thiết để đóng circuit
        self.reset_timeout = Config.RESET_TIMEOUT
        self.last_failure_time = None
        self.last_success_time = None
        self.state = 'CLOSED'

    def can_execute(self):
        """Kiểm tra xem có thể thực hiện operation không"""
        if self.state == 'OPEN':
            if time.time() - (self.last_failure_time or 0) > self.reset_timeout:
                logger.info("Circuit breaker chuyển sang trạng thái HALF-OPEN")
                self.state = 'HALF-OPEN'
                return True
            logger.warning("Circuit breaker đang mở, không thể thực hiện operation")
            return False
        return True

    def record_failure(self):
        """Ghi nhận lỗi và cập nhật trạng thái circuit breaker"""
        self.failure_count += 1
        self.last_failure_time = time.time()
        logger.warning(f"Ghi nhận lỗi thứ {self.failure_count}")
        
        if self.failure_count >= self.failure_threshold:
            self.state = 'OPEN'
            logger.error(f"Circuit breaker chuyển sang trạng thái OPEN sau {self.failure_count} lỗi")
        
        # Reset success count khi có lỗi
        self.success_count = 0

    def record_success(self):
        """Ghi nhận thành công và cập nhật trạng thái circuit breaker"""
        self.success_count += 1
        self.last_success_time = time.time()
        logger.info(f"Ghi nhận thành công thứ {self.success_count}")
        
        if self.state == 'HALF-OPEN' and self.success_count >= self.success_threshold:
            self.state = 'CLOSED'
            self.failure_count = 0
            logger.info("Circuit breaker chuyển sang trạng thái CLOSED sau nhiều lần thành công")

class RetryStrategy:
    def __init__(self):
        self.max_retries = Config.MAX_RETRIES
        self.base_delay = Config.BASE_DELAY

    def should_retry(self, retries):
        """Kiểm tra xem có nên thử lại không"""
        should = retries < self.max_retries
        if should:
            logger.info(f"Sẽ thử lại lần {retries + 1}/{self.max_retries}")
        else:
            logger.warning(f"Đã vượt quá số lần thử lại tối đa ({self.max_retries})")
        return should

    def get_delay(self, retries):
        """Tính toán thời gian chờ trước khi thử lại"""
        delay = self.base_delay * (2 ** retries)
        logger.info(f"Thời gian chờ trước khi thử lại: {delay}s")
        return delay 