import time
import logging
import requests
import json
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from datetime import datetime
from .config import Config
from .redis_service import RedisService
from utils.circuit_breaker import CircuitBreaker, RetryStrategy
from .employee_service import merge_employee_data

# Tắt log của werkzeug và urllib3
logging.getLogger('werkzeug').setLevel(logging.ERROR)
logging.getLogger('urllib3').setLevel(logging.ERROR)
logging.getLogger('requests').setLevel(logging.ERROR)

logger = logging.getLogger(__name__)

class BaseConsumer(ABC):
    def __init__(self, api_url: str, service_name: str):
        self.api_url = api_url
        self.service_name = service_name
        self.redis = RedisService()
        self.circuit_breaker = CircuitBreaker()
        self.retry_strategy = RetryStrategy()
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })

    @abstractmethod
    def transform_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        pass

    def _log_request(self, data: Dict[str, Any]):
        """Log thông tin request"""
        logger.info(f"\n{'='*50}\nGửi request đến {self.service_name}:")
        logger.info(f"URL: {self.api_url}")
        logger.info(f"Data: {json.dumps(data, indent=2, ensure_ascii=False)}")
        logger.info(f"{'='*50}")

    def _log_response(self, response: requests.Response):
        """Log thông tin response"""
        logger.info(f"\n{'='*50}\nNhận response từ {self.service_name}:")
        logger.info(f"Status: {response.status_code}")
        try:
            response_data = response.json()
            logger.info(f"Data: {json.dumps(response_data, indent=2, ensure_ascii=False)}")
        except:
            logger.info(f"Text: {response.text}")
        logger.info(f"{'='*50}")

    def process_message(self, message: Dict[str, Any]) -> bool:
        """Xử lý message với cơ chế retry và circuit breaker"""
        if not self.circuit_breaker.can_execute():
            logger.warning(f"Circuit breaker đang mở cho {self.service_name}")
            return False

        try:
            data = self.transform_data(message['data'])
            self._log_request(data)
            
            response = self.session.post(self.api_url, json=data, timeout=10)
            self._log_response(response)
            
            if response.status_code == 200:
                try:
                    response_data = response.json()
                    if response_data.get('success'):
                        self._update_success_status(message)
                        self.circuit_breaker.record_success()
                        logger.info(f"Xử lý message thành công cho {self.service_name}")
                        return True
                    else:
                        error_msg = response_data.get('error', 'Unknown error')
                        logger.error(f"API {self.service_name} trả về lỗi: {error_msg}")
                        raise Exception(error_msg)
                except json.JSONDecodeError:
                    logger.error(f"Không thể parse JSON từ response của {self.service_name}")
                    raise Exception("Invalid JSON response")
            else:
                raise Exception(f"API error: {response.text}")

        except Exception as e:
            self.circuit_breaker.record_failure()
            retries = message['processing_status'][self.service_name].get('retries', 0)
            
            if self.retry_strategy.should_retry(retries):
                self._update_retry_status(message, str(e))
                delay = self.retry_strategy.get_delay(retries)
                logger.warning(f"Lỗi khi xử lý message, thử lại sau {delay}s: {e}")
                time.sleep(delay)
                return False
            else:
                self._move_to_failed_queue(message, str(e))
                return False

    def _update_success_status(self, message: Dict[str, Any]):
        """Cập nhật trạng thái thành công"""
        message['processing_status'][self.service_name] = {
            "status": "SUCCESS",
            "completed_at": datetime.now().isoformat()
        }
        self.redis.set_status(f"employee_create:{message['id']}", message)
        
        try:
            result = merge_employee_data()
            if result and 'merged_data' in result:
                self.redis.client.set('merged_employee_list', json.dumps(result['merged_data']))
                if 'stats' in result:
                    self.redis.client.set('merged_stats', json.dumps(result['stats']))
                logger.info("Đã cập nhật dữ liệu merge thành công")
        except Exception as e:
            logger.error(f"Lỗi khi cập nhật dữ liệu merge: {e}")

    def _update_retry_status(self, message: Dict[str, Any], error: str):
        """Cập nhật trạng thái retry"""
        retries = message['processing_status'][self.service_name].get('retries', 0)
        message['processing_status'][self.service_name] = {
            "status": "FAILED",
            "retries": retries + 1,
            "last_retry": datetime.now().isoformat(),
            "error": error
        }
        message['metadata']['total_retries'] += 1
        self.redis.push_to_queue(Config.QUEUE_NAME, message)

    def _move_to_failed_queue(self, message: Dict[str, Any], error: str):
        """Di chuyển message vào failed queue"""
        message['processing_status'][self.service_name] = {
            "status": "FAILED",
            "error": error,
            "failed_at": datetime.now().isoformat()
        }
        message['status'] = 'FAILED'
        message['error'] = error
        self.redis.push_to_queue(Config.FAILED_QUEUE_NAME, message)
        logger.error(f"Message đã được chuyển vào failed queue: {error}")

    def run(self):
        """Chạy consumer"""
        logger.info(f"Bắt đầu consumer cho {self.service_name}")
        while True:
            try:
                message = self.redis.pop_from_queue(Config.QUEUE_NAME)
                if message and message['processing_status'][self.service_name]['status'] == 'PENDING':
                    self.process_message(message)
                time.sleep(1)
            except Exception as e:
                logger.error(f"Lỗi trong consumer {self.service_name}: {e}")
                time.sleep(5)  # Đợi 5s trước khi thử lại

class API1Consumer(BaseConsumer):
    def __init__(self):
        super().__init__(Config.API1_CREATE_EMPLOYEE, 'api1')

    def transform_data(self, data):
        return {
            "Employee_ID": data.get("employeeNumber"),
            "First_Name": data.get("firstName"),
            "Last_Name": data.get("lastName"),
            "Email": data.get("email"),
            "Phone_Number": data.get("phoneNumber"),
            "Address1": data.get("address"),
            "City": data.get("city"),
            "State": data.get("state"),
            "Zip": data.get("zip"),
            "Gender": data.get("gender"),
            "Benefit_Plans": data.get("benefitPlans"),
            "Ethnicity": data.get("ethnicity")
        }

class API2Consumer(BaseConsumer):
    def __init__(self):
        super().__init__(Config.API2_CREATE_EMPLOYEE, 'api2')

    def transform_data(self, data):
        return {
            "employeeNumber": data.get("employeeNumber"),
            "lastName": data.get("lastName"),
            "firstName": data.get("firstName"),
            "ssn": data.get("ssn"),
            "payRate": data.get("payRate", "0"),
            "payRatesId": data.get("payRatesId", "0"),
            "vacationDays": data.get("vacationDays", "0"),
            "paidToDate": data.get("paidToDate", "0"),
            "paidLastYear": data.get("paidLastYear", "0")
        } 