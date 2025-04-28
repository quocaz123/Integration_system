import uuid
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from .redis_service import RedisService
from .config import Config
import json
import requests

logger = logging.getLogger(__name__)

class QueueService:
    def __init__(self):
        self.redis = RedisService()
        self.max_retries = Config.MAX_RETRIES
        self.base_delay = Config.BASE_DELAY

    def create_message(self, employee_data: Dict[str, Any]) -> Dict[str, Any]:
        """Tạo message mới với thông tin đầy đủ"""
        request_id = str(uuid.uuid4())
        return {
            "id": request_id,
            "timestamp": datetime.now().isoformat(),
            "status": "PENDING",
            "data": employee_data,
            "processing_status": {
                "api1": {
                    "status": "PENDING",
                    "retries": 0,
                    "last_retry": None,
                    "error": None
                },
                "api2": {
                    "status": "PENDING",
                    "retries": 0,
                    "last_retry": None,
                    "error": None
                }
            },
            "metadata": {
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat(),
                "total_retries": 0
            }
        }

    def enqueue_employee(self, employee_data: Dict[str, Any]) -> str:
        """Thêm employee vào queue"""
        try:
            message = self.create_message(employee_data)
            if self.redis.push_to_queue(Config.QUEUE_NAME, message):
                logger.info(f"Đã thêm employee vào queue với ID: {message['id']}")
                return message["id"]
            else:
                raise Exception("Không thể thêm vào queue")
        except Exception as e:
            logger.error(f"Lỗi khi thêm employee vào queue: {e}")
            raise

    def get_message_status(self, message_id: str) -> Optional[Dict[str, Any]]:
        """Lấy trạng thái của message"""
        try:
            return self.redis.get_status(f"employee_create:{message_id}")
        except Exception as e:
            logger.error(f"Lỗi khi lấy trạng thái message {message_id}: {e}")
            return None

    def update_message_status(self, message_id: str, status: Dict[str, Any]) -> bool:
        """Cập nhật trạng thái của message"""
        try:
            current_status = self.get_message_status(message_id)
            if current_status:
                current_status.update(status)
                current_status['metadata']['updated_at'] = datetime.now().isoformat()
                return self.redis.set_status(f"employee_create:{message_id}", current_status)
            return False
        except Exception as e:
            logger.error(f"Lỗi khi cập nhật trạng thái message {message_id}: {e}")
            return False

    def move_to_failed_queue(self, message: Dict[str, Any], error: str) -> bool:
        """Di chuyển message bị lỗi vào failed queue"""
        try:
            message['status'] = 'FAILED'
            message['error'] = error
            message['metadata']['updated_at'] = datetime.now().isoformat()
            return self.redis.push_to_queue(Config.FAILED_QUEUE_NAME, message)
        except Exception as e:
            logger.error(f"Lỗi khi di chuyển message vào failed queue: {e}")
            return False

    def update_employee(self, api1_url: str, employee_number: str, api1_data: Dict[str, Any], api2_url: str, id_employee: str, api2_data: Dict[str, Any]) -> bool:
        """Cập nhật thông tin employee"""
        try:
            logger.info(f"[UPDATE][API1] URL: {api1_url}")
            logger.info(f"[UPDATE][API1] employeeNumber: {employee_number}")
            logger.info(f"[UPDATE][API1] Data: {json.dumps(api1_data, ensure_ascii=False)}")
            logger.info(f"[UPDATE][API2] URL: {api2_url}")
            logger.info(f"[UPDATE][API2] idEmployee: {id_employee}")
            logger.info(f"[UPDATE][API2] Data: {json.dumps(api2_data, ensure_ascii=False)}")
            return True
        except Exception as e:
            logger.error(f"Lỗi khi cập nhật thông tin employee: {e}")
            return False

class QueueEmployeeService:
    def __init__(self):
        self.api1_url = Config.API1_CREATE_EMPLOYEE
        self.api2_url = Config.API2_CREATE_EMPLOYEE
        self.redis = RedisService()

    def transform_for_api1(self, employee_data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "Employee_ID": employee_data.get("employeeNumber", ""),
            "First_Name": employee_data.get("firstName", ""),
            "Last_Name": employee_data.get("lastName", ""),
            "Email": employee_data.get("email", ""),
            "Phone_Number": employee_data.get("phoneNumber", ""),
            "City": employee_data.get("city", ""),
            "Gender": employee_data.get("gender", "")
        }

    def transform_for_api2(self, employee_data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "idEmployee": employee_data.get("idEmployee", ""),
            "firstName": employee_data.get("firstName", ""),
            "lastName": employee_data.get("lastName", ""),
            "email": employee_data.get("email", ""),
            "phoneNumber": employee_data.get("phoneNumber", ""),
            "city": employee_data.get("city", ""),
            "ssn": employee_data.get("ssn", ""),
            "payRate": employee_data.get("payRate", ""),
            "vacationDays": employee_data.get("vacationDays", ""),
            "gender": employee_data.get("gender", "")
        } 