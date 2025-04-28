import requests
import json
import time
import redis
from datetime import datetime
import logging
import os
from typing import Dict, Any, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
import asyncio
from aiohttp import ClientSession
import uuid

# Cấu hình logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Config:
    # Redis config
    REDIS_HOST = 'localhost'
    REDIS_PORT = 6379
    REDIS_DB = 0
    
    # API endpoints
    API1_BASE = 'http://localhost:19335'
    API2_BASE = 'http://localhost:8080'
    
    API1_CREATE = f"{API1_BASE}/API/CreateEmployee"
    API2_CREATE = f"{API2_BASE}/springapp/api/employee/add"
    
    # Queue names
    RETRY_QUEUE = "employee_retry_queue"
    TRANSACTION_QUEUE = "employee_transaction_queue"
    
    # Retry settings
    MAX_RETRIES = 3
    RETRY_DELAY = 5  # seconds
    
    # Transaction timeout
    TRANSACTION_TIMEOUT = 30  # seconds

class RedisService:
    def __init__(self):
        self.client = redis.Redis(
            host=Config.REDIS_HOST,
            port=Config.REDIS_PORT,
            db=Config.REDIS_DB,
            decode_responses=True
        )

    def add_to_retry_queue(self, data: Dict[str, Any], api_name: str) -> bool:
        try:
            retry_data = {
                "data": data,
                "api": api_name,
                "retries": 0,
                "last_retry": None,
                "created_at": datetime.now().isoformat(),
                "status": "PENDING"
            }
            self.client.rpush(Config.RETRY_QUEUE, json.dumps(retry_data))
            return True
        except Exception as e:
            logger.error(f"Error adding to retry queue: {e}")
            return False

    def add_to_failed_queue(self, data: Dict[str, Any], error: str) -> bool:
        try:
            failed_data = {
                "data": data,
                "error": error,
                "created_at": datetime.now().isoformat(),
                "status": "FAILED"
            }
            self.client.rpush("employee_failed_queue", json.dumps(failed_data))
            return True
        except Exception as e:
            logger.error(f"Error adding to failed queue: {e}")
            return False

    def get_from_retry_queue(self) -> Optional[Dict[str, Any]]:
        try:
            data = self.client.lpop(Config.RETRY_QUEUE)
            return json.loads(data) if data else None
        except Exception as e:
            logger.error(f"Error getting from retry queue: {e}")
            return None

    def get_from_failed_queue(self) -> Optional[Dict[str, Any]]:
        try:
            data = self.client.lpop("employee_failed_queue")
            return json.loads(data) if data else None
        except Exception as e:
            logger.error(f"Error getting from failed queue: {e}")
            return None

    def add_to_transaction_queue(self, transaction_id: str, data: Dict[str, Any]) -> bool:
        try:
            transaction_data = {
                "id": transaction_id,
                "data": data,
                "status": "PENDING",
                "created_at": datetime.now().isoformat(),
                "api1_status": "PENDING",
                "api2_status": "PENDING",
                "api1_data": None,
                "api2_data": None
            }
            self.client.set(f"transaction:{transaction_id}", json.dumps(transaction_data))
            return True
        except Exception as e:
            logger.error(f"Error adding to transaction queue: {e}")
            return False

    def update_transaction_status(self, transaction_id: str, status: str, api_status: Dict[str, str], api_data: Dict[str, Any] = None) -> bool:
        try:
            transaction_data = self.client.get(f"transaction:{transaction_id}")
            if transaction_data:
                data = json.loads(transaction_data)
                data["status"] = status
                data.update(api_status)
                if api_data:
                    data.update(api_data)
                self.client.set(f"transaction:{transaction_id}", json.dumps(data))
                return True
            return False
        except Exception as e:
            logger.error(f"Error updating transaction status: {e}")
            return False

    def get_transaction_status(self, transaction_id: str) -> Optional[Dict[str, Any]]:
        try:
            data = self.client.get(f"transaction:{transaction_id}")
            return json.loads(data) if data else None
        except Exception as e:
            logger.error(f"Error getting transaction status: {e}")
            return None

class EmployeeService:
    def __init__(self):
        self.redis = RedisService()
        self.executor = ThreadPoolExecutor(max_workers=2)

    async def create_employee_async(self, employee_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Tạo nhân viên mới bất đồng bộ
        """
        transaction_id = str(uuid.uuid4())
        self.redis.add_to_transaction_queue(transaction_id, employee_data)

        try:
            # Lấy ID mới cho cả hai API
            api1_id, api2_id = await self._get_next_employee_ids_async()
            
            # Format dữ liệu cho cả hai API
            api1_data = self._format_data_for_api1(employee_data)
            api2_data = self._format_data_for_api2(employee_data)
            
            # Cập nhật ID
            api1_data['Employee_ID'] = api1_id
            api2_data['employeeNumber'] = str(api2_id)

            # Lưu dữ liệu đã format vào transaction
            self.redis.update_transaction_status(transaction_id, "PENDING", {
                "api1_status": "PENDING",
                "api2_status": "PENDING"
            }, {
                "api1_data": api1_data,
                "api2_data": api2_data
            })

            # Gọi cả hai API đồng thời
            async with ClientSession() as session:
                api1_task = self._call_api1_async(session, api1_data)
                api2_task = self._call_api2_async(session, api2_data)
                
                api1_result, api2_result = await asyncio.gather(api1_task, api2_task)

            # Xử lý kết quả
            if api1_result["status"] == "SUCCESS" and api2_result["status"] == "SUCCESS":
                # Cả hai API thành công
                status = "SUCCESS"
                self.redis.update_transaction_status(transaction_id, status, {
                    "api1_status": "SUCCESS",
                    "api2_status": "SUCCESS"
                })
            elif api1_result["status"] == "FAILED" and api2_result["status"] == "FAILED":
                # Cả hai API thất bại
                status = "FAILED"
                self.redis.add_to_failed_queue(employee_data, f"API1: {api1_result.get('error')}, API2: {api2_result.get('error')}")
                self.redis.update_transaction_status(transaction_id, status, {
                    "api1_status": "FAILED",
                    "api2_status": "FAILED",
                    "api1_error": api1_result.get("error"),
                    "api2_error": api2_result.get("error")
                })
            else:
                # Một API thành công, một API thất bại
                status = "PARTIAL_SUCCESS"
                if api1_result["status"] == "SUCCESS":
                    self.redis.add_to_retry_queue(api2_data, "api2")
                else:
                    self.redis.add_to_retry_queue(api1_data, "api1")
                
                self.redis.update_transaction_status(transaction_id, status, {
                    "api1_status": api1_result["status"],
                    "api2_status": api2_result["status"],
                    "api1_error": api1_result.get("error"),
                    "api2_error": api2_result.get("error")
                })

            return {
                "transaction_id": transaction_id,
                "status": status,
                "api1_result": api1_result,
                "api2_result": api2_result
            }

        except Exception as e:
            logger.error(f"Error in create_employee_async: {e}")
            self.redis.add_to_failed_queue(employee_data, str(e))
            self.redis.update_transaction_status(transaction_id, "FAILED", {
                "api1_status": "FAILED",
                "api2_status": "FAILED",
                "api1_error": str(e),
                "api2_error": str(e)
            })
            return {
                "transaction_id": transaction_id,
                "status": "FAILED",
                "error": str(e)
            }

    async def _get_next_employee_ids_async(self) -> Tuple[int, int]:
        """
        Lấy ID mới cho cả hai API bất đồng bộ
        """
        async with ClientSession() as session:
            api1_task = self._get_last_id_api1_async(session)
            api2_task = self._get_last_id_api2_async(session)
            
            last_id_api1, last_id_api2 = await asyncio.gather(api1_task, api2_task)
            
            next_id_api1 = 1 if last_id_api1 is None else last_id_api1 + 1
            next_id_api2 = 1001 if last_id_api2 is None else last_id_api2 + 1
            
            return next_id_api1, next_id_api2

    async def _get_last_id_api1_async(self, session: ClientSession) -> Optional[int]:
        try:
            async with session.get(f"{Config.API1_BASE}/API/GetALlEmployees") as response:
                if response.status == 200:
                    data = await response.json()
                    if isinstance(data, dict) and 'data' in data:
                        employees = data['data']
                        valid_ids = [int(emp['Employee_ID']) for emp in employees 
                                   if isinstance(emp, dict) and 'Employee_ID' in emp 
                                   and str(emp['Employee_ID']).isdigit()]
                        return max(valid_ids) if valid_ids else None
        except Exception as e:
            logger.error(f"Error getting API1 last ID: {e}")
        return None

    async def _get_last_id_api2_async(self, session: ClientSession) -> Optional[int]:
        try:
            async with session.get(f"{Config.API2_BASE}/springapp/api/employee/list") as response:
                if response.status == 200:
                    data = await response.json()
                    if isinstance(data, dict) and 'data' in data:
                        employees = data['data']
                        valid_ids = [int(emp['employeeNumber']) for emp in employees 
                                   if isinstance(emp, dict) and 'employeeNumber' in emp 
                                   and str(emp['employeeNumber']).isdigit()]
                        return max(valid_ids) if valid_ids else None
        except Exception as e:
            logger.error(f"Error getting API2 last ID: {e}")
        return None

    async def _call_api1_async(self, session: ClientSession, data: Dict[str, Any]) -> Dict[str, Any]:
        try:
            async with session.post(Config.API1_CREATE, json=data) as response:
                if response.status == 200:
                    result = await response.json()
                    if isinstance(result, dict) and result.get('success'):
                        return {"status": "SUCCESS"}
                    return {"status": "FAILED", "error": result.get('message', 'Unknown error')}
                return {"status": "FAILED", "error": f"HTTP {response.status}"}
        except Exception as e:
            return {"status": "FAILED", "error": str(e)}

    async def _call_api2_async(self, session: ClientSession, data: Dict[str, Any]) -> Dict[str, Any]:
        try:
            async with session.post(Config.API2_CREATE, json=data) as response:
                if response.status == 200:
                    result = await response.json()
                    if isinstance(result, dict) and result.get('success'):
                        return {"status": "SUCCESS"}
                    return {"status": "FAILED", "error": result.get('message', 'Unknown error')}
                return {"status": "FAILED", "error": f"HTTP {response.status}"}
        except Exception as e:
            return {"status": "FAILED", "error": str(e)}

    def _format_data_for_api1(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "Employee_ID": int(data.get("employeeNumber", 0)),
            "First_Name": data.get("firstName", ""),
            "Last_Name": data.get("lastName", ""),
            "Email": data.get("email", ""),
            "Phone_Number": data.get("phoneNumber", ""),
            "Address1": data.get("address", ""),
            "City": data.get("city", ""),
            "State": data.get("state", ""),
            "Zip": data.get("zip", ""),
            "Gender": data.get("gender", True),
            "Shareholder_Status": False,
            "Benefit_Plans": 1,
            "Ethnicity": 1
        }

    def _format_data_for_api2(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "employeeNumber": str(data.get("employeeNumber", "")),
            "lastName": data.get("lastName", ""),
            "firstName": data.get("firstName", ""),
            "ssn": data.get("ssn", ""),
            "payRate": str(data.get("payRate", "0")),
            "payRatesId": "0",
            "vacationDays": str(data.get("vacationDays", "0")),
            "paidToDate": "0",
            "paidLastYear": "0"
        }

    async def retry_failed_creations_async(self):
        """
        Xử lý retry bất đồng bộ cho các tạo nhân viên thất bại
        """
        while True:
            retry_item = self.redis.get_from_retry_queue()
            if not retry_item:
                break

            if retry_item["retries"] >= Config.MAX_RETRIES:
                logger.error(f"Max retries reached for {retry_item['api']}")
                self.redis.add_to_failed_queue(retry_item["data"], f"Max retries reached for {retry_item['api']}")
                continue

            try:
                async with ClientSession() as session:
                    if retry_item["api"] == "api1":
                        response = await session.post(Config.API1_CREATE, json=retry_item["data"])
                    else:
                        response = await session.post(Config.API2_CREATE, json=retry_item["data"])

                    if response.status != 200:
                        retry_item["retries"] += 1
                        retry_item["last_retry"] = datetime.now().isoformat()
                        self.redis.add_to_retry_queue(retry_item["data"], retry_item["api"])
                        await asyncio.sleep(Config.RETRY_DELAY)
                    else:
                        logger.info(f"Retry successful for {retry_item['api']}")
                        # Cập nhật trạng thái transaction nếu có
                        if "transaction_id" in retry_item:
                            self.redis.update_transaction_status(
                                retry_item["transaction_id"],
                                "SUCCESS",
                                {f"{retry_item['api']}_status": "SUCCESS"}
                            )
            except Exception as e:
                logger.error(f"Error during retry: {e}")
                retry_item["retries"] += 1
                retry_item["last_retry"] = datetime.now().isoformat()
                self.redis.add_to_retry_queue(retry_item["data"], retry_item["api"])
                await asyncio.sleep(Config.RETRY_DELAY)

    async def process_failed_queue_async(self):
        """
        Xử lý các bản ghi thất bại từ failed queue
        """
        while True:
            failed_item = self.redis.get_from_failed_queue()
            if not failed_item:
                break

            try:
                # Thử tạo lại nhân viên
                result = await self.create_employee_async(failed_item["data"])
                if result["status"] == "SUCCESS":
                    logger.info(f"Successfully reprocessed failed employee: {failed_item['data']}")
                else:
                    # Nếu vẫn thất bại, thêm lại vào failed queue
                    self.redis.add_to_failed_queue(failed_item["data"], f"Reprocessing failed: {result.get('error')}")
            except Exception as e:
                logger.error(f"Error reprocessing failed employee: {e}")
                self.redis.add_to_failed_queue(failed_item["data"], str(e))

# Sử dụng service
async def main():
    service = EmployeeService()
    
    # Tạo nhân viên mới
    employee_data = {
        "firstName": "John",
        "lastName": "Doe",
        "email": "john.doe@example.com",
        "phoneNumber": "1234567890",
        "address": "123 Main St",
        "city": "New York",
        "state": "NY",
        "zip": "10001",
        "gender": True,
        "ssn": "123-45-6789",
        "payRate": "50000",
        "vacationDays": "20"
    }
    
    result = await service.create_employee_async(employee_data)
    print(json.dumps(result, indent=2))
    
    # Xử lý retry và failed queue
    await asyncio.gather(
        service.retry_failed_creations_async(),
        service.process_failed_queue_async()
    )

if __name__ == "__main__":
    asyncio.run(main()) 