import requests
import json
import time
import redis
from datetime import datetime
import logging
import os
from typing import Dict, Any, Optional

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
    
    # Retry settings
    MAX_RETRIES = 3
    RETRY_DELAY = 5  # seconds

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
                "created_at": datetime.now().isoformat()
            }
            self.client.rpush(Config.RETRY_QUEUE, json.dumps(retry_data))
            return True
        except Exception as e:
            logger.error(f"Error adding to retry queue: {e}")
            return False

    def get_from_retry_queue(self) -> Optional[Dict[str, Any]]:
        try:
            data = self.client.lpop(Config.RETRY_QUEUE)
            return json.loads(data) if data else None
        except Exception as e:
            logger.error(f"Error getting from retry queue: {e}")
            return None

class EmployeeCreator:
    def __init__(self):
        self.redis = RedisService()
        self.api1_url = "http://localhost:19335/API/CreateEmployee"
        self.api2_url = "http://localhost:8080/springapp/api/employee/add"

    def transform_for_api1(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "Employee_ID": data.get("employeeNumber", ""),
            "First_Name": data.get("firstName", ""),
            "Last_Name": data.get("lastName", ""),
            "Email": data.get("email", ""),
            "Phone_Number": data.get("phoneNumber", ""),
            "Address1": data.get("address", ""),
            "City": data.get("city", ""),
            "State": data.get("state", ""),
            "Zip": data.get("zip", ""),
            "Gender": data.get("gender", True),
            "Benefit_Plans": data.get("benefitPlans", 1),
            "Ethnicity": data.get("ethnicity", 1)
        }

    def transform_for_api2(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "employeeNumber": data.get("employeeNumber", ""),
            "lastName": data.get("lastName", ""),
            "firstName": data.get("firstName", ""),
            "ssn": data.get("ssn", ""),
            "payRate": str(data.get("payRate", "0")),
            "payRatesId": "0",
            "vacationDays": str(data.get("vacationDays", "0")),
            "paidToDate": "0",
            "paidLastYear": "0"
        }

    def _get_next_employee_ids(self) -> tuple:
        """
        Lấy ID cuối cùng từ mỗi API và tạo ID mới riêng cho từng API
        Returns:
            tuple: (api1_id, api2_id)
        """
        try:
            last_id_api1 = None
            last_id_api2 = None

            # Lấy danh sách từ API1
            try:
                api1_response = requests.get(
                    "http://localhost:19335/API/GetALlEmployees",
                    timeout=5
                )
                if api1_response.ok:
                    api1_data = api1_response.json()
                    logger.info(f"API1 response: {json.dumps(api1_data, indent=2, ensure_ascii=False)}")
                    
                    # Kiểm tra response format và lấy mảng data
                    if isinstance(api1_data, dict) and 'data' in api1_data and api1_data['data']:
                        employees = api1_data['data']
                        # Lọc ra các Employee_ID hợp lệ
                        valid_ids = []
                        for emp in employees:
                            if isinstance(emp, dict):
                                emp_id = emp.get('Employee_ID')
                                if emp_id is not None and str(emp_id).isdigit():
                                    valid_ids.append(int(emp_id))
                        
                        if valid_ids:
                            last_id_api1 = max(valid_ids)  # Lấy ID lớn nhất
                            logger.info(f"API1 valid IDs: {sorted(valid_ids)}")
                            logger.info(f"API1 last ID: {last_id_api1}")

            except Exception as e:
                logger.error(f"Error getting API1 employees: {str(e)}")

            # Lấy danh sách từ API2
            try:
                api2_response = requests.get(
                    "http://localhost:8080/springapp/api/employee/list",
                    timeout=5
                )
                if api2_response.ok:
                    api2_data = api2_response.json()
                    logger.info(f"API2 response: {json.dumps(api2_data, indent=2, ensure_ascii=False)}")
                    
                    if isinstance(api2_data, dict) and 'data' in api2_data and api2_data['data']:
                        employees = api2_data['data']
                        # Lọc ra các employeeNumber hợp lệ
                        valid_ids = []
                        for emp in employees:
                            if isinstance(emp, dict):
                                emp_num = emp.get('employeeNumber')
                                if emp_num and str(emp_num).isdigit():
                                    valid_ids.append(int(emp_num))
                        
                        if valid_ids:
                            last_id_api2 = max(valid_ids)  # Lấy ID lớn nhất
                            logger.info(f"API2 valid IDs: {sorted(valid_ids)}")
                            logger.info(f"API2 last ID: {last_id_api2}")

            except Exception as e:
                logger.error(f"Error getting API2 employees: {str(e)}")

            # Tạo ID mới cho API1
            next_id_api1 = 1
            if last_id_api1 is not None:
                next_id_api1 = last_id_api1 + 1
            logger.info(f"Generated next ID for API1: {next_id_api1}")

            # Tạo ID mới cho API2
            next_id_api2 = 1001
            if last_id_api2 is not None:
                next_id_api2 = last_id_api2 + 1
            logger.info(f"Generated next ID for API2: {next_id_api2}")
            
            return next_id_api1, next_id_api2

        except Exception as e:
            logger.error(f"Error in ID generation: {str(e)}")
            # Giá trị mặc định nếu có lỗi
            return 1, 1001

    def create_employee(self, employee_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Tạo nhân viên mới bằng cách gọi cả hai API
        """
        # Tạo ID mới cho nhân viên
        api1_id, api2_id = self._get_next_employee_ids()
        
        result = {
            'api1_status': 'PENDING',
            'api2_status': 'PENDING',
            'api1_error': None,
            'api2_error': None
        }
        
        # Format dữ liệu cho cả hai API
        api1_data = self._format_data_for_api1(employee_data)
        api2_data = self._format_data_for_api2(employee_data)
        
        # Cập nhật ID cho từng API
        api1_data['Employee_ID'] = api1_id
        api2_data['employeeNumber'] = str(api2_id)
        
        # Log dữ liệu sẽ gửi đi
        logger.info("========== REQUEST DATA ==========")
        logger.info(f"API1 URL: {self.api1_url}")
        logger.info(f"API1 Request Data: {json.dumps(api1_data, indent=2, ensure_ascii=False)}")
        logger.info("---------------------------------")
        logger.info(f"API2 URL: {self.api2_url}")
        logger.info(f"API2 Request Data: {json.dumps(api2_data, indent=2, ensure_ascii=False)}")
        logger.info("================================")
        
        # Gọi API 1
        try:
            api1_response = requests.post(
                self.api1_url,
                json=api1_data,
                timeout=5
            )
            logger.info(f"API1 Response Status: {api1_response.status_code}")
            logger.info(f"API1 Response: {api1_response.text}")
            
            # Phân tích response API1
            if api1_response.ok:
                try:
                    api1_json = api1_response.json()
                    logger.info(f"API1 parsed response: {json.dumps(api1_json, indent=2, ensure_ascii=False)}")
                    if isinstance(api1_json, dict):
                        # API1 trả về success=true và có message thành công
                        if api1_json.get('success') == True:
                            result['api1_status'] = 'SUCCESS'
                            result['api1_error'] = None
                            logger.info("API1 creation successful")
                        else:
                            result['api1_status'] = 'FAILED'
                            result['api1_error'] = api1_json.get('message') or 'Unknown error from API1'
                            logger.error(f"API1 creation failed: {result['api1_error']}")
                    else:
                        result['api1_status'] = 'FAILED'
                        result['api1_error'] = 'Invalid response format from API1'
                        logger.error("API1 invalid response format")
                except ValueError as e:
                    result['api1_status'] = 'FAILED'
                    result['api1_error'] = f'Invalid JSON response from API1: {str(e)}'
                    logger.error(f"API1 JSON parsing error: {str(e)}")
            else:
                result['api1_status'] = 'FAILED'
                result['api1_error'] = f"API1 error: {api1_response.text}"
                logger.error(f"API1 HTTP error: {api1_response.status_code}")
                
        except requests.exceptions.ConnectionError:
            result['api1_status'] = 'FAILED'
            result['api1_error'] = "Không thể kết nối đến API1. Vui lòng kiểm tra lại server API1."
            logger.error("API1 Connection Error")
        except requests.exceptions.Timeout:
            result['api1_status'] = 'FAILED'
            result['api1_error'] = "API1 không phản hồi. Vui lòng thử lại sau."
            logger.error("API1 Timeout")
        except Exception as e:
            result['api1_status'] = 'FAILED'
            result['api1_error'] = f"Lỗi không xác định từ API1: {str(e)}"
            logger.error(f"API1 Unknown Error: {str(e)}")
            
        # Gọi API 2
        try:
            # Thêm header để tránh lỗi transaction
            headers = {
                'Content-Type': 'application/json',
                'Accept': 'application/json',
                'X-Requested-With': 'XMLHttpRequest'
            }
            
            api2_response = requests.post(
                self.api2_url,
                json=api2_data,
                headers=headers,
                timeout=5
            )
            logger.info(f"API2 Response Status: {api2_response.status_code}")
            logger.info(f"API2 Response: {api2_response.text}")
            
            # Phân tích response API2
            if api2_response.ok:
                try:
                    api2_json = api2_response.json()
                    logger.info(f"API2 parsed response: {json.dumps(api2_json, indent=2, ensure_ascii=False)}")
                    if isinstance(api2_json, dict):
                        # API2 trả về success=true và có message thành công
                        if api2_json.get('success') == True:
                            result['api2_status'] = 'SUCCESS'
                            result['api2_error'] = None
                            logger.info("API2 creation successful")
                        else:
                            # Kiểm tra các trường hợp lỗi khác nhau
                            error_msg = api2_json.get('error') or api2_json.get('message') or 'Unknown error from API2'
                            result['api2_status'] = 'FAILED'
                            result['api2_error'] = error_msg
                            logger.error(f"API2 creation failed: {error_msg}")
                            
                            # Nếu lỗi là do transaction, thử lại sau
                            if "nested transactions" in error_msg.lower():
                                logger.warning("Lỗi transaction, sẽ thử lại sau")
                                self.redis.add_to_retry_queue(api2_data, 'api2')
                    else:
                        result['api2_status'] = 'FAILED'
                        result['api2_error'] = 'Invalid response format from API2'
                        logger.error("API2 invalid response format")
                except ValueError as e:
                    result['api2_status'] = 'FAILED'
                    result['api2_error'] = f'Invalid JSON response from API2: {str(e)}'
                    logger.error(f"API2 JSON parsing error: {str(e)}")
            else:
                # Kiểm tra nếu response là 200 nhưng có lỗi trong nội dung
                if api2_response.status_code == 200:
                    try:
                        api2_json = api2_response.json()
                        if isinstance(api2_json, dict):
                            error_msg = api2_json.get('error') or api2_json.get('message') or 'Unknown error from API2'
                            result['api2_status'] = 'FAILED'
                            result['api2_error'] = error_msg
                            logger.error(f"API2 creation failed: {error_msg}")
                        else:
                            result['api2_status'] = 'FAILED'
                            result['api2_error'] = 'Invalid response format from API2'
                            logger.error("API2 invalid response format")
                    except ValueError:
                        result['api2_status'] = 'FAILED'
                        result['api2_error'] = f"API2 error: {api2_response.text}"
                        logger.error(f"API2 HTTP error: {api2_response.status_code}")
                else:
                    result['api2_status'] = 'FAILED'
                    result['api2_error'] = f"API2 error: {api2_response.text}"
                    logger.error(f"API2 HTTP error: {api2_response.status_code}")
                
        except requests.exceptions.ConnectionError:
            result['api2_status'] = 'FAILED'
            result['api2_error'] = "Không thể kết nối đến API2. Vui lòng kiểm tra lại server API2."
            logger.error("API2 Connection Error")
        except requests.exceptions.Timeout:
            result['api2_status'] = 'FAILED'
            result['api2_error'] = "API2 không phản hồi. Vui lòng thử lại sau."
            logger.error("API2 Timeout")
        except Exception as e:
            result['api2_status'] = 'FAILED'
            result['api2_error'] = f"Lỗi không xác định từ API2: {str(e)}"
            logger.error(f"API2 Unknown Error: {str(e)}")
            
        # Log kết quả cuối cùng với đầy đủ thông tin
        logger.info("========== FINAL RESULT ==========")
        logger.info(json.dumps(result, indent=2, ensure_ascii=False))
        logger.info("=================================")

        # Đảm bảo response luôn có cấu trúc đúng
        if not isinstance(result, dict):
            result = {
                'api1_status': 'FAILED',
                'api2_status': 'FAILED',
                'api1_error': 'Invalid result format',
                'api2_error': 'Invalid result format'
            }
        
        return result
    
    def _format_data_for_api1(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Format dữ liệu cho API1 (localhost:19335/API/CreateEmployee)
        """
        return {
            "Employee_ID": int(data.get("employeeNumber", 0)),
            "First_Name": data.get("firstName", ""),
            "Last_Name": data.get("lastName", ""),
            "Email": data.get("email", ""),
            "Phone_Number": data.get("phoneNumber", ""),
            "Address1": "",  # Không yêu cầu trong form
            "City": data.get("city", ""),
            "State": "",  # Không yêu cầu trong form
            "Zip": "",  # Không yêu cầu trong form
            "Gender": data.get("gender", True),
            "Shareholder_Status": False,  # Giá trị mặc định
            "Benefit_Plans": 1,  # Giá trị mặc định
            "Ethnicity": 1  # Giá trị mặc định
        }
    
    def _format_data_for_api2(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Format dữ liệu cho API2 (localhost:8080/springapp/api/employee/add)
        """
        try:
            # Chuyển đổi các giá trị số sang string
            pay_rate = str(data.get("payRate", "0"))
            vacation_days = str(data.get("vacationDays", "0"))
            
            # Lấy ID từ API2
            try:
                api2_response = requests.get(
                    "http://localhost:8080/springapp/api/employee/list",
                    timeout=5
                )
                if api2_response.ok:
                    api2_data = api2_response.json()
                    if isinstance(api2_data, dict) and 'data' in api2_data and api2_data['data']:
                        employees = api2_data['data']
                        # Lọc ra các idEmployee hợp lệ
                        valid_ids = []
                        for emp in employees:
                            if isinstance(emp, dict):
                                emp_id = emp.get('idEmployee')
                                if emp_id is not None and str(emp_id).isdigit():
                                    valid_ids.append(int(emp_id))
                        
                        if valid_ids:
                            next_id = max(valid_ids) + 1
                            logger.info(f"API2 valid IDs: {sorted(valid_ids)}")
                            logger.info(f"API2 next ID: {next_id}")
                        else:
                            next_id = 1
                            logger.warning("Không tìm thấy ID hợp lệ từ API2, sử dụng ID mặc định")
                    else:
                        next_id = 1
                        logger.warning("Dữ liệu API2 không hợp lệ, sử dụng ID mặc định")
                else:
                    next_id = 1
                    logger.warning(f"Không thể kết nối API2: {api2_response.text}, sử dụng ID mặc định")
            except Exception as e:
                next_id = 1
                logger.error(f"Lỗi khi lấy ID từ API2: {str(e)}, sử dụng ID mặc định")
            
            # Đảm bảo các giá trị không bị null
            return {
                "idEmployee": next_id,  # Thêm trường idEmployee
                "employeeNumber": str(data.get("employeeNumber", "")),
                "lastName": data.get("lastName", ""),
                "firstName": data.get("firstName", ""),
                "ssn": data.get("ssn", ""),
                "payRate": pay_rate,
                "payRatesId": "0",  # Giá trị mặc định
                "vacationDays": vacation_days,
                "paidToDate": "0",  # Giá trị mặc định
                "paidLastYear": "0"  # Giá trị mặc định
            }
        except Exception as e:
            logger.error(f"Lỗi khi format dữ liệu cho API2: {e}")
            # Trả về dữ liệu mặc định nếu có lỗi
            return {
                "idEmployee": 1,  # ID mặc định
                "employeeNumber": "0",
                "lastName": "",
                "firstName": "",
                "ssn": "",
                "payRate": "0",
                "payRatesId": "0",
                "vacationDays": "0",
                "paidToDate": "0",
                "paidLastYear": "0"
            }

    def retry_failed_creations(self):
        while True:
            retry_item = self.redis.get_from_retry_queue()
            if not retry_item:
                break

            if retry_item["retries"] >= Config.MAX_RETRIES:
                logger.error(f"Max retries reached for {retry_item['api']}")
                continue

            try:
                if retry_item["api"] == "api1":
                    response = requests.post(Config.API1_CREATE, json=retry_item["data"])
                else:
                    response = requests.post(Config.API2_CREATE, json=retry_item["data"])

                if response.status_code != 200:
                    retry_item["retries"] += 1
                    retry_item["last_retry"] = datetime.now().isoformat()
                    self.redis.add_to_retry_queue(retry_item["data"], retry_item["api"])
                    time.sleep(Config.RETRY_DELAY)
                else:
                    logger.info(f"Retry successful for {retry_item['api']}")
            except Exception as e:
                logger.error(f"Error during retry: {e}")
                retry_item["retries"] += 1
                retry_item["last_retry"] = datetime.now().isoformat()
                self.redis.add_to_retry_queue(retry_item["data"], retry_item["api"])
                time.sleep(Config.RETRY_DELAY) 