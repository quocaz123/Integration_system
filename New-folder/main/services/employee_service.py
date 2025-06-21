import logging
import json
import requests
import time
import random
from typing import Dict, List, Optional, Any, Tuple
from .config import Config
import uuid
import asyncio
from aiohttp import ClientSession
from datetime import datetime
from .redis_service import RedisService

logger = logging.getLogger(__name__)

def validate_employee_data(data: Dict[str, Any]) -> Tuple[bool, str]:
    """Validate dữ liệu nhân viên"""
    required_fields = ['firstName', 'lastName', 'email']
    for field in required_fields:
        if not data.get(field):
            return False, f"Thiếu trường bắt buộc: {field}"
    
    # Validate email
    email = data.get('email', '')
    if '@' not in email or '.' not in email:
        return False, "Email không hợp lệ"
    
    # Validate phone number
    phone = data.get('phoneNumber', '')
    if phone and not phone.replace('-', '').replace(' ', '').isdigit():
        return False, "Số điện thoại không hợp lệ"
    
    return True, ""

def fetch_employees_from_api1() -> List[Dict]:
    """Lấy danh sách nhân viên từ API1"""
    try:
        url = Config.API1_GET_EMPLOYEES
        logger.info(f"Fetching from API1: {url}")
        
        response = requests.get(url, timeout=10)
        logger.info(f"API1 response status: {response.status_code}")
        
        if response.status_code == 200:
            try:
                data = response.json()
                if isinstance(data, dict) and 'success' in data and data['success'] and 'data' in data:
                    employees = data['data']
                    if isinstance(employees, list):
                        logger.info(f"API1 returned {len(employees)} employees")
                        return employees
                    else:
                        logger.error(f"API1 data is not a list: {type(employees)}")
                        return []
                else:
                    logger.error(f"API1 returned unexpected data structure")
                    logger.error(f"API1 response content: {json.dumps(data)[:500]}")
                    return []
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse API1 response as JSON: {e}")
                logger.error(f"Raw response: {response.text[:500]}")
                return []
        logger.error(f"Error fetching from API1: {response.text[:500]}")
        return []
    except Exception as e:
        logger.error(f"Exception fetching from API1: {e}")
        return []

def fetch_employees_from_api2() -> List[Dict]:
    """Lấy danh sách nhân viên từ API2"""
    try:
        url = Config.API2_GET_EMPLOYEES
        logger.info(f"Fetching from API2: {url}")
        
        response = requests.get(url, timeout=10)
        logger.info(f"API2 response status: {response.status_code}")
        
        if response.status_code == 200:
            try:
                data = response.json()
                if isinstance(data, dict) and 'data' in data:
                    employees = data['data']
                    if isinstance(employees, list):
                        logger.info(f"API2 returned {len(employees)} employees")
                        return employees
                    else:
                        logger.error(f"API2 data is not a list: {type(employees)}")
                        return []
                else:
                    logger.error(f"API2 returned unexpected data structure")
                    logger.error(f"API2 response content: {json.dumps(data)[:500]}")
                    return []
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse API2 response as JSON: {e}")
                logger.error(f"Raw response: {response.text[:500]}")
                return []
        logger.error(f"Error fetching from API2: {response.text[:500]}")
        return []
    except Exception as e:
        logger.error(f"Exception fetching from API2: {e}")
        return []

def merge_employee_data() -> Optional[Dict]:
    """Merge dữ liệu nhân viên từ cả hai API"""
    api1_data = fetch_employees_from_api1()
    api2_data = fetch_employees_from_api2()
    
    # Tạo mapping cho cả hai API
    api1_mapping = {}
    api2_mapping = {}
    stats = {
        'total_employees': 0,
        'total_pay_rate': 0,
        'from_db1_only': 0,
        'from_db2_only': 0,
        'from_both': 0
    }
    
    # Map dữ liệu API1
    for emp1 in api1_data:
        full_name = emp1.get('FullName', '')
        first_name = emp1.get('First_Name', '')
        last_name = emp1.get('Last_Name', '')
        if not first_name and not last_name and full_name:
            name_parts = full_name.split(' ', 1)
            first_name = name_parts[0]
            last_name = name_parts[1] if len(name_parts) > 1 else ''
        first_name = first_name.lower().strip()
        last_name = last_name.lower().strip()
        name_key = f"{first_name}_{last_name}"
        if first_name and last_name:
            api1_mapping[name_key] = emp1
    # Map dữ liệu API2
    for emp2 in api2_data:
        first_name = emp2.get('firstName', '').lower().strip()
        last_name = emp2.get('lastName', '').lower().strip()
        if first_name and last_name:
            name_key = f"{first_name}_{last_name}"
            api2_mapping[name_key] = emp2
    # Tập hợp tất cả các key
    all_keys = set(api1_mapping.keys()) | set(api2_mapping.keys())
    merged_data = []
    # Xử lý tất cả các nhân viên
    for name_key in all_keys:
        emp1 = api1_mapping.get(name_key)
        emp2 = api2_mapping.get(name_key)
        merged_employee = None
        if emp1 and emp2:
            try:
                merged_employee = {
                    "employeeNumber": str(emp1.get("Employee_ID", "N/A")),
                    "idEmployee": str(emp2.get("idEmployee", emp2.get("employeeNumber", "N/A"))),
                    "fullName": emp1.get("FullName", "N/A"),
                    "firstName": emp1.get("First_Name", "N/A").capitalize(),
                    "lastName": emp1.get("Last_Name", "N/A").capitalize(),
                    "email": emp1.get("Email", "N/A"),
                    "phoneNumber": emp1.get("Phone_Number", "N/A"),
                    "ssn": emp2.get("ssn", "N/A"),
                    "vacationDays": str(int(float(emp2.get("vacationDays", 0)))),
                    "payRate": "{:.2f}".format(float(emp2.get("payRate", 0))),
                    "city": emp1.get("City", "N/A"),
                    "gender": emp1.get("Gender", "N/A"),
                    "status": "Đồng bộ"
                }
                stats['from_both'] += 1
                stats['total_pay_rate'] += float(emp2.get("payRate", 0))
            except (ValueError, TypeError):
                merged_employee = create_employee_from_api1(emp1)
                stats['from_db1_only'] += 1
        elif emp1:
            merged_employee = create_employee_from_api1(emp1)
            stats['from_db1_only'] += 1
        elif emp2:
            merged_employee = create_employee_from_api2(emp2, name_key)
            if merged_employee:
                stats['from_db2_only'] += 1
                stats['total_pay_rate'] += float(emp2.get("payRate", 0))
        if merged_employee:
            merged_data.append(merged_employee)
    stats['total_employees'] = len(merged_data)
    return {
        'merged_data': merged_data,
        'stats': stats
    }

def create_employee_from_api1(emp1: Dict) -> Dict:
    """Tạo bản ghi nhân viên từ dữ liệu API1"""
    return {
        "employeeNumber": str(emp1.get("Employee_ID", "N/A")),
        "fullName": emp1.get("FullName", "N/A"),
        "firstName": emp1.get("First_Name", "N/A").capitalize(),
        "lastName": emp1.get("Last_Name", "N/A").capitalize(),
        "address": emp1.get("Address1", "N/A"),
        "city": emp1.get("City", "N/A"),
        "state": emp1.get("State", "N/A"),
        "email": emp1.get("Email", "N/A"),
        "phoneNumber": emp1.get("Phone_Number", "N/A"),
        "gender": emp1.get("Gender", "N/A"),
        "ssn": "N/A",
        "vacationDays": "0",
        "payRate": "0.00",
        "payRatesId": "0",
        "paidToDate": "0.00",
        "paidLastYear": "0.00",
        "status": "Chỉ có API1",
        "lastUpdated": datetime.now().isoformat()
    }

def create_employee_from_api2(emp2: Dict, name_key: str) -> Optional[Dict]:
    """Tạo bản ghi nhân viên từ dữ liệu API2"""
    try:
        first_name, last_name = name_key.split('_')
        return {
            "employeeNumber": str(emp2.get("employeeNumber", "N/A")),
            "fullName": f"{first_name.capitalize()} {last_name.capitalize()}",
            "firstName": first_name.capitalize(),
            "lastName": last_name.capitalize(),
            "address": "N/A",
            "city": "N/A",
            "state": "N/A",
            "email": "N/A",
            "phoneNumber": "N/A",
            "gender": "N/A",
            "ssn": emp2.get("ssn", "N/A"),
            "vacationDays": str(int(float(emp2.get("vacationDays", 0)))),
            "payRate": "{:.2f}".format(float(emp2.get("payRate", 0))),
            "payRatesId": str(int(float(emp2.get("payRatesId", 0)))),
            "paidToDate": "{:.2f}".format(float(emp2.get("paidToDate", 0))),
            "paidLastYear": "{:.2f}".format(float(emp2.get("paidLastYear", 0))),
            "status": "Chỉ có API2",
            "lastUpdated": datetime.now().isoformat()
        }
    except (ValueError, TypeError) as e:
        logger.warning(f"Lỗi khi tạo dữ liệu từ API2 cho nhân viên {name_key}: {e}")
        return None

def _get_next_employee_ids(self) -> Tuple[int, int]:
    """
    Lấy ID mới từ Redis với cơ chế retry và logging chi tiết
    """
    try:
        logger.info("Bắt đầu lấy ID mới từ Redis")
        
        # Lấy ID hiện tại từ Redis
        api1_id = self.redis.client.incr('last_api1_id')
        api2_id = self.redis.client.incr('last_api2_id')
        
        logger.info(f"ID ban đầu từ Redis: API1={api1_id}, API2={api2_id}")

        # Nếu chưa có ID trong Redis, khởi tạo giá trị ban đầu
        if api1_id == 1:
            logger.info("Khởi tạo ID cho API1")
            # Lấy ID lớn nhất từ API1
            api1_response = requests.get(Config.API1_GET_EMPLOYEES, timeout=10)
            if api1_response.ok:
                api1_data = api1_response.json()
                if isinstance(api1_data, dict) and 'data' in api1_data:
                    max_id = 0
                    for emp in api1_data['data']:
                        try:
                            emp_id = int(emp.get('Employee_ID', 0))
                            max_id = max(max_id, emp_id)
                        except (ValueError, TypeError):
                            continue
                    if max_id > 0:
                        logger.info(f"Tìm thấy ID lớn nhất từ API1: {max_id}")
                        self.redis.client.set('last_api1_id', max_id)
                        api1_id = max_id + 1
                    else:
                        logger.warning("Không tìm thấy ID hợp lệ từ API1, sử dụng ID mặc định")
                        api1_id = 1
                else:
                    logger.warning("Dữ liệu API1 không hợp lệ, sử dụng ID mặc định")
                    api1_id = 1
            else:
                logger.warning(f"Không thể kết nối API1: {api1_response.text}, sử dụng ID mặc định")
                api1_id = 1

        if api2_id == 1:
            logger.info("Khởi tạo ID cho API2")
            # Lấy ID lớn nhất từ API2
            api2_response = requests.get(Config.API2_GET_EMPLOYEES, timeout=10)
            if api2_response.ok:
                api2_data = api2_response.json()
                if isinstance(api2_data, dict) and 'data' in api2_data:
                    max_id = 0
                    for emp in api2_data['data']:
                        try:
                            emp_id = int(emp.get('employeeNumber', 0))
                            max_id = max(max_id, emp_id)
                        except (ValueError, TypeError):
                            continue
                    if max_id > 0:
                        logger.info(f"Tìm thấy ID lớn nhất từ API2: {max_id}")
                        self.redis.client.set('last_api2_id', max_id)
                        api2_id = max_id + 1
                    else:
                        logger.warning("Không tìm thấy ID hợp lệ từ API2, sử dụng ID mặc định")
                        api2_id = 1
                else:
                    logger.warning("Dữ liệu API2 không hợp lệ, sử dụng ID mặc định")
                    api2_id = 1
            else:
                logger.warning(f"Không thể kết nối API2: {api2_response.text}, sử dụng ID mặc định")
                api2_id = 1

        # Đảm bảo ID không trùng nhau
        if api1_id == api2_id:
            api2_id += 1
            logger.info(f"Điều chỉnh ID API2 để tránh trùng: {api2_id}")

        logger.info(f"ID cuối cùng: API1={api1_id}, API2={api2_id}")
        return api1_id, api2_id

    except Exception as e:
        logger.error(f"Lỗi khi lấy ID mới: {str(e)}")
        # Sử dụng phương thức dự phòng
        return self._generate_employee_ids()

def _generate_employee_ids(self) -> Tuple[int, int]:
    """
    Tạo ID mới dựa trên timestamp và random number
    """
    try:
        logger.info("Bắt đầu tạo ID mới bằng timestamp và random")
        
        # Lấy timestamp hiện tại (10 chữ số)
        timestamp = int(time.time())
        logger.info(f"Timestamp: {timestamp}")
        
        # Tạo số ngẫu nhiên 4 chữ số
        random_num = random.randint(1000, 9999)
        logger.info(f"Random number: {random_num}")
        
        # Kết hợp timestamp và random number để tạo ID
        api1_id = int(f"{timestamp}{random_num}")
        api2_id = api1_id + 1  # Đảm bảo ID của API2 khác API1
        
        logger.info(f"ID mới được tạo: API1={api1_id}, API2={api2_id}")
        return api1_id, api2_id

    except Exception as e:
        logger.error(f"Lỗi khi tạo ID mới: {str(e)}")
        # Trả về ID mặc định nếu có lỗi
        return 1, 2

def create_employee(self, employee_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Tạo nhân viên mới bằng cách gọi cả hai API
    """
    # Validate dữ liệu đầu vào
    is_valid, error_message = self.validate_employee_data(employee_data)
    if not is_valid:
        return {
            'api1_status': 'FAILED',
            'api2_status': 'FAILED',
            'api1_error': error_message,
            'api2_error': error_message
        }

    # Tạo ID mới
    api1_id, api2_id = self._generate_employee_ids()
    result = {
        'api1_status': 'PENDING',
        'api2_status': 'PENDING',
        'api1_error': None,
        'api2_error': None
    }

    # Format dữ liệu
    api1_data = self.transform_for_api1(employee_data)
    api2_data = self.transform_for_api2(employee_data)

    # Gán ID mới
    api1_data['Employee_ID'] = str(api1_id)
    api2_data['employeeNumber'] = str(api2_id)

    # Log dữ liệu gửi đi
    logger.info(f"\n{'='*50}\nGửi dữ liệu đến API1:\nURL: {self.api1_url}\nData: {json.dumps(api1_data, indent=2, ensure_ascii=False)}\n{'='*50}")
    logger.info(f"\n{'='*50}\nGửi dữ liệu đến API2:\nURL: {self.api2_url}\nData: {json.dumps(api2_data, indent=2, ensure_ascii=False)}\n{'='*50}")

    # Gọi API1
    try:
        api1_response = requests.post(self.api1_url, json=api1_data, timeout=5)
        logger.info(f"\n{'='*50}\nPhản hồi từ API1:\nStatus: {api1_response.status_code}\nContent: {api1_response.text}\n{'='*50}")
        
        if api1_response.ok:
            api1_json = api1_response.json()
            if api1_json.get('success'):
                result['api1_status'] = 'SUCCESS'
            else:
                result['api1_status'] = 'FAILED'
                result['api1_error'] = api1_json.get('message', 'Unknown error from API1')
        else:
            result['api1_status'] = 'FAILED'
            result['api1_error'] = api1_response.text
    except Exception as e:
        result['api1_status'] = 'FAILED'
        result['api1_error'] = str(e)

    # Gọi API2
    try:
        api2_response = requests.post(self.api2_url, json=api2_data, timeout=5)
        logger.info(f"\n{'='*50}\nPhản hồi từ API2:\nStatus: {api2_response.status_code}\nContent: {api2_response.text}\n{'='*50}")
        
        if api2_response.ok:
            api2_json = api2_response.json()
            if api2_json.get('success'):
                result['api2_status'] = 'SUCCESS'
            else:
                result['api2_status'] = 'FAILED'
                result['api2_error'] = api2_json.get('message', 'Unknown error from API2')
        else:
            result['api2_status'] = 'FAILED'
            result['api2_error'] = api2_response.text
    except Exception as e:
        result['api2_status'] = 'FAILED'
        result['api2_error'] = str(e)

    logger.info(f"Kết quả cuối cùng: {json.dumps(result, ensure_ascii=False)}")
    return result

    async def _get_next_employee_ids_async(self):
        # Implementation of _get_next_employee_ids_async method
        pass

    async def _format_data_for_api1(self, employee_data: Dict[str, Any]) -> Dict[str, Any]:
        # Implementation of _format_data_for_api1 method
        pass

    async def _format_data_for_api2(self, employee_data: Dict[str, Any]) -> Dict[str, Any]:
        # Implementation of _format_data_for_api2 method
        pass

    async def _call_api1_async(self, session: ClientSession, api1_data: Dict[str, Any]) -> Dict[str, Any]:
        # Implementation of _call_api1_async method
        pass

    async def _call_api2_async(self, session: ClientSession, api2_data: Dict[str, Any]) -> Dict[str, Any]:
        # Implementation of _call_api2_async method
        pass

    def _merge_results(self, api1_result: Dict[str, Any], api2_result: Dict[str, Any]) -> Dict[str, Any]:
        # Implementation of _merge_results method
        pass

    def get_employees(self) -> Dict[str, Any]:
        """
        Lấy danh sách nhân viên từ cả hai API và merge dữ liệu
        """
        try:
            # Lấy dữ liệu từ API1
            logger.info(f"\n{'='*50}\nGửi request đến API1:\nURL: {self.api1_url}\n{'='*50}")
            api1_response = requests.get(self.api1_url)
            logger.info(f"\n{'='*50}\nPhản hồi từ API1:\nStatus: {api1_response.status_code}\nContent: {api1_response.text}\n{'='*50}")
            
            if not api1_response.ok:
                logger.error(f"API1 error: {api1_response.text}")
                return {"error": f"API1 error: {api1_response.text}"}
            
            try:
                api1_data = api1_response.json()
                if not isinstance(api1_data, list):
                    logger.error(f"API1 returned unexpected data structure: {api1_data}")
                    return {"error": "API1 returned unexpected data structure"}
            except json.JSONDecodeError:
                logger.error(f"API1 returned invalid JSON: {api1_response.text}")
                return {"error": "API1 returned invalid JSON"}

            # Lấy dữ liệu từ API2
            logger.info(f"\n{'='*50}\nGửi request đến API2:\nURL: {self.api2_url}\n{'='*50}")
            api2_response = requests.get(self.api2_url)
            logger.info(f"\n{'='*50}\nPhản hồi từ API2:\nStatus: {api2_response.status_code}\nContent: {api2_response.text}\n{'='*50}")
            
            if not api2_response.ok:
                logger.error(f"API2 error: {api2_response.text}")
                return {"error": f"API2 error: {api2_response.text}"}
            
            try:
                api2_data = api2_response.json()
                if not isinstance(api2_data, list):
                    logger.error(f"API2 returned unexpected data structure: {api2_data}")
                    return {"error": "API2 returned unexpected data structure"}
            except json.JSONDecodeError:
                logger.error(f"API2 returned invalid JSON: {api2_response.text}")
                return {"error": "API2 returned invalid JSON"}

            # Merge dữ liệu
            merged_data = []
            for emp1 in api1_data:
                # Tìm employee tương ứng trong API2
                emp2 = next((e for e in api2_data if str(e.get('employeeNumber')) == str(emp1.get('Employee_ID'))), None)
                
                if emp2:
                    merged_emp = {
                        'employeeNumber': emp1.get('Employee_ID'),
                        'lastName': emp1.get('Last_Name'),
                        'firstName': emp1.get('First_Name'),
                        'ssn': emp2.get('ssn'),
                        'payRate': emp2.get('payRate'),
                        'vacationDays': emp2.get('vacationDays'),
                        'paidToDate': emp2.get('paidToDate'),
                        'paidLastYear': emp2.get('paidLastYear'),
                        'benefitPlans': emp1.get('Benefit_Plans'),
                        'ethnicity': emp1.get('Ethnicity')
                    }
                    merged_data.append(merged_emp)

            # Tính toán thống kê
            stats = {
                'totalEmployees': len(merged_data),
                'totalPayRate': sum(float(emp.get('payRate', 0)) for emp in merged_data),
                'totalVacationDays': sum(float(emp.get('vacationDays', 0)) for emp in merged_data)
            }

            return {
                'merged_data': merged_data,
                'stats': stats
            }

        except Exception as e:
            logger.error(f"Error in get_employees: {str(e)}")
            return {"error": str(e)}

    def update_employee(self, employee_data: Dict[str, Any]) -> Dict[str, Any]:
        result = {
            'api1_status': 'PENDING',
            'api2_status': 'PENDING',
            'api1_error': None,
            'api2_error': None
        }

        api1_data = self.transform_for_api1(employee_data)
        api2_data = self.transform_for_api2(employee_data)
        employee_number = str(employee_data.get('employeeNumber', '') or '')
        id_employee = str(employee_data.get('idEmployee', '') or '')

        # Gọi API1 nếu có employeeNumber
        if employee_number and employee_number.lower() != 'n/a':
            try:
                api1_url = f"{Config.API1_URL}/UpdateEmployee/{employee_number}"
                logger.info(f"[UPDATE][API1] URL: {api1_url}")
                logger.info(f"[UPDATE][API1] employeeNumber: {employee_number}")
                logger.info(f"[UPDATE][API1] Data: {json.dumps(api1_data, ensure_ascii=False)}")
                api1_response = requests.put(api1_url, json=api1_data, timeout=5)
                logger.info(f"API1 Response: {api1_response.status_code} {api1_response.text}")
                if api1_response.ok:
                    api1_json = api1_response.json()
                    if api1_json.get('success'):
                        result['api1_status'] = 'SUCCESS'
                    else:
                        result['api1_status'] = 'FAILED'
                        result['api1_error'] = api1_json.get('message', 'Unknown error from API1')
                else:
                    result['api1_status'] = 'FAILED'
                    result['api1_error'] = api1_response.text
            except Exception as e:
                result['api1_status'] = 'FAILED'
                result['api1_error'] = str(e)
        else:
            result['api1_status'] = 'FAILED'
            result['api1_error'] = 'Thiếu employeeNumber, không thể cập nhật API1'

        # Gọi API2 nếu có idEmployee
        if id_employee and id_employee.lower() != 'n/a':
            try:
                api2_url = f"{Config.API2_URL}/employee/{id_employee}"
                logger.info(f"[UPDATE][API2] URL: {api2_url}")
                logger.info(f"[UPDATE][API2] idEmployee: {id_employee}")
                logger.info(f"[UPDATE][API2] Data: {json.dumps(api2_data, ensure_ascii=False)}")
                api2_response = requests.put(api2_url, json=api2_data, timeout=5)
                logger.info(f"API2 Response: {api2_response.status_code} {api2_response.text}")
                if api2_response.ok:
                    api2_json = api2_response.json()
                    if api2_json.get('success'):
                        result['api2_status'] = 'SUCCESS'
                    else:
                        result['api2_status'] = 'FAILED'
                        result['api2_error'] = api2_json.get('message', 'Unknown error from API2')
                else:
                    result['api2_status'] = 'FAILED'
                    result['api2_error'] = api2_response.text
            except Exception as e:
                result['api2_status'] = 'FAILED'
                result['api2_error'] = str(e)
        else:
            result['api2_status'] = 'FAILED'
            result['api2_error'] = 'Thiếu idEmployee, không thể cập nhật API2'

        logger.info(f"Kết quả cập nhật: {json.dumps(result, ensure_ascii=False)}")
        return result

class EmployeeService:
    def __init__(self):
        self.api1_url = Config.API1_CREATE_EMPLOYEE
        self.api2_url = Config.API2_CREATE_EMPLOYEE
        self.redis = RedisService()

    def _generate_employee_ids(self) -> Tuple[int, int]:
        """
        Tạo ID mới dựa trên timestamp và random number
        """
        try:
            logger.info("Bắt đầu tạo ID mới bằng timestamp và random")
            
            # Lấy timestamp hiện tại (10 chữ số)
            timestamp = int(time.time())
            logger.info(f"Timestamp: {timestamp}")
            
            # Tạo số ngẫu nhiên 4 chữ số
            random_num = random.randint(1000, 9999)
            logger.info(f"Random number: {random_num}")
            
            # Kết hợp timestamp và random number để tạo ID
            api1_id = int(f"{timestamp}{random_num}")
            api2_id = api1_id + 1  # Đảm bảo ID của API2 khác API1
            
            logger.info(f"ID mới được tạo: API1={api1_id}, API2={api2_id}")
            return api1_id, api2_id

        except Exception as e:
            logger.error(f"Lỗi khi tạo ID mới: {str(e)}")
            # Trả về ID mặc định nếu có lỗi
            return 1, 2

    def transform_for_api1(self, employee_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Chuyển đổi dữ liệu cho API1 với xử lý null và format
        """
        return {
            "Employee_ID": str(employee_data.get("employeeNumber", "")),
            "First_Name": str(employee_data.get("firstName", "")).strip(),
            "Last_Name": str(employee_data.get("lastName", "")).strip(),
            "Email": str(employee_data.get("email", "")).strip(),
            "Phone_Number": str(employee_data.get("phoneNumber", "")).strip(),
            "Address1": str(employee_data.get("address", "")).strip(),
            "City": str(employee_data.get("city", "")).strip(),
            "State": str(employee_data.get("state", "")).strip(),
            "Zip": str(employee_data.get("zip", "")).strip(),
            "Gender": str(employee_data.get("gender", "")).strip(),
            "Benefit_Plans": str(employee_data.get("benefitPlans", "")).strip(),
            "Ethnicity": str(employee_data.get("ethnicity", "")).strip()
        }

    def transform_for_api2(self, employee_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Chuyển đổi dữ liệu cho API2 với xử lý null và format
        """
        # Xử lý các trường số
        try:
            pay_rate = float(employee_data.get("payRate", "0"))
        except (ValueError, TypeError):
            pay_rate = 0.0

        try:
            vacation_days = int(float(employee_data.get("vacationDays", "0")))
        except (ValueError, TypeError):
            vacation_days = 0

        try:
            paid_to_date = float(employee_data.get("paidToDate", "0"))
        except (ValueError, TypeError):
            paid_to_date = 0.0

        try:
            paid_last_year = float(employee_data.get("paidLastYear", "0"))
        except (ValueError, TypeError):
            paid_last_year = 0.0

        return {
            "employeeNumber": str(employee_data.get("employeeNumber", "")),
            "lastName": str(employee_data.get("lastName", "")).strip(),
            "firstName": str(employee_data.get("firstName", "")).strip(),
            "ssn": str(employee_data.get("ssn", "")).strip(),
            "payRate": str(pay_rate),
            "payRatesId": str(employee_data.get("payRatesId", "0")),
            "vacationDays": str(vacation_days),
            "paidToDate": str(paid_to_date),
            "paidLastYear": str(paid_last_year)
        }

    def validate_employee_data(self, employee_data: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Kiểm tra tính hợp lệ của dữ liệu nhân viên
        """
        # Kiểm tra các trường bắt buộc
        required_fields = ['firstName', 'lastName', 'email']
        for field in required_fields:
            if not employee_data.get(field):
                return False, f"Thiếu trường bắt buộc: {field}"

        # Kiểm tra email
        email = employee_data.get('email', '')
        if '@' not in email or '.' not in email:
            return False, "Email không hợp lệ"

        # Kiểm tra số điện thoại
        phone = employee_data.get('phoneNumber', '')
        if phone and not phone.replace('-', '').replace(' ', '').isdigit():
            return False, "Số điện thoại không hợp lệ"

        # Kiểm tra các trường số
        numeric_fields = ['payRate', 'vacationDays', 'paidToDate', 'paidLastYear']
        for field in numeric_fields:
            value = employee_data.get(field, '0')
            try:
                float(value)
            except (ValueError, TypeError):
                return False, f"Trường {field} phải là số"

        return True, "Dữ liệu hợp lệ"

    def update_employee(self, employee_data: Dict[str, Any]) -> Dict[str, Any]:
        result = {
            'api1_status': 'PENDING',
            'api2_status': 'PENDING',
            'api1_error': None,
            'api2_error': None
        }

        api1_data = self.transform_for_api1(employee_data)
        api2_data = self.transform_for_api2(employee_data)
        employee_number = str(employee_data.get('employeeNumber', '') or '')
        id_employee = str(employee_data.get('idEmployee', '') or '')

        # Gọi API1 nếu có employeeNumber
        if employee_number and employee_number.lower() != 'n/a':
            try:
                api1_url = f"{Config.API1_URL}/UpdateEmployee/{employee_number}"
                logger.info(f"[UPDATE][API1] URL: {api1_url}")
                logger.info(f"[UPDATE][API1] employeeNumber: {employee_number}")
                logger.info(f"[UPDATE][API1] Data: {json.dumps(api1_data, ensure_ascii=False)}")
                api1_response = requests.put(api1_url, json=api1_data, timeout=5)
                logger.info(f"API1 Response: {api1_response.status_code} {api1_response.text}")
                if api1_response.ok:
                    api1_json = api1_response.json()
                    if api1_json.get('success'):
                        result['api1_status'] = 'SUCCESS'
                    else:
                        result['api1_status'] = 'FAILED'
                        result['api1_error'] = api1_json.get('message', 'Unknown error from API1')
                else:
                    result['api1_status'] = 'FAILED'
                    result['api1_error'] = api1_response.text
            except Exception as e:
                result['api1_status'] = 'FAILED'
                result['api1_error'] = str(e)
        else:
            result['api1_status'] = 'FAILED'
            result['api1_error'] = 'Thiếu employeeNumber, không thể cập nhật API1'

        # Gọi API2 nếu có idEmployee
        if id_employee and id_employee.lower() != 'n/a':
            try:
                api2_url = f"{Config.API2_URL}/employee/{id_employee}"
                logger.info(f"[UPDATE][API2] URL: {api2_url}")
                logger.info(f"[UPDATE][API2] idEmployee: {id_employee}")
                logger.info(f"[UPDATE][API2] Data: {json.dumps(api2_data, ensure_ascii=False)}")
                api2_response = requests.put(api2_url, json=api2_data, timeout=5)
                logger.info(f"API2 Response: {api2_response.status_code} {api2_response.text}")
                if api2_response.ok:
                    api2_json = api2_response.json()
                    if api2_json.get('success'):
                        result['api2_status'] = 'SUCCESS'
                    else:
                        result['api2_status'] = 'FAILED'
                        result['api2_error'] = api2_json.get('message', 'Unknown error from API2')
                else:
                    result['api2_status'] = 'FAILED'
                    result['api2_error'] = api2_response.text
            except Exception as e:
                result['api2_status'] = 'FAILED'
                result['api2_error'] = str(e)
        else:
            result['api2_status'] = 'FAILED'
            result['api2_error'] = 'Thiếu idEmployee, không thể cập nhật API2'

        logger.info(f"Kết quả cập nhật: {json.dumps(result, ensure_ascii=False)}")
        return result