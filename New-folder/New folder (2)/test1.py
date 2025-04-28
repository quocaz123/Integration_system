from flask import Flask, render_template, jsonify, send_from_directory, Blueprint, request
from flask_socketio import SocketIO, emit
import requests
import threading
import time
from unidecode import unidecode
from rapidfuzz import fuzz
import os
from dotenv import load_dotenv
from cache_manager import CacheManager
import redis
import logging
import json
from typing import Any, Optional, Dict
from datetime import datetime
import uuid
from abc import ABC, abstractmethod
from flask_cors import CORS

# Cấu hình logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

load_dotenv()

class Config:
    # Redis config
    REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
    REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
    REDIS_DB = int(os.getenv('REDIS_DB', 0))
    
    # Base API URLs
    API1_BASE = 'http://localhost:19335'
    API2_BASE = 'http://localhost:8080'  # Sửa lại base URL
    
    # API endpoints
    API1_URL = API1_BASE + '/API'
    API2_URL = API2_BASE + '/springapp/api'
    
    # Queue config
    QUEUE_NAME = 'employee_create_queue'
    FAILED_QUEUE_NAME = 'employee_create_failed_queue'
    
    # Retry config
    MAX_RETRIES = 3
    BASE_DELAY = 5  # seconds
    
    # Circuit breaker config
    FAILURE_THRESHOLD = 5
    RESET_TIMEOUT = 60  # seconds

    # API endpoints for getting employee data
    API1_GET_EMPLOYEES = API1_URL + '/GetEmployees'
    API2_GET_EMPLOYEES = API2_URL + '/employee/list'  # Sửa lại endpoint
    
    # API endpoints for creating employee
    API1_CREATE_EMPLOYEE = API1_URL + '/CreateEmployee'
    API2_CREATE_EMPLOYEE = API2_URL + '/employee/add'

class CircuitBreaker:
    def __init__(self):
        self.failure_count = 0
        self.failure_threshold = Config.FAILURE_THRESHOLD
        self.reset_timeout = Config.RESET_TIMEOUT
        self.last_failure_time = None
        self.state = 'CLOSED'

    def can_execute(self):
        if self.state == 'OPEN':
            if time.time() - (self.last_failure_time or 0) > self.reset_timeout:
                self.state = 'HALF-OPEN'
                return True
            return False
        return True

    def record_failure(self):
        self.failure_count += 1
        self.last_failure_time = time.time()
        if self.failure_count >= self.failure_threshold:
            self.state = 'OPEN'

class RetryStrategy:
    def __init__(self):
        self.max_retries = Config.MAX_RETRIES
        self.base_delay = Config.BASE_DELAY

    def should_retry(self, retries):
        return retries < self.max_retries

    def get_delay(self, retries):
        return self.base_delay * (2 ** retries)

class RedisService:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.client = redis.Redis(
                host=Config.REDIS_HOST,
                port=Config.REDIS_PORT,
                db=Config.REDIS_DB,
                decode_responses=True
            )
        return cls._instance

    def push_to_queue(self, queue_name: str, data: dict) -> bool:
        try:
            self.client.rpush(queue_name, json.dumps(data))
            return True
        except Exception as e:
            logger.error(f"Error pushing to queue: {e}")
            return False

    def pop_from_queue(self, queue_name: str) -> Optional[dict]:
        try:
            data = self.client.blpop(queue_name, timeout=1)
            if data:
                return json.loads(data[1])
            return None
        except Exception as e:
            logger.error(f"Error popping from queue: {e}")
            return None

    def set_status(self, key: str, data: dict, expire: int = 3600) -> bool:
        try:
            self.client.set(key, json.dumps(data), ex=expire)
            return True
        except Exception as e:
            logger.error(f"Error setting status: {e}")
            return False

    def get_status(self, key: str) -> Optional[dict]:
        try:
            data = self.client.get(key)
            return json.loads(data) if data else None
        except Exception as e:
            logger.error(f"Error getting status: {e}")
            return None

class BaseConsumer(ABC):
    def __init__(self, api_url: str, service_name: str):
        self.api_url = api_url
        self.service_name = service_name
        self.redis = RedisService()
        self.circuit_breaker = CircuitBreaker()
        self.retry_strategy = RetryStrategy()

    @abstractmethod
    def transform_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        pass

    def process_message(self, message: Dict[str, Any]) -> bool:
        if not self.circuit_breaker.can_execute():
            return False

        try:
            data = self.transform_data(message['data'])
            response = requests.post(self.api_url, json=data)
            
            if response.status_code == 200:
                self._update_success_status(message)
                return True
            else:
                raise Exception(f"API error: {response.text}")

        except Exception as e:
            self.circuit_breaker.record_failure()
            retries = message['processing_status'][self.service_name].get('retries', 0)
            
            if self.retry_strategy.should_retry(retries):
                self._update_retry_status(message, str(e))
                delay = self.retry_strategy.get_delay(retries)
                time.sleep(delay)
                return False
            else:
                self._move_to_failed_queue(message, str(e))
                return False

    def _update_success_status(self, message: Dict[str, Any]):
        message['processing_status'][self.service_name] = {
            "status": "SUCCESS",
            "completed_at": datetime.now().isoformat()
        }
        self.redis.set_status(f"employee_create:{message['id']}", message)
        
        # Cập nhật dữ liệu và gửi qua Socket.IO
        try:
            result = merge_employee_data()
            if result and 'merged_data' in result:
                # Lưu danh sách nhân viên và stats riêng biệt
                self.redis.client.set('merged_employee_list', json.dumps(result['merged_data']))
                if 'stats' in result:
                    self.redis.client.set('merged_stats', json.dumps(result['stats']))
                
                # Gửi dữ liệu mới qua Socket.IO
                socketio.emit('data_updated', {
                    'merged_data': result['merged_data'],
                    'stats': result['stats']
                })
                logger.info("Đã cập nhật và gửi dữ liệu mới qua Socket.IO")
        except Exception as e:
            logger.error(f"Lỗi khi cập nhật dữ liệu sau khi thêm nhân viên: {e}")

    def _update_retry_status(self, message: Dict[str, Any], error: str):
        retries = message['processing_status'][self.service_name].get('retries', 0)
        message['processing_status'][self.service_name] = {
            "status": "FAILED",
            "retries": retries + 1,
            "last_retry": datetime.now().isoformat(),
            "error": error
        }
        self.redis.push_to_queue(Config.QUEUE_NAME, message)

    def _move_to_failed_queue(self, message: Dict[str, Any], error: str):
        message['processing_status'][self.service_name] = {
            "status": "FAILED",
            "error": error,
            "failed_at": datetime.now().isoformat()
        }
        self.redis.push_to_queue(Config.FAILED_QUEUE_NAME, message)

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

    def run(self):
        while True:
            message = self.redis.pop_from_queue(Config.QUEUE_NAME)
            if message and message['processing_status']['api1']['status'] == 'PENDING':
                self.process_message(message)
            time.sleep(1)

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

    def run(self):
        while True:
            message = self.redis.pop_from_queue(Config.QUEUE_NAME)
            if message and message['processing_status']['api2']['status'] == 'PENDING':
                self.process_message(message)
            time.sleep(1)

class QueueService:
    def __init__(self):
        self.redis = RedisService()

    def create_message(self, employee_data: Dict[str, Any]) -> Dict[str, Any]:
        request_id = str(uuid.uuid4())
        return {
            "id": request_id,
            "timestamp": datetime.now().isoformat(),
            "status": "PENDING",
            "data": employee_data,
            "processing_status": {
                "api1": {"status": "PENDING"},
                "api2": {"status": "PENDING"}
            }
        }

    def enqueue_employee(self, employee_data: Dict[str, Any]) -> str:
        message = self.create_message(employee_data)
        self.redis.push_to_queue(Config.QUEUE_NAME, message)
        return message["id"]

app = Flask(__name__)
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')
queue_service = QueueService()
redis_service = RedisService()

# Thêm event handler cho Socket.IO
@socketio.on('connect')
def handle_connect():
    logger.info("Client connected")
    # Gửi dữ liệu ngay khi client kết nối
    try:
        data = redis_service.get_status('merged_data')
        if data:
            emit('update_data', data)
    except Exception as e:
        logger.error(f"Error sending initial data: {e}")

@socketio.on('disconnect')
def handle_disconnect():
    logger.info("Client disconnected")

@app.route('/')
def index():
    # Khởi tạo dữ liệu mặc định
    initial_data = {
        'stats': {
            'total_employees': 0, 'total_pay_rate': 0,
            'from_db1_only': 0, 'from_db2_only': 0, 'from_both': 0
        },
        'merged_data': [] # Đổi tên key này cho nhất quán với JS
    }

    # Cố gắng cập nhật dữ liệu khi tải trang
    update_redis_data()

    # Lấy dữ liệu mới nhất từ Redis
    try:
        employee_list_json = redis_service.client.get('merged_employee_list')
        stats_json = redis_service.client.get('merged_stats')

        if employee_list_json:
            initial_data['merged_data'] = json.loads(employee_list_json)
        if stats_json:
            initial_data['stats'] = json.loads(stats_json)
            
    except Exception as e:
        logger.error(f"Error getting data from Redis for index: {e}")

    return render_template('index.html', initial_data=initial_data)

@app.route('/api/employee/create', methods=['POST'])
def create_employee():
    try:
        employee_data = request.json
        tracking_id = queue_service.enqueue_employee(employee_data)
        
        return jsonify({
            "success": True,
            "message": "Employee creation in progress",
            "tracking_id": tracking_id
        })
    except Exception as e:
        logger.error(f"Error creating employee: {e}")
        return jsonify({
            "success": False,
            "message": str(e)
        }), 500

@app.route('/api/employee/status/<tracking_id>')
def check_status(tracking_id):
    status = redis_service.get_status(f"employee_create:{tracking_id}")
    if status:
        return jsonify(status)
    return jsonify({"error": "Not found"}), 404

def start_consumers():
    api1_consumer = API1Consumer()
    api2_consumer = API2Consumer()
    
    threading.Thread(target=api1_consumer.run, daemon=True).start()
    threading.Thread(target=api2_consumer.run, daemon=True).start()

def fetch_employees_from_api1():
    """Lấy danh sách nhân viên từ API1"""
    try:
        url = Config.API1_GET_EMPLOYEES
        logger.info(f"Fetching from API1: {url}")
        logger.info(f"Base URL: {Config.API1_BASE}")
        logger.info(f"API URL: {Config.API1_URL}")
        
        response = requests.get(url)
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

def fetch_employees_from_api2():
    """Lấy danh sách nhân viên từ API2"""
    try:
        url = Config.API2_GET_EMPLOYEES
        logger.info(f"Fetching from API2: {url}")
        logger.info(f"Base URL: {Config.API2_BASE}")
        logger.info(f"API URL: {Config.API2_URL}")
        
        response = requests.get(url)
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

def merge_employee_data():
    """Merge dữ liệu nhân viên từ cả hai API dựa trên tên"""
    api1_data = fetch_employees_from_api1()
    api2_data = fetch_employees_from_api2()
    
    logger.info(f"API1 data count: {len(api1_data)}")
    logger.info(f"API2 data count: {len(api2_data)}")
    
    # Tạo dictionary để lưu trữ dữ liệu merged
    stats = {
        'total_employees': 0,
        'total_pay_rate': 0,
        'from_db1_only': 0,
        'from_db2_only': 0,
        'from_both': 0
    }

    # Tạo mapping cho cả hai API sử dụng tên
    api1_mapping = {}
    api2_mapping = {}
    
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
        
        merged_employee = None # Khởi tạo

        if emp1 and emp2:  # Có trong cả hai API
            try:
                merged_employee = {
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
                    "ssn": emp2.get("ssn", "N/A"),
                    "vacationDays": str(int(float(emp2.get("vacationDays", 0)))),
                    "payRate": "{:.2f}".format(float(emp2.get("payRate", 0))),
                    "payRatesId": str(int(float(emp2.get("payRatesId", 0)))),
                    "paidToDate": "{:.2f}".format(float(emp2.get("paidToDate", 0))),
                    "paidLastYear": "{:.2f}".format(float(emp2.get("paidLastYear", 0))),
                    "status": "Đồng bộ"
                }
                stats['from_both'] += 1
                stats['total_pay_rate'] += float(emp2.get("payRate", 0))
            except (ValueError, TypeError):
                logger.warning(f"Invalid numeric data for employee {name_key} from API2")
                # Vẫn tạo bản ghi từ API1 nếu API2 có lỗi
                merged_employee = {
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
                    "status": "Lỗi dữ liệu API2"
                }
                stats['from_db1_only'] += 1 # Coi như chỉ có từ DB1 nếu DB2 lỗi
                
        elif emp1:  # Chỉ có trong API1
            merged_employee = {
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
                "status": "Chỉ có API1"
            }
            stats['from_db1_only'] += 1
            
        elif emp2:  # Chỉ có trong API2
            first_name, last_name = name_key.split('_')
            try:
                merged_employee = {
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
                    "status": "Chỉ có API2"
                }
                stats['from_db2_only'] += 1
                stats['total_pay_rate'] += float(emp2.get("payRate", 0))
            except (ValueError, TypeError):
                logger.warning(f"Invalid numeric data for employee {name_key} from API2")
                # Không thêm vào nếu chỉ có trong API2 và dữ liệu lỗi
        
        # Chỉ thêm vào danh sách nếu tạo thành công
        if merged_employee:
            merged_data.append(merged_employee)
    
    stats['total_employees'] = len(merged_data)
    
    # Log chi tiết hơn trước khi return
    logger.info("--- Thống kê dữ liệu Merge ---")
    logger.info(f"Tổng số bản ghi: {stats['total_employees']}")
    logger.info(f"Từ cả hai API: {stats['from_both']}")
    logger.info(f"Chỉ từ API1: {stats['from_db1_only']}")
    logger.info(f"Chỉ từ API2: {stats['from_db2_only']}")
    logger.info(f"Tổng Pay Rate: ${stats['total_pay_rate']:.2f}")
    logger.info("--- Dữ liệu Merge chi tiết (Tối đa 5 bản ghi) ---")
    for i, emp in enumerate(merged_data[:5]): # Chỉ log 5 bản ghi đầu
        logger.info(f"Bản ghi {i+1}: {emp}")
    if len(merged_data) > 5:
        logger.info("... và các bản ghi khác.")
    logger.info("-----------------------------------")

    return {
        'merged_data': merged_data,
        'stats': stats
    }

def update_redis_data():
    """Cập nhật dữ liệu trong Redis"""
    try:
        result = merge_employee_data()
        if result and 'merged_data' in result:
            # Lưu danh sách nhân viên và stats riêng biệt
            redis_service.client.set('merged_employee_list', json.dumps(result['merged_data']))
            if 'stats' in result:
                 redis_service.client.set('merged_stats', json.dumps(result['stats']))
            
            logger.info("Đã cập nhật dữ liệu merge vào Redis.")
            
            # Gửi chỉ danh sách nhân viên qua socket
            socketio.emit('data_updated', {'merged_data': result['merged_data']})
            return True
        else:
            logger.error("Không thể merge dữ liệu hoặc kết quả không hợp lệ.")
            return False
    except Exception as e:
        logger.error(f"Error updating Redis data: {e}")
        return False

# Thêm route để cập nhật dữ liệu thủ công
@app.route('/api/refresh-data', methods=['POST'])
def refresh_data():
    if update_redis_data():
        return jsonify({"success": True, "message": "Data updated successfully"})
    return jsonify({"success": False, "message": "Error updating data"}), 500

# Thêm hàm để cập nhật dữ liệu định kỳ
def schedule_data_updates():
    while True:
        update_redis_data()
        time.sleep(60)  # Cập nhật mỗi 1 phút

if __name__ == '__main__':
    logger.info("Khởi động ứng dụng") 
    start_consumers()
    # Khởi động thread cập nhật dữ liệu
    threading.Thread(target=schedule_data_updates, daemon=True).start()
    socketio.run(app, debug=True)