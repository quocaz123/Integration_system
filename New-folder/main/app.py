from flask import Flask, render_template, jsonify, request
from flask_socketio import SocketIO, emit
from flask_cors import CORS
import threading
import time
import logging
import json
from services.config import Config
from services.redis_service import RedisService
from services.queue_service import QueueService
from services.consumer_service import API1Consumer, API2Consumer
from services.employee_service import merge_employee_data, EmployeeService
from services.employee_creator import EmployeeCreator
import requests

# Cấu hình logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')
queue_service = QueueService()
redis_service = RedisService()
employee_creator = EmployeeCreator()
employee_service = EmployeeService()

@socketio.on('connect')
def handle_connect():
    logger.info("Client connected")
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
    initial_data = {
        'stats': {
            'total_employees': 0, 'total_pay_rate': 0,
            'from_db1_only': 0, 'from_db2_only': 0, 'from_both': 0
        },
        'merged_data': []
    }

    update_redis_data()

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
        result = employee_creator.create_employee(employee_data)
        
        # Nếu có lỗi, thêm vào retry queue
        if result['api1_status'] == 'FAILED':
            employee_creator.redis.add_to_retry_queue(employee_data, 'api1')
        if result['api2_status'] == 'FAILED':
            employee_creator.redis.add_to_retry_queue(employee_data, 'api2')
        
        # Thêm trường success cho FE dễ kiểm tra
        result['success'] = (result['api1_status'] == 'SUCCESS' and result['api2_status'] == 'SUCCESS')
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error creating employee: {e}")
        return jsonify({
            "success": False,
            "message": str(e)
        }), 500


def start_consumers():
    api1_consumer = API1Consumer()
    api2_consumer = API2Consumer()
    
    threading.Thread(target=api1_consumer.run, daemon=True).start()
    threading.Thread(target=api2_consumer.run, daemon=True).start()
    threading.Thread(target=employee_creator.retry_failed_creations, daemon=True).start()

def update_redis_data():
    """Cập nhật dữ liệu trong Redis"""
    try:
        result = merge_employee_data()
        if result and 'merged_data' in result:
            redis_service.client.set('merged_employee_list', json.dumps(result['merged_data']))
            if 'stats' in result:
                 redis_service.client.set('merged_stats', json.dumps(result['stats']))
            
            logger.info("Đã cập nhật dữ liệu merge vào Redis.")
            socketio.emit('data_updated', {'merged_data': result['merged_data']})
            return True
        else:
            logger.error("Không thể merge dữ liệu hoặc kết quả không hợp lệ.")
            return False
    except Exception as e:
        logger.error(f"Error updating Redis data: {e}")
        return False

@app.route('/api/refresh-data', methods=['POST'])
def refresh_data():
    if update_redis_data():
        return jsonify({"success": True, "message": "Data updated successfully"})
    return jsonify({"success": False, "message": "Error updating data"}), 500

def schedule_data_updates():
    while True:
        update_redis_data()
        time.sleep(60)  # Cập nhật mỗi 1 phút

@app.route('/api/employee/delete', methods=['POST'])
def delete_employee():
    try:
        data = request.json
        employee_number = data.get('employeeNumber')
        # Lấy thông tin nhân viên từ merged data (nếu cần lấy idEmployee cho API2)
        merged_data_json = redis_service.client.get('merged_employee_list')
        merged_data = json.loads(merged_data_json) if merged_data_json else []
        employee = next((e for e in merged_data if str(e.get('employeeNumber')) == str(employee_number)), None)
        if not employee:
            logger.error(f"Không tìm thấy nhân viên với employeeNumber: {employee_number}")
            return jsonify({'success': False, 'message': 'Không tìm thấy nhân viên'}), 404
        id_employee = employee.get('idEmployee')
        # Gọi API xóa thật
        api1_url = f"http://localhost:19335/API/DeleteEmployee/{employee_number}"
        api2_url = f"http://localhost:8080/springapp/api/employee/{employee_number}"
        logger.info(f"[API1] Chuẩn bị xóa với URL: {api1_url}")
        logger.info(f"[API2] Chuẩn bị xóa với URL: {api2_url}")
        api1_success = False
        api2_success = False
        # Xóa ở API1
        try:
            resp1 = requests.delete(api1_url, timeout=5)
            api1_success = resp1.status_code == 200
            logger.info(f"[API1] Xóa nhân viên {employee_number}: {'Thành công' if api1_success else 'Thất bại'} (status {resp1.status_code})")
        except Exception as ex1:
            logger.error(f"[API1] Lỗi khi xóa nhân viên {employee_number}: {ex1}")
        # Xóa ở API2
        try:
            resp2 = requests.delete(api2_url, timeout=5)
            api2_success = resp2.status_code == 200
            logger.info(f"[API2] Xóa nhân viên {id_employee}: {'Thành công' if api2_success else 'Thất bại'} (status {resp2.status_code})")
        except Exception as ex2:
            logger.error(f"[API2] Lỗi khi xóa nhân viên {id_employee}: {ex2}")
        return jsonify({
            'success': api1_success and api2_success,
            'api1_success': api1_success,
            'api2_success': api2_success
        })
    except Exception as e:
        logger.error(f"Lỗi khi xóa nhân viên: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/employee/update', methods=['POST'])
def update_employee():
    try:
        employee_data = request.json
        result = employee_service.update_employee(employee_data)
        result['success'] = (result['api1_status'] == 'SUCCESS' or result['api2_status'] == 'SUCCESS')
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error updating employee: {e}")
        return jsonify({
            "success": False,
            "message": str(e)
        }), 500

if __name__ == '__main__':
    logger.info("Khởi động ứng dụng") 
    start_consumers()
    threading.Thread(target=schedule_data_updates, daemon=True).start()
    socketio.run(app, debug=True) 