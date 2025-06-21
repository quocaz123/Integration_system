# Hệ Thống Quản Lý Nhân Viên (Employee Management System)

## Giới Thiệu
Hệ thống Quản lý Nhân viên là một ứng dụng web được xây dựng bằng Flask, cho phép đồng bộ và quản lý thông tin nhân viên từ hai API khác nhau. Hệ thống cung cấp giao diện người dùng để xem, thêm, sửa và xóa thông tin nhân viên, đồng thời đảm bảo dữ liệu được đồng bộ giữa hai hệ thống API.

## Tính Năng Chính
- **Đồng Bộ Dữ Liệu**: Tự động đồng bộ dữ liệu từ hai API khác nhau
- **Quản Lý Nhân Viên**: 
  - Thêm nhân viên mới
  - Cập nhật thông tin nhân viên
  - Xóa nhân viên
  - Xem danh sách nhân viên
- **Xử Lý Lỗi**: Hệ thống retry tự động khi gặp lỗi
- **Real-time Updates**: Sử dụng WebSocket để cập nhật dữ liệu real-time
- **Validation**: Kiểm tra dữ liệu đầu vào và email trùng lặp

## Cấu Trúc Project
```
New-folder/main/
├── app.py                 # File chính của ứng dụng Flask
├── services/             
│   ├── config.py         # Cấu hình hệ thống
│   ├── redis_service.py  # Xử lý cache với Redis
│   ├── queue_service.py  # Quản lý hàng đợi
│   ├── employee_service.py # Logic xử lý nhân viên
│   ├── employee_creator.py # Tạo nhân viên mới
│   └── consumer_service.py # Xử lý consumer
├── templates/
│   └── index.html        # Giao diện người dùng
└── static/
    ├── css/             # Style sheets
    └── js/              # JavaScript files
```

## Yêu Cầu Hệ Thống
- Python 3.7+
- Redis Server
- Các thư viện Python:
  - Flask
  - Flask-SocketIO
  - Flask-CORS
  - Redis-py
  - Requests
  - aiohttp

## Cài Đặt và Chạy
1. **Cài đặt các gói phụ thuộc**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Cấu hình Redis**:
   - Đảm bảo Redis Server đang chạy
   - Cập nhật cấu hình Redis trong `services/config.py`

3. **Khởi động ứng dụng**:
   ```bash
   python app.py
   ```

## API Endpoints

### 1. Quản Lý Nhân Viên
- `GET /`: Trang chủ với danh sách nhân viên
- `POST /api/employee/create`: Tạo nhân viên mới
- `POST /api/employee/update`: Cập nhật thông tin nhân viên
- `POST /api/employee/delete`: Xóa nhân viên
- `GET /api/employee/check-email`: Kiểm tra email tồn tại

### 2. Đồng Bộ Dữ Liệu
- `POST /api/refresh-data`: Cập nhật dữ liệu thủ công

## WebSocket Events
- `connect`: Kết nối client
- `disconnect`: Ngắt kết nối client
- `update_data`: Gửi dữ liệu cập nhật
- `data_updated`: Thông báo dữ liệu đã được cập nhật

## Xử Lý Lỗi và Retry
- Hệ thống tự động thêm các yêu cầu thất bại vào hàng đợi retry
- Consumer service xử lý các yêu cầu trong hàng đợi
- Logging chi tiết cho việc debug và theo dõi

## Tính Năng Chi Tiết

### 1. Employee Service
- Xử lý CRUD operations cho nhân viên
- Validate dữ liệu đầu vào
- Transform dữ liệu cho phù hợp với từng API
- Xử lý merge dữ liệu từ hai API

### 2. Queue Service
- Quản lý hàng đợi message
- Xử lý retry cho các request thất bại
- Theo dõi trạng thái message

### 3. Redis Service
- Cache dữ liệu
- Lưu trữ trạng thái
- Quản lý session

### 4. Consumer Service
- Xử lý async các message trong queue
- Retry logic cho các request thất bại
- Logging và monitoring

## Bảo Mật
- Validate input data
- Xử lý lỗi an toàn
- Kiểm tra quyền truy cập API
- Bảo vệ thông tin nhạy cảm

## Logging
- Logging chi tiết cho mọi operation
- Log rotation
- Error tracking
- Performance monitoring

## Hiệu Năng
- Caching với Redis
- Async processing
- Batch processing cho các operation lớn
- Real-time updates với WebSocket



## License
MIT License

