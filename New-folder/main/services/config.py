import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Redis config
    REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
    REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
    REDIS_DB = int(os.getenv('REDIS_DB', 0))
    
    # Base API URLs
    API1_BASE = 'http://localhost:19335'
    API2_BASE = 'http://localhost:8080'
    
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
    API2_GET_EMPLOYEES = API2_URL + '/employee/list'
    
    # API endpoints for creating employee
    API1_CREATE_EMPLOYEE = API1_URL + '/CreateEmployee'
    API2_CREATE_EMPLOYEE = API2_URL + '/employee/add' 