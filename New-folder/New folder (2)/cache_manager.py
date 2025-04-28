import redis
import json
import hashlib
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

class CacheManager:
    def __init__(self):
        self.redis_client = redis.Redis(
            host=os.getenv('REDIS_HOST'),
            port=int(os.getenv('REDIS_PORT')),
            db=int(os.getenv('REDIS_DB'))
        )
        self.cache_ttl = int(os.getenv('CACHE_TTL'))

    def get_cache(self, key):
        """Lấy dữ liệu từ cache"""
        cached_data = self.redis_client.get(key)
        if cached_data:
            return json.loads(cached_data)
        return None

    def set_cache(self, key, data):
        """Lưu dữ liệu vào cache"""
        self.redis_client.setex(
            key,
            self.cache_ttl,
            json.dumps(data)
        )

    def generate_hash(self, data):
        """Tạo hash cho dữ liệu"""
        return hashlib.md5(json.dumps(data, sort_keys=True).encode()).hexdigest()

    def check_data_changes(self, new_data, cache_key):
        """Kiểm tra thay đổi dữ liệu"""
        cached_data = self.get_cache(cache_key)
        
        if not cached_data:
            # Nếu không có cache, lưu dữ liệu mới
            self.set_cache(cache_key, new_data)
            return True

        # So sánh hash
        new_hash = self.generate_hash(new_data)
        cached_hash = self.generate_hash(cached_data)

        if new_hash != cached_hash:
            # Nếu có thay đổi, cập nhật cache
            self.set_cache(cache_key, new_data)
            return True

        return False

    def get_cache_stats(self):
        """Lấy thông tin về cache"""
        keys = self.redis_client.keys('*')
        stats = {
            'total_keys': len(keys),
            'keys': [key.decode() for key in keys],
            'ttl': self.cache_ttl
        }
        return stats 