"""
OsaGaming CRM - Redis кэширование
==================================

Кэширование с использованием Redis (с fallback на in-memory)
"""

import os
import json
import hashlib
from datetime import datetime, timedelta
from functools import wraps
import logging

logger = logging.getLogger(__name__)

# Попытка импортировать Redis
_redis_client = None
_redis_available = False

try:
    import redis
    redis_host = os.environ.get('REDIS_HOST', 'localhost')
    redis_port = int(os.environ.get('REDIS_PORT', 6379))
    redis_db = int(os.environ.get('REDIS_DB', 0))
    redis_password = os.environ.get('REDIS_PASSWORD')
    
    try:
        _redis_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            db=redis_db,
            password=redis_password,
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=2
        )
        # Проверяем подключение
        _redis_client.ping()
        _redis_available = True
        logger.info(f"Redis подключен: {redis_host}:{redis_port}")
    except Exception as e:
        logger.warning(f"Redis недоступен, используется in-memory кэш: {e}")
        _redis_client = None
        _redis_available = False
except ImportError:
    logger.warning("Redis не установлен, используется in-memory кэш")
    _redis_available = False

# Fallback: in-memory кэш
_memory_cache = {}
_memory_cache_timestamps = {}

# Настройки TTL
CACHE_TTL = {
    'stats': 60,  # 1 минута
    'chats': 30,  # 30 секунд
    'shops': 300,  # 5 минут
    'users': 300,  # 5 минут
    'settings': 600,  # 10 минут
    'listings': 600,  # 10 минут
    'default': 60  # 1 минута по умолчанию
}


def get_redis_client():
    """Получение Redis клиента"""
    return _redis_client if _redis_available else None


def get_cache_key(prefix, *args, **kwargs):
    """Генерация ключа кэша"""
    key_data = {
        'prefix': prefix,
        'args': args,
        'kwargs': sorted(kwargs.items())
    }
    key_string = json.dumps(key_data, sort_keys=True, default=str)
    return f"crm:{prefix}:{hashlib.md5(key_string.encode()).hexdigest()}"


def get_cached(key):
    """Получение значения из кэша"""
    if _redis_available and _redis_client:
        try:
            value = _redis_client.get(key)
            if value:
                return json.loads(value)
        except Exception as e:
            logger.warning(f"Ошибка чтения из Redis: {e}")
    
    # Fallback: in-memory
    if key in _memory_cache:
        if key in _memory_cache_timestamps:
            ttl = _memory_cache_timestamps[key].get('ttl', CACHE_TTL['default'])
            if datetime.now() - _memory_cache_timestamps[key]['timestamp'] > timedelta(seconds=ttl):
                del _memory_cache[key]
                del _memory_cache_timestamps[key]
                return None
        return _memory_cache[key]
    
    return None


def set_cached(key, value, ttl=None):
    """Сохранение значения в кэш"""
    ttl = ttl or CACHE_TTL['default']
    
    if _redis_available and _redis_client:
        try:
            _redis_client.setex(key, ttl, json.dumps(value, default=str))
            return
        except Exception as e:
            logger.warning(f"Ошибка записи в Redis: {e}")
    
    # Fallback: in-memory
    _memory_cache[key] = value
    _memory_cache_timestamps[key] = {
        'timestamp': datetime.now(),
        'ttl': ttl
    }


def invalidate_cache(prefix=None):
    """Инвалидация кэша"""
    if _redis_available and _redis_client:
        try:
            if prefix:
                pattern = f"crm:{prefix}:*"
                keys = _redis_client.keys(pattern)
                if keys:
                    _redis_client.delete(*keys)
            else:
                # Очищаем все ключи с префиксом crm:
                keys = _redis_client.keys("crm:*")
                if keys:
                    _redis_client.delete(*keys)
            return
        except Exception as e:
            logger.warning(f"Ошибка инвалидации Redis кэша: {e}")
    
    # Fallback: in-memory
    if prefix:
        keys_to_delete = [k for k in _memory_cache.keys() if k.startswith(f"crm:{prefix}:")]
        for key in keys_to_delete:
            _memory_cache.pop(key, None)
            _memory_cache_timestamps.pop(key, None)
    else:
        _memory_cache.clear()
        _memory_cache_timestamps.clear()


def cached(ttl=None, prefix='default'):
    """Декоратор для кэширования"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            cache_key = get_cache_key(prefix, *args, **kwargs)
            
            # Пытаемся получить из кэша
            cached_value = get_cached(cache_key)
            if cached_value is not None:
                return cached_value
            
            # Выполняем функцию и кэшируем результат
            result = func(*args, **kwargs)
            set_cached(cache_key, result, ttl or CACHE_TTL.get(prefix, CACHE_TTL['default']))
            
            return result
        return wrapper
    return decorator

