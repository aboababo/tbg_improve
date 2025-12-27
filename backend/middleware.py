"""
OsaGaming CRM - Middleware для безопасности и оптимизации
=========================================================

Rate limiting, CSRF protection, request logging
"""

from functools import wraps
from datetime import datetime, timedelta
from flask import request, jsonify, session
import time
import threading
import logging

logger = logging.getLogger(__name__)

# Rate limiting storage (thread-safe, в продакшене использовать Redis)
_rate_limit_storage = {}
_rate_limit_lock = threading.Lock()

# Настройки rate limiting
RATE_LIMIT_CONFIG = {
    'default': {'requests': 100, 'window': 60},  # 100 запросов в минуту
    '/api/chats': {'requests': 200, 'window': 60},  # 200 запросов в минуту для чатов
    '/api/messages': {'requests': 300, 'window': 60},  # 300 запросов в минуту для сообщений
    '/api/sync': {'requests': 10, 'window': 60},  # 10 запросов в минуту для синхронизации
}

def rate_limit(max_requests=None, window=None, endpoint=None):
    """
    Декоратор для ограничения частоты запросов (thread-safe)
    
    Args:
        max_requests: Максимальное количество запросов (если None, берется из конфига)
        window: Временное окно в секундах (если None, берется из конфига)
        endpoint: Ключ endpoint для конфига (если None, используется путь запроса)
    """
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            # Определяем конфигурацию rate limiting
            config_key = endpoint or request.path
            config = RATE_LIMIT_CONFIG.get(config_key, RATE_LIMIT_CONFIG['default'])
            
            req_limit = max_requests or config['requests']
            time_window = window or config['window']
            
            # Получаем идентификатор клиента
            client_id = request.remote_addr
            if 'user_id' in session:
                client_id = f"{client_id}_{session['user_id']}"
            
            # Получаем текущее время
            now = time.time()
            
            # Thread-safe очистка старых записей
            with _rate_limit_lock:
                if client_id in _rate_limit_storage:
                    _rate_limit_storage[client_id] = [
                        req_time for req_time in _rate_limit_storage[client_id]
                        if now - req_time < time_window
                    ]
                else:
                    _rate_limit_storage[client_id] = []
                
                # Проверяем лимит
                if len(_rate_limit_storage[client_id]) >= req_limit:
                    logger.warning(f"Rate limit exceeded for {client_id} on {config_key}: {len(_rate_limit_storage[client_id])}/{req_limit}")
                    # Для HTML страниц возвращаем HTML ответ
                    if request.path.startswith('/login') and request.method == 'GET':
                        from flask import render_template
                        return render_template('login.html', error=f'Превышен лимит запросов. Подождите {time_window} секунд.'), 429
                    return jsonify({
                        'error': 'Too many requests',
                        'message': f'Превышен лимит запросов. Максимум {req_limit} запросов в {time_window} секунд.',
                        'retry_after': time_window
                    }), 429
                
                # Добавляем текущий запрос
                _rate_limit_storage[client_id].append(now)
            
            return f(*args, **kwargs)
        return decorated_function
    return decorator

def csrf_protect(f):
    """
    Простая CSRF защита через проверку Origin/Referer
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # Пропускаем GET запросы
        if request.method == 'GET':
            return f(*args, **kwargs)
        
        # Проверяем Origin для API запросов
        origin = request.headers.get('Origin')
        referer = request.headers.get('Referer')
        
        # Разрешаем запросы с того же домена
        if origin and origin.startswith(request.host_url[:-1]):
            return f(*args, **kwargs)
        
        if referer and referer.startswith(request.host_url[:-1]):
            return f(*args, **kwargs)
        
        # Для API запросов проверяем токен
        csrf_token = request.headers.get('X-CSRF-Token') or request.form.get('csrf_token')
        if csrf_token and csrf_token == session.get('csrf_token'):
            return f(*args, **kwargs)
        
        # Если это не API запрос, разрешаем (для форм)
        if not request.is_json:
            return f(*args, **kwargs)
        
        return jsonify({'error': 'CSRF token missing or invalid'}), 403
    
    return decorated_function

def log_request(f):
    """
    Логирование запросов для мониторинга (структурированное)
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        start_time = time.time()
        
        # Логируем начало запроса
        logger.info(f"Request: {request.method} {request.path} from {request.remote_addr}")
        
        try:
            response = f(*args, **kwargs)
            duration = (time.time() - start_time) * 1000  # в миллисекундах
            
            # Логируем результат
            status_code = response.status_code if hasattr(response, 'status_code') else 200
            logger.info(f"Response: {request.method} {request.path} - {status_code} - {duration:.2f}ms")
            
            # Логируем медленные запросы (>1 секунда)
            if duration > 1000:
                logger.warning(f"Медленный запрос: {request.method} {request.path} - {duration:.2f}ms")
            
            return response
        except Exception as e:
            duration = (time.time() - start_time) * 1000
            logger.error(f"Error in {request.method} {request.path}: {str(e)} - {duration:.2f}ms", exc_info=True)
            raise
    return decorated_function

