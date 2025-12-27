"""
OsaGaming CRM - Health Check модуль
====================================

Проверка работоспособности системы:
- Health check endpoint
- Readiness check endpoint
- Metrics endpoint
"""

from flask import jsonify, request
from database import get_db_connection
import time
import os
import logging

logger = logging.getLogger(__name__)

# Глобальные метрики
_metrics = {
    'requests_total': 0,
    'requests_by_method': {},
    'requests_by_endpoint': {},
    'response_times': [],
    'errors_total': 0,
    'errors_by_type': {},
    'start_time': time.time()
}


def record_request(method, endpoint, duration, status_code):
    """Запись метрик запроса"""
    _metrics['requests_total'] += 1
    
    # Метод
    _metrics['requests_by_method'][method] = _metrics['requests_by_method'].get(method, 0) + 1
    
    # Endpoint
    _metrics['requests_by_endpoint'][endpoint] = _metrics['requests_by_endpoint'].get(endpoint, 0) + 1
    
    # Время ответа
    _metrics['response_times'].append(duration)
    # Храним только последние 1000 записей
    if len(_metrics['response_times']) > 1000:
        _metrics['response_times'] = _metrics['response_times'][-1000:]
    
    # Ошибки
    if status_code >= 400:
        _metrics['errors_total'] += 1
        error_type = f"{status_code // 100}xx"
        _metrics['errors_by_type'][error_type] = _metrics['errors_by_type'].get(error_type, 0) + 1


def get_health_status():
    """Проверка работоспособности системы"""
    status = {
        'status': 'healthy',
        'timestamp': time.time(),
        'uptime_seconds': time.time() - _metrics['start_time'],
        'checks': {}
    }
    
    # Проверка БД
    try:
        conn = get_db_connection()
        conn.execute('SELECT 1').fetchone()
        conn.close()
        status['checks']['database'] = {'status': 'ok', 'response_time_ms': 0}
    except Exception as e:
        status['status'] = 'unhealthy'
        status['checks']['database'] = {'status': 'error', 'error': str(e)}
    
    # Проверка Redis (если доступен)
    try:
        from cache import get_redis_client
        redis_client = get_redis_client()
        if redis_client:
            start = time.time()
            redis_client.ping()
            status['checks']['redis'] = {
                'status': 'ok',
                'response_time_ms': (time.time() - start) * 1000
            }
        else:
            status['checks']['redis'] = {'status': 'not_configured'}
    except Exception as e:
        status['checks']['redis'] = {'status': 'not_available', 'error': str(e)}
    
    # Проверка дискового пространства
    try:
        import shutil
        total, used, free = shutil.disk_usage(os.path.dirname(os.path.abspath(__file__)))
        status['checks']['disk'] = {
            'status': 'ok',
            'free_gb': round(free / (1024**3), 2),
            'used_percent': round(used / total * 100, 2)
        }
        if free / total < 0.1:  # Меньше 10% свободно
            status['status'] = 'degraded'
            status['checks']['disk']['status'] = 'warning'
    except Exception as e:
        status['checks']['disk'] = {'status': 'error', 'error': str(e)}
    
    # Проверка Avito API для всех магазинов
    try:
        from avito_api import AvitoAPI
        conn = get_db_connection()
        shops = conn.execute('''
            SELECT id, name, client_id, client_secret, user_id, webhook_registered
            FROM avito_shops
            WHERE is_active = 1 AND client_id IS NOT NULL AND client_secret IS NOT NULL AND user_id IS NOT NULL
        ''').fetchall()
        conn.close()
        
        avito_status = {
            'total_shops': len(shops),
            'shops_ok': 0,
            'shops_error': 0,
            'webhooks_registered': 0,
            'details': []
        }

        MAX_CHECK = 20
        if len(shops) > MAX_CHECK:
            status['status'] = 'degraded'
            avito_status['truncated'] = True
            shops_to_check = shops[:MAX_CHECK]
        else:
            shops_to_check = shops
        
        for shop in shops_to_check:
            shop_dict = dict(shop)
            shop_detail = {
                'shop_id': shop_dict['id'],
                'shop_name': shop_dict['name'],
                'status': 'unknown'
            }
            try:
                api = AvitoAPI(client_id=shop_dict['client_id'], client_secret=shop_dict['client_secret'])
                health = api.health_check()
                if health.get('status') == 'ok':
                    shop_detail['status'] = 'ok'
                    shop_detail['latency_ms'] = health.get('latency_ms')
                    shop_detail['expires_at'] = health.get('expires_at')
                    avito_status['shops_ok'] += 1
                else:
                    shop_detail['status'] = 'error'
                    shop_detail['error'] = health.get('details', 'Connection test failed')
                    avito_status['shops_error'] += 1
            except Exception as e:
                shop_detail['status'] = 'error'
                shop_detail['error'] = str(e)[:100]
                avito_status['shops_error'] += 1
            
            # Проверяем webhook_registered (может быть None для старых записей)
            webhook_registered = bool(shop_dict.get('webhook_registered')) if 'webhook_registered' in shop_dict else False
            if webhook_registered:
                avito_status['webhooks_registered'] += 1
            shop_detail['webhook_registered'] = webhook_registered
            
            avito_status['details'].append(shop_detail)
        
        if avito_status['shops_error'] > 0:
            status['status'] = 'degraded'
        
        status['checks']['avito_api'] = avito_status
    except Exception as e:
        status['checks']['avito_api'] = {'status': 'error', 'error': str(e)}
    
    return status


def get_readiness_status():
    """Проверка готовности системы к работе"""
    status = {
        'ready': True,
        'timestamp': time.time(),
        'checks': {}
    }
    
    # Проверка БД
    try:
        conn = get_db_connection()
        conn.execute('SELECT 1').fetchone()
        conn.close()
        status['checks']['database'] = True
    except Exception as e:
        status['ready'] = False
        status['checks']['database'] = False
        status['error'] = f'Database error: {str(e)}'
    
    return status


def get_metrics():
    """Получение метрик производительности"""
    response_times = _metrics.get('response_times', [])
    
    avg_response_time = 0
    p50_response_time = 0
    p95_response_time = 0
    p99_response_time = 0
    
    if response_times:
        sorted_times = sorted(response_times)
        avg_response_time = sum(sorted_times) / len(sorted_times)
        p50_response_time = sorted_times[int(len(sorted_times) * 0.5)]
        p95_response_time = sorted_times[int(len(sorted_times) * 0.95)] if len(sorted_times) > 20 else sorted_times[-1]
        p99_response_time = sorted_times[int(len(sorted_times) * 0.99)] if len(sorted_times) > 100 else sorted_times[-1]
    
    uptime = time.time() - _metrics['start_time']
    requests_per_second = _metrics['requests_total'] / uptime if uptime > 0 else 0
    
    return {
        'requests': {
            'total': _metrics['requests_total'],
            'per_second': round(requests_per_second, 2),
            'by_method': _metrics['requests_by_method'],
            'by_endpoint': dict(list(_metrics['requests_by_endpoint'].items())[:20])  # Топ 20
        },
        'response_times': {
            'avg_ms': round(avg_response_time, 2),
            'p50_ms': round(p50_response_time, 2),
            'p95_ms': round(p95_response_time, 2),
            'p99_ms': round(p99_response_time, 2)
        },
        'errors': {
            'total': _metrics['errors_total'],
            'by_type': _metrics['errors_by_type']
        },
        'uptime_seconds': round(uptime, 2)
    }


def register_health_routes(app):
    """Регистрация health check endpoints"""
    
    @app.route('/health', methods=['GET'])
    def health():
        """Health check endpoint"""
        status = get_health_status()
        status_code = 200 if status['status'] == 'healthy' else 503
        return jsonify(status), status_code
    
    @app.route('/ready', methods=['GET'])
    def ready():
        """Readiness check endpoint"""
        status = get_readiness_status()
        status_code = 200 if status['ready'] else 503
        return jsonify(status), status_code
    
    @app.route('/metrics', methods=['GET'])
    def metrics():
        """Metrics endpoint"""
        # Простая базовая аутентификация для метрик
        auth_header = request.headers.get('Authorization', '')
        metrics_token = os.environ.get('METRICS_TOKEN', '')
        
        if metrics_token and auth_header != f'Bearer {metrics_token}':
            return jsonify({'error': 'Unauthorized'}), 401
        
        return jsonify(get_metrics()), 200

