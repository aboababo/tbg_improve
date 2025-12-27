"""
OSAGAMING CRM - Основной файл приложения Flask
==============================================

Этот файл содержит:
- Инициализацию Flask приложения
- API endpoints для работы с данными
- Маршруты для отображения страниц
- Декораторы для безопасности и обработки ошибок
- Утилиты для валидации и логирования

Автор: OSAGAMING Development Team
Версия: 2.0
"""

from flask import Flask, jsonify, render_template, send_from_directory, request, session, redirect, url_for, flash, make_response
try:
    from flask_cors import CORS
    CORS_AVAILABLE = True
except ImportError:
    CORS_AVAILABLE = False
from functools import wraps
import json
import os
import re
import secrets
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any
from database import get_db_connection, init_database
from auth import authenticate_user, get_user_by_id, get_user_settings
from health import register_health_routes
import time

# ==================== ИНИЦИАЛИЗАЦИЯ ПРИЛОЖЕНИЯ ====================

# Создаем экземпляр Flask приложения
# __name__ используется для определения корневой директории проекта
app = Flask(__name__)

# Настройка логирования в файл
import logging
from logging.handlers import RotatingFileHandler

# Создаем папку для логов если её нет
log_dir = os.path.join(os.path.dirname(__file__), 'logs')
os.makedirs(log_dir, exist_ok=True)

# Настройка файлового логирования
file_handler = RotatingFileHandler(
    os.path.join(log_dir, 'app.log'),
    maxBytes=10240000,  # 10MB
    backupCount=10
)
file_handler.setFormatter(logging.Formatter(
    '%(asctime)s [%(levelname)s] %(name)s: %(message)s [in %(pathname)s:%(lineno)d]'
))
file_handler.setLevel(logging.INFO)
app.logger.addHandler(file_handler)
app.logger.setLevel(logging.INFO)

# Также выводим в консоль
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter(
    '%(asctime)s [%(levelname)s] %(message)s'
))
console_handler.setLevel(logging.INFO)
app.logger.addHandler(console_handler)

# Устанавливаем секретный ключ для сессий
# Используется для шифрования данных сессии (cookies)
# ВАЖНО: SECRET_KEY должен быть постоянным! Если он меняется, все существующие сессии становятся невалидными
# Если переменная окружения SECRET_KEY не установлена, используем фиксированный ключ из файла или генерируем один раз
_secret_key_file = os.path.join(os.path.dirname(__file__), '.secret_key')
if os.environ.get('SECRET_KEY'):
    app.secret_key = os.environ.get('SECRET_KEY')
    app.logger.info("[INIT] Используется SECRET_KEY из переменной окружения")
elif os.path.exists(_secret_key_file):
    # Читаем сохраненный ключ из файла
    with open(_secret_key_file, 'r') as f:
        app.secret_key = f.read().strip()
    app.logger.info("[INIT] Используется SECRET_KEY из файла .secret_key")
else:
    # Генерируем новый ключ и сохраняем в файл
    app.secret_key = secrets.token_hex(32)
    try:
        with open(_secret_key_file, 'w') as f:
            f.write(app.secret_key)
        app.logger.info("[INIT] Сгенерирован новый SECRET_KEY и сохранен в .secret_key")
    except Exception as e:
        app.logger.warning(f"[INIT] Не удалось сохранить SECRET_KEY в файл: {e}")
        app.logger.warning("[INIT] ⚠️ ВНИМАНИЕ: При перезапуске сервера все сессии станут невалидными!")

# Настройки сессии для правильной работы с cookies
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'  # Разрешаем отправку cookies при cross-site запросах
app.config['SESSION_COOKIE_HTTPONLY'] = True   # Защита от XSS
app.config['SESSION_COOKIE_SECURE'] = os.environ.get('SESSION_COOKIE_SECURE', 'False').lower() == 'true'  # HTTPS только в продакшене
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=7)  # Сессия живет 7 дней

# Включаем CORS (Cross-Origin Resource Sharing) для работы с фронтендом
# Ограничиваем список разрешённых источников через переменную окружения CORS_ORIGINS
# (например: "http://localhost:3000,https://crm.example.com")
if CORS_AVAILABLE:
    cors_origins_env = os.environ.get('CORS_ORIGINS')
    cors_origins = [o.strip() for o in cors_origins_env.split(',')] if cors_origins_env else ['http://localhost:3000', 'http://127.0.0.1:3000']
    CORS(app, supports_credentials=True, origins=cors_origins)
    app.logger.info("[INIT] CORS включен")
else:
    app.logger.warning("[INIT] CORS отключен (flask_cors не установлен)")

# Инициализируем базу данных при старте приложения
# Создает все необходимые таблицы, индексы и тестовые данные
try:
    from database import safe_init_database, get_db_connection
    safe_init_database()
    # Инициализируем глобальное соединение при старте
    get_db_connection()
except Exception as e:
    app.logger.warning(f"Не удалось автоматически инициализировать БД: {e}")
    # Пробуем обычную инициализацию
    try:
        init_database()
        # Инициализируем глобальное соединение при старте
        get_db_connection()
    except Exception as e2:
        app.logger.error(f"Критическая ошибка инициализации БД: {e2}")
        import traceback
        traceback.print_exc()
# Регистрируем health/readiness/metrics
register_health_routes(app)

# Регистрируем blueprints для API
# ВРЕМЕННО ОТКЛЮЧЕН: chats_bp конфликтует с @app.route('/api/chats') в app.py
# Используем обработчик из app.py вместо blueprint
try:
    from api.chats_api import chats_bp
    # app.register_blueprint(chats_bp)  # ОТКЛЮЧЕНО - используем обработчик из app.py
    app.logger.info("[INIT] Blueprint chats_api НЕ зарегистрирован (используется обработчик из app.py)")
except Exception as e:
    app.logger.warning(f"[INIT] Не удалось импортировать blueprint chats_api: {e}")

# ==================== КОНТЕКСТНЫЙ ПРОЦЕССОР ====================

@app.context_processor
def inject_user():
    """
    Автоматически добавляет объект user и настройки видимости вкладок во все шаблоны
    """
    # Всегда возвращаем словарь, даже при ошибках
    user = None
    tab_visibility = {}
    
    # ВАЖНО: context_processor может вызываться вне контекста запроса
    # Обертываем ВЕСЬ код в один большой try-except для максимальной безопасности
    # Если произойдет ЛЮБАЯ ошибка, просто возвращаем пустые значения
    try:
        from flask import has_request_context
        
        # Проверяем контекст запроса - обертываем в try-except
        try:
            if not has_request_context():
                return dict(user=user, tab_visibility=tab_visibility)
        except Exception:
            # Ошибка при проверке контекста - возвращаем пустые значения
            return dict(user=user, tab_visibility=tab_visibility)

        # Пытаемся получить доступ к session
        # ВАЖНО: даже простое обращение к session может вызвать RuntimeError
        # Обертываем ВСЕ обращения к session в try-except
        try:
            # Пытаемся получить user_id из session
            # Если session недоступна, это вызовет RuntimeError
            user_id = session.get('user_id')
        except Exception:
            # ЛЮБАЯ ошибка при доступе к session - это нормально вне HTTP-запроса
            # Не логируем, так как это ожидаемое поведение
            return dict(user=user, tab_visibility=tab_visibility)
        
        if user_id:
            try:
                user = get_user_by_id(user_id)
                
                # Загружаем индивидуальные настройки видимости вкладок для текущего пользователя
                if user:
                    user_role = user.get('role', '').strip()
                    user_id = user.get('id')
                    try:
                        conn = get_db_connection()
                        try:
                            # Сначала пытаемся загрузить индивидуальные настройки пользователя
                            user_setting = conn.execute(
                                'SELECT tab_visibility FROM user_settings WHERE user_id = ?',
                                (user_id,)
                            ).fetchone()
                            
                            if user_setting and user_setting['tab_visibility']:
                                import json
                                tab_visibility = json.loads(user_setting['tab_visibility'])
                            else:
                                # Если индивидуальных настроек нет, используем значения по умолчанию на основе роли
                                if user_role in ['admin', 'super_admin']:
                                    tab_visibility = {
                                        'dashboard': True, 'chats': True, 'buyout': True,
                                        'deliveries': True, 'quick_replies': True,
                                        'shops': True, 'analytics': True, 'settings': True
                                    }
                                elif user_role == 'manager':
                                    tab_visibility = {
                                        'dashboard': True, 'chats': True, 'buyout': True,
                                        'deliveries': True, 'quick_replies': True,
                                        'shops': False, 'analytics': False, 'settings': False
                                    }
                                else:
                                    tab_visibility = {}
                            
                            # Соединение глобальное, не закрываем
                        except Exception as e:
                            app.logger.warning(f"[INJECT_USER] Ошибка загрузки настроек видимости вкладок: {e}")
                            # Соединение глобальное, не закрываем
                            # Используем значения по умолчанию при ошибке
                            tab_visibility = {
                                'dashboard': True, 'chats': True, 'buyout': True,
                                'deliveries': True, 'quick_replies': True,
                                'shops': user_role in ['admin', 'super_admin'],
                                'analytics': user_role in ['admin', 'super_admin'],
                                'settings': user_role in ['admin', 'super_admin']
                            }
                    except Exception as e:
                        app.logger.warning(f"[INJECT_USER] Ошибка подключения к БД: {e}")
                        # Используем значения по умолчанию при ошибке
                        tab_visibility = {
                            'dashboard': True, 'chats': True, 'buyout': True,
                            'deliveries': True, 'quick_replies': True,
                            'shops': user_role in ['admin', 'super_admin'],
                            'analytics': user_role in ['admin', 'super_admin'],
                            'settings': user_role in ['admin', 'super_admin']
                        }
            except Exception as e:
                app.logger.warning(f"[INJECT_USER] Ошибка получения пользователя для шаблона: {e}")
    except Exception as e:
        # Критическая ошибка - логируем, но не падаем
        app.logger.error(f"[INJECT_USER] КРИТИЧЕСКАЯ ОШИБКА в context_processor: {e}", exc_info=True)
    
    return dict(user=user, tab_visibility=tab_visibility)

# ==================== УТИЛИТЫ И ВАЛИДАЦИЯ ====================

def validate_email(email):
    """
    Валидация email адреса
    
    Проверяет корректность формата email с помощью регулярного выражения.
    Поддерживает стандартный формат: user@domain.com
    
    Args:
        email (str): Email адрес для проверки
    
    Returns:
        bool: True если email валиден, False в противном случае
    
    Примеры:
        validate_email("user@example.com") -> True
        validate_email("invalid.email") -> False
    """
    # Регулярное выражение для проверки формата email
    # ^ - начало строки
    # [a-zA-Z0-9._%+-]+ - имя пользователя (один или более символов)
    # @ - символ @
    # [a-zA-Z0-9.-]+ - доменное имя
    # \. - точка перед доменом верхнего уровня
    # [a-zA-Z]{2,} - домен верхнего уровня (минимум 2 буквы)
    # $ - конец строки
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(pattern, email) is not None

def validate_phone(phone):
    """
    Валидация номера телефона
    
    Проверяет корректность формата международного номера телефона.
    Удаляет пробелы и дефисы перед проверкой.
    
    Args:
        phone (str): Номер телефона для проверки
    
    Returns:
        bool: True если номер валиден, False в противном случае
    
    Примеры:
        validate_phone("+79161234567") -> True
        validate_phone("7916-123-45-67") -> True (после удаления дефисов)
        validate_phone("123") -> False
    """
    # Удаляем пробелы и дефисы для унификации формата
    cleaned_phone = phone.replace(' ', '').replace('-', '')
    
    # Регулярное выражение для международного формата:
    # ^\+? - необязательный знак + в начале
    # [1-9] - первая цифра не может быть 0
    # \d{1,14} - от 1 до 14 цифр (стандарт E.164)
    pattern = r'^\+?[1-9]\d{1,14}$'
    return re.match(pattern, cleaned_phone) is not None

def require_auth(f):
    """
    Декоратор для проверки аутентификации пользователя
    
    Проверяет наличие user_id в сессии. Если пользователь не авторизован,
    возвращает ошибку 401 для API запросов или перенаправляет на страницу входа.
    
    Использование:
        @app.route('/api/data')
        @require_auth
        def get_data():
            # Этот код выполнится только если пользователь авторизован
            return jsonify({'data': 'secret'})
    
    Args:
        f: Функция-обработчик маршрута
    
    Returns:
        decorated_function: Обернутая функция с проверкой аутентификации
    """
    @wraps(f)  # Сохраняет метаданные оригинальной функции
    def decorated_function(*args, **kwargs):
        # Проверяем наличие user_id в сессии
        if 'user_id' not in session:
            # Диагностика для API запросов
            if request.path.startswith('/api/'):
                cookie_header = request.headers.get('Cookie', 'None')
                session_keys = list(session.keys())
                app.logger.warning(f"[REQUIRE_AUTH] API запрос без аутентификации: {request.path}")
                app.logger.warning(f"[REQUIRE_AUTH] Cookie present: {cookie_header != 'None'}, Session keys: {session_keys}")
                if cookie_header != 'None' and len(session_keys) == 0:
                    app.logger.warning(f"[REQUIRE_AUTH] ⚠️ Cookie отправлен, но сессия не расшифрована! Вероятно, SECRET_KEY изменился.")
                return jsonify({'error': 'Not authenticated', 'message': 'Session expired or invalid. Please login again.'}), 401
            # Иначе перенаправляем на страницу входа
            return redirect('/login')
        # Если пользователь авторизован, выполняем оригинальную функцию
        return f(*args, **kwargs)
    return decorated_function

def require_role(role):
    """
    Декоратор для проверки роли пользователя
    
    Поддерживает иерархию ролей: super_admin > admin > manager
    Супер-админ имеет доступ ко всем функциям админа и менеджера.
    
    Args:
        role (str): Требуемая роль ('manager', 'admin', 'super_admin')
    
    Returns:
        decorator: Декоратор для применения к функции
    """
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            user_role = session.get('user_role')

            # Иерархия ролей
            role_hierarchy = {
                'manager': 1,
                'admin': 2,
                'super_admin': 3
            }

            required_level = role_hierarchy.get(role, 999)
            user_level = role_hierarchy.get(user_role, 0)

            if user_level < required_level:
                if request.is_json:
                    return jsonify({'error': 'Access denied'}), 403
                return redirect('/login')
            return f(*args, **kwargs)
        return decorated_function
    return decorator

def handle_errors(f):
    """
    Декоратор для обработки ошибок в функциях-обработчиках
    
    Перехватывает все исключения, логирует их и возвращает понятный ответ
    пользователю. Предотвращает отображение технических деталей ошибок.
    
    Использование:
        @app.route('/api/data')
        @handle_errors
        def get_data():
            # Если здесь произойдет ошибка, она будет обработана
            return jsonify({'data': data})
    
    Args:
        f: Функция-обработчик маршрута
    
    Returns:
        decorated_function: Обернутая функция с обработкой ошибок
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        try:
            # Пытаемся выполнить функцию
            return f(*args, **kwargs)
        except Exception as error:
            # Логируем ошибку с полным стеком вызовов
            app.logger.error(f'Error in {f.__name__}: {str(error)}', exc_info=True)
            
            # Возвращаем ошибку в формате, соответствующем типу запроса
            # Если это API запрос (начинается с /api/), всегда возвращаем JSON
            if request.path.startswith('/api/'):
                return jsonify({'error': 'Internal server error', 'message': str(error)}), 500
            # Для HTML запросов возвращаем страницу ошибки
            return render_template('error.html', error=str(error)), 500
    return decorated_function

def check_name_columns(conn):
    """
    Проверяет, существуют ли колонки first_name и last_name в таблице users.
    
    Args:
        conn: Соединение с базой данных
        
    Returns:
        bool: True если обе колонки существуют, False в противном случае
    """
    try:
        cursor = conn.execute("PRAGMA table_info(users)")
        columns_info = cursor.fetchall()
        # PRAGMA table_info возвращает кортежи: (cid, name, type, notnull, dflt_value, pk)
        user_columns = [row[1] if len(row) > 1 else str(row[0]) for row in columns_info]
        return 'first_name' in user_columns and 'last_name' in user_columns
    except Exception:
        return False


def log_activity(user_id, action_type, action_description=None, target_type=None, target_id=None, metadata=None):
    """
    Логирование действий пользователя в базу данных
    
    Записывает все действия пользователей в таблицу activity_logs для аудита
    и анализа активности. Используется для отслеживания изменений, входа/выхода,
    отправки сообщений и других операций.
    
    Args:
        user_id (int): ID пользователя, выполнившего действие
        action_type (str): Тип действия (login, logout, send_message, update_delivery и т.д.)
        action_description (str, optional): Текстовое описание действия
        target_type (str, optional): Тип объекта, на который направлено действие (chat, delivery, user)
        target_id (int, optional): ID объекта, на который направлено действие
        metadata (dict, optional): Дополнительные данные в формате JSON
    
    Примеры использования:
        log_activity(user_id, 'login', 'Вход в систему', 'user', user_id)
        log_activity(user_id, 'send_message', 'Отправлено сообщение', 'chat', chat_id, {'message_length': 50})
        log_activity(user_id, 'update_delivery', 'Обновлена доставка', 'delivery', delivery_id)
    """
    conn = get_db_connection()
    try:
        # Вставляем запись о действии в таблицу логов
        conn.execute('''
            INSERT INTO activity_logs (user_id, action_type, action_description, target_type, target_id, metadata, ip_address, user_agent)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            user_id, 
            action_type, 
            action_description, 
            target_type, 
            target_id, 
            # Преобразуем словарь metadata в JSON строку для хранения в БД
            json.dumps(metadata) if metadata else None,
            # Получаем IP адрес клиента из запроса
            request.remote_addr,
            # Получаем информацию о браузере пользователя
            request.headers.get('User-Agent')
        ))
        conn.commit()
    except Exception as e:
        # Если не удалось записать лог, логируем ошибку, но не прерываем выполнение
        app.logger.error(f'Error logging activity: {str(e)}')
    finally:
        # Соединение глобальное, не закрываем
        pass


def get_system_stats():
    """
    Получение общей статистики системы с данными из Avito API
    
    Собирает основные метрики системы для отображения на дашбордах:
    - Общее количество чатов (из БД и Avito API)
    - Активные чаты (со статусом 'active')
    - Срочные чаты (с приоритетом 'urgent')
    - Статистика из Avito API (новые сообщения, ответы и т.д.)
    - Общее количество пользователей
    - Количество менеджеров
    - Количество магазинов
    
    Returns:
        dict: Словарь со статистикой
    """
    from avito_api import AvitoAPI
    from datetime import datetime, timedelta, timezone
    
    conn = get_db_connection()

    # Считаем общее количество чатов в системе
    total_chats = conn.execute('SELECT COUNT(*) as count FROM avito_chats').fetchone()['count']

    # Считаем активные чаты (не завершенные, требующие внимания)
    active_chats = conn.execute('SELECT COUNT(*) as count FROM avito_chats WHERE status = "active"').fetchone()['count']

    # Считаем срочные чаты (требующие немедленного ответа)
    urgent_chats = conn.execute('SELECT COUNT(*) as count FROM avito_chats WHERE priority = "urgent"').fetchone()['count']
    
    # Считаем чаты с непрочитанными сообщениями
    unread_chats = conn.execute('SELECT COUNT(*) as count FROM avito_chats WHERE unread_count > 0').fetchone()['count']
    
    # Считаем чаты в пуле (не назначенные менеджерам)
    pool_chats = conn.execute('SELECT COUNT(*) as count FROM avito_chats WHERE assigned_manager_id IS NULL AND status != "completed"').fetchone()['count']
    
    # Среднее время ответа
    avg_response_time = conn.execute('SELECT AVG(response_timer) as avg FROM avito_chats WHERE response_timer IS NOT NULL').fetchone()['avg'] or 0

    # Считаем общее количество пользователей (админы + менеджеры)
    total_users = conn.execute('SELECT COUNT(*) as count FROM users').fetchone()['count']
    
    # Считаем только менеджеров (исключая администраторов)
    total_managers = conn.execute('SELECT COUNT(*) as count FROM users WHERE role = "manager"').fetchone()['count']

    # Считаем количество магазинов Авито, подключенных к системе
    total_shops = conn.execute('SELECT COUNT(*) as count FROM avito_shops').fetchone()['count']
    
    # Считаем магазины с настроенными ключами
    shops_with_keys = conn.execute('''
        SELECT COUNT(*) as count FROM avito_shops 
        WHERE client_id IS NOT NULL AND client_secret IS NOT NULL AND user_id IS NOT NULL
    ''').fetchone()['count']
    
    # Статистика из Avito API
    avito_stats = {
        'total_chats_avito': 0,
        'active_chats_avito': 0,
        'unread_messages_avito': 0,
        'shops_synced': 0,
        'last_sync_time': None
    }
    
    try:
        # Получаем магазины с ключами для получения статистики из Avito API
        shops = conn.execute('''
            SELECT id, name, client_id, client_secret, user_id 
            FROM avito_shops 
            WHERE client_id IS NOT NULL AND client_secret IS NOT NULL AND user_id IS NOT NULL
        ''').fetchall()
        
        total_avito_chats = 0
        total_unread = 0
        synced_shops = 0
        active_avito_chats = 0
        blocked_avito_chats = 0
        archived_avito_chats = 0
        total_views = 0
        total_contacts = 0
        total_favorites = 0
        total_items = 0
        
        # Получаем даты для статистики (последние 30 дней)
        date_to = datetime.now().strftime('%Y-%m-%d')
        date_from = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        
        for shop in shops:
            try:
                api = AvitoAPI(shop['client_id'], shop['client_secret'], shop_id=str(shop['id']))
                
                # Получаем чаты из Avito API
                response = api.get_chats(user_id=str(shop['user_id']), limit=100, offset=0)
                
                if isinstance(response, dict):
                    chats_data = response.get('chats', []) or response.get('data', {}).get('chats', [])
                elif isinstance(response, list):
                    chats_data = response
                else:
                    chats_data = []
                
                if chats_data:
                    total_avito_chats += len(chats_data)
                    # Считаем непрочитанные сообщения и активные чаты
                    for chat in chats_data:
                        unread = chat.get('unread_count', 0) or chat.get('unreadCount', 0)
                        total_unread += unread
                        
                        # Активный чат - не заблокирован и не архивирован
                        if not chat.get('is_blocked', False) and not chat.get('is_archived', False):
                            active_avito_chats += 1
                        elif chat.get('is_blocked', False):
                            blocked_avito_chats += 1
                        elif chat.get('is_archived', False):
                            archived_avito_chats += 1
                    
                    synced_shops += 1
                
                # Получаем статистику аккаунта из Avito API
                try:
                    account_stats = api.get_account_statistics(
                        user_id=str(shop['user_id']),
                        date_from=date_from,
                        date_to=date_to
                    )
                    
                    # Обрабатываем статистику аккаунта
                    if isinstance(account_stats, dict):
                        # Статистика может быть в разных форматах
                        stats_data = account_stats.get('result', account_stats.get('data', account_stats))
                        
                        if isinstance(stats_data, dict):
                            # Суммируем просмотры, контакты, избранное
                            views = stats_data.get('views', 0) or stats_data.get('total_views', 0)
                            contacts = stats_data.get('contacts', 0) or stats_data.get('total_contacts', 0)
                            favorites = stats_data.get('favorites', 0) or stats_data.get('total_favorites', 0)
                            
                            if isinstance(views, (int, float)):
                                total_views += views
                            if isinstance(contacts, (int, float)):
                                total_contacts += contacts
                            if isinstance(favorites, (int, float)):
                                total_favorites += favorites
                        
                        # Если статистика в виде массива (по дням/неделям)
                        elif isinstance(stats_data, list):
                            for day_stat in stats_data:
                                if isinstance(day_stat, dict):
                                    views = day_stat.get('views', 0) or day_stat.get('total_views', 0)
                                    contacts = day_stat.get('contacts', 0) or day_stat.get('total_contacts', 0)
                                    favorites = day_stat.get('favorites', 0) or day_stat.get('total_favorites', 0)
                                    
                                    if isinstance(views, (int, float)):
                                        total_views += views
                                    if isinstance(contacts, (int, float)):
                                        total_contacts += contacts
                                    if isinstance(favorites, (int, float)):
                                        total_favorites += favorites
                except Exception as stats_err:
                    app.logger.debug(f'Не удалось получить статистику аккаунта для магазина {shop["id"]}: {stats_err}')
                    
            except Exception as e:
                app.logger.warning(f'Ошибка получения статистики из Avito API для магазина {shop["id"]}: {e}')
                continue
        
        avito_stats = {
            'total_chats_avito': total_avito_chats,
            'active_chats_avito': active_avito_chats,
            'blocked_chats_avito': blocked_avito_chats,
            'archived_chats_avito': archived_avito_chats,
            'unread_messages_avito': total_unread,
            'shops_synced': synced_shops,
            'last_sync_time': datetime.now().isoformat(),
            'account_stats': {
                'total_views_30d': total_views,
                'total_contacts_30d': total_contacts,
                'total_favorites_30d': total_favorites,
                'period': f'{date_from} - {date_to}'
            }
        }
    except Exception as e:
        app.logger.warning(f'Ошибка получения статистики из Avito API: {e}')

            # Соединение глобальное, не закрываем

    # Возвращаем словарь со всей статистикой
    return {
        'total_chats': total_chats,
        'active_chats': active_chats,
        'urgent_chats': urgent_chats,
        'unread_chats': unread_chats,
        'pool_chats': pool_chats,
        'avg_response_time': round(avg_response_time, 2),
        'total_users': total_users,
        'total_managers': total_managers,
        'total_shops': total_shops,
        'shops_with_keys': shops_with_keys,
        'avito_stats': avito_stats
    }


# Главная страница - редирект на логин
@app.route('/')
def home():
    if 'user_id' in session:
        user = get_user_by_id(session['user_id'])
        if user['role'] == 'admin':
            return redirect('/admin/dashboard')
        else:
            return redirect('/manager/dashboard')
    return redirect('/login')


# Страница смены пароля (для новых пользователей)
@app.route('/change-password', methods=['GET', 'POST'])
@require_auth
@handle_errors
def change_password():
    user = get_user_by_id(session['user_id'])
    if not user:
        return redirect('/login')
    
    # Проверяем, это первый вход (нужно ли требовать текущий пароль)
    # password_changed может быть None, False или True
    # None или False означает, что пароль еще не был изменен (первый вход)
    password_changed = user.get('password_changed')
    is_first_login = not (password_changed is True)
    
    if request.method == 'POST':
        current_password = request.form.get('current_password', '').strip()
        new_password = request.form.get('new_password', '').strip()
        confirm_password = request.form.get('confirm_password', '').strip()

        if not new_password or not confirm_password:
            return render_template('change_password.html', error='Заполните все поля', hide_header=True, is_first_login=is_first_login)

        if new_password != confirm_password:
            return render_template('change_password.html', error='Пароли не совпадают', hide_header=True, is_first_login=is_first_login)

        if len(new_password) < 6:
            return render_template('change_password.html', error='Пароль должен быть не менее 6 символов', hide_header=True, is_first_login=is_first_login)

        # Проверяем текущий пароль только если это не первый вход
        if not is_first_login:
            from auth import authenticate_user
            auth_result = authenticate_user(user['email'], current_password)
            if not auth_result:
                return render_template('change_password.html', error='Текущий пароль неверен', hide_header=True, is_first_login=is_first_login)

        # Обновляем пароль
        from auth import update_user_password
        if update_user_password(session['user_id'], new_password):
            # Логируем изменение пароля
            log_activity(session['user_id'], 'change_password',
                        'Пользователь изменил пароль при первом входе' if is_first_login else 'Пользователь изменил пароль', 'user', session['user_id'])

            flash('Пароль успешно изменен!')
            # Перенаправляем на соответствующую панель
            if user['role'] == 'super_admin' or user['role'] == 'admin':
                return redirect('/admin/dashboard')
            else:
                return redirect('/manager/dashboard')
        else:
            return render_template('change_password.html', error='Ошибка изменения пароля', hide_header=True, is_first_login=is_first_login)

    return render_template('change_password.html', hide_header=True, is_first_login=is_first_login)

# Страница входа
@app.route('/login', methods=['GET', 'POST'])
@handle_errors
def login():
    if request.method == 'POST':
        email = request.form.get('email', '').strip()
        password = request.form.get('password', '')

        # Валидация
        if not email or not password:
            return render_template('login.html', error='Заполните все поля', hide_header=True)
        
        if not validate_email(email):
            return render_template('login.html', error='Неверный формат email', hide_header=True)

        user = authenticate_user(email, password)
        if user:
            session['user_id'] = user['id']
            session['user_role'] = user['role']
            session['login_time'] = datetime.now().isoformat()
            session.permanent = True  # Делаем сессию постоянной (живет 7 дней согласно PERMANENT_SESSION_LIFETIME)
            
            # Логируем создание сессии для диагностики
            app.logger.info(f"[LOGIN] Сессия создана для пользователя {user['id']} ({user.get('username', 'unknown')})")
            app.logger.info(f"[LOGIN] Session keys после логина: {list(session.keys())}")
            app.logger.info(f"[LOGIN] Session permanent: {session.permanent}")
            
            # Принудительно помечаем сессию как измененную
            session.modified = True

            # Проверяем, нужно ли менять пароль (первый вход или одноразовый пароль)
            temp_password_used = user.get('temp_password_used', False)
            password_changed = user.get('password_changed', True)

            if temp_password_used or not password_changed:
                # Перенаправляем на страницу смены пароля
                return redirect('/change-password')

            # Логируем вход
            log_activity(user['id'], 'login', f'Вход в систему', 'user', user['id'])

            if user['role'] == 'super_admin':
                return redirect('/admin/dashboard')
            elif user['role'] == 'admin':
                return redirect('/admin/dashboard')
            else:
                return redirect('/manager/dashboard')
        else:
            return render_template('login.html', error='Неверный email или пароль', hide_header=True)

    return render_template('login.html', hide_header=True)


# Выход
@app.route('/logout')
def logout():
    session.clear()
    return redirect('/login')


# Админ панель
@app.route('/admin/dashboard')
@require_auth
@require_role('admin')
@handle_errors
def admin_dashboard():
    user = get_user_by_id(session['user_id'])
    stats = get_system_stats()
    return render_template('admin_dashboard.html', user=user, stats=stats)


@app.route('/admin/webhooks')
@require_auth
@require_role('super_admin')
@handle_errors
def webhooks_page():
    """Страница управления webhooks для супер-админа"""
    user = get_user_by_id(session['user_id'])
    return render_template('webhooks.html', user=user)


# API для получения информации о webhook
@app.route('/api/admin/webhooks', methods=['GET'])
@require_auth
@require_role('super_admin')
@handle_errors
def get_webhook_info():
    """Получение информации о текущем webhook v3"""
    try:
        # Получаем первый магазин для получения credentials
        conn = get_db_connection()
        shop = conn.execute('''
            SELECT client_id, client_secret, user_id
            FROM avito_shops
            WHERE client_id IS NOT NULL AND client_secret IS NOT NULL
            LIMIT 1
        ''').fetchone()
        
        if not shop:
            return jsonify({
                'webhook': None,
                'error': 'No shops with API credentials found'
            }), 404
        
        shop = dict(shop) if not isinstance(shop, dict) else shop
        
        # Получаем информацию о webhook через Avito API
        from avito_api import AvitoAPI
        api = AvitoAPI(shop['client_id'], shop['client_secret'])
        
        webhook = api.get_webhook_v3()
        
        return jsonify({
            'webhook': webhook if webhook else None
        }), 200
        
    except Exception as e:
        app.logger.error(f"Ошибка получения информации о webhook: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'webhook': None,
            'error': str(e)
        }), 500
    finally:
        pass  # Соединение глобальное, не закрываем


# API для регистрации webhook
@app.route('/api/admin/webhooks', methods=['POST'])
@require_auth
@require_role('super_admin')
@handle_errors
def register_webhook():
    """Регистрация нового webhook v3"""
    data = request.get_json()
    
    if not data:
        return jsonify({'error': 'No data provided'}), 400
    
    url = data.get('url', '').strip()
    types = data.get('types', ['message', 'chat'])
    
    if not url:
        return jsonify({'error': 'URL is required'}), 400
    
    if not url.startswith('https://'):
        return jsonify({'error': 'URL must start with https://'}), 400
    
    if not isinstance(types, list) or len(types) == 0:
        return jsonify({'error': 'Types must be a non-empty list'}), 400
    
    # Валидация типов
    valid_types = ['message', 'chat', 'user']
    for t in types:
        if t not in valid_types:
            return jsonify({'error': f'Invalid type: {t}. Valid types: {valid_types}'}), 400
    
    try:
        # Получаем первый магазин для получения credentials
        conn = get_db_connection()
        shop = conn.execute('''
            SELECT client_id, client_secret, user_id
            FROM avito_shops
            WHERE client_id IS NOT NULL AND client_secret IS NOT NULL
            LIMIT 1
        ''').fetchone()
        
        if not shop:
            return jsonify({'error': 'No shops with API credentials found'}), 404
        
        shop = dict(shop) if not isinstance(shop, dict) else shop
        
        # Регистрируем webhook через Avito API
        from avito_api import AvitoAPI
        api = AvitoAPI(shop['client_id'], shop['client_secret'])
        
        result = api.register_webhook_v3(url=url, types=types)
        
        # Логируем действие
        log_activity(session['user_id'], 'register_webhook', 
                    f'Зарегистрирован webhook: {url}', 'system')
        
        return jsonify({
            'success': True,
            'webhook': result
        }), 201
        
    except Exception as e:
        app.logger.error(f"Ошибка регистрации webhook: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500
    finally:
        pass  # Соединение глобальное, не закрываем


# API для обновления webhook
@app.route('/api/admin/webhooks', methods=['PUT'])
@require_auth
@require_role('super_admin')
@handle_errors
def update_webhook():
    """Обновление webhook v3"""
    data = request.get_json()
    
    if not data:
        return jsonify({'error': 'No data provided'}), 400
    
    url = data.get('url', '').strip()
    types = data.get('types', ['message', 'chat'])
    
    if not url:
        return jsonify({'error': 'URL is required'}), 400
    
    if not url.startswith('https://'):
        return jsonify({'error': 'URL must start with https://'}), 400
    
    if not isinstance(types, list) or len(types) == 0:
        return jsonify({'error': 'Types must be a non-empty list'}), 400
    
    # Валидация типов
    valid_types = ['message', 'chat', 'user']
    for t in types:
        if t not in valid_types:
            return jsonify({'error': f'Invalid type: {t}. Valid types: {valid_types}'}), 400
    
    try:
        # Получаем первый магазин для получения credentials
        conn = get_db_connection()
        shop = conn.execute('''
            SELECT client_id, client_secret, user_id
            FROM avito_shops
            WHERE client_id IS NOT NULL AND client_secret IS NOT NULL
            LIMIT 1
        ''').fetchone()
        
        if not shop:
            return jsonify({'error': 'No shops with API credentials found'}), 404
        
        shop = dict(shop) if not isinstance(shop, dict) else shop
        
        # Обновляем webhook через Avito API
        from avito_api import AvitoAPI
        api = AvitoAPI(shop['client_id'], shop['client_secret'])
        
        result = api.update_webhook_v3(url=url, types=types)
        
        # Логируем действие
        log_activity(session['user_id'], 'update_webhook', 
                    f'Обновлен webhook: {url}', 'system')
        
        return jsonify({
            'success': True,
            'webhook': result
        }), 200
        
    except Exception as e:
        app.logger.error(f"Ошибка обновления webhook: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500
    finally:
        pass  # Соединение глобальное, не закрываем


# API для удаления webhook
@app.route('/api/admin/webhooks', methods=['DELETE'])
@require_auth
@require_role('super_admin')
@handle_errors
def delete_webhook():
    """Удаление webhook v3"""
    try:
        # Получаем первый магазин для получения credentials
        conn = get_db_connection()
        shop = conn.execute('''
            SELECT client_id, client_secret, user_id
            FROM avito_shops
            WHERE client_id IS NOT NULL AND client_secret IS NOT NULL
            LIMIT 1
        ''').fetchone()
        
        if not shop:
            return jsonify({'error': 'No shops with API credentials found'}), 404
        
        shop = dict(shop) if not isinstance(shop, dict) else shop
        
        # Удаляем webhook через Avito API
        from avito_api import AvitoAPI
        api = AvitoAPI(shop['client_id'], shop['client_secret'])
        
        success = api.delete_webhook_v3()
        
        if success:
            # Логируем действие
            log_activity(session['user_id'], 'delete_webhook', 
                        'Удален webhook', 'system')
            
            return jsonify({
                'success': True,
                'message': 'Webhook deleted successfully'
            }), 200
        else:
            return jsonify({'error': 'Failed to delete webhook'}), 500
        
    except Exception as e:
        app.logger.error(f"Ошибка удаления webhook: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500
    finally:
        pass  # Соединение глобальное, не закрываем


# Панель менеджера
@app.route('/manager/dashboard')
@require_auth
@require_role('manager')
@handle_errors
def manager_dashboard():
    user = get_user_by_id(session['user_id'])
    stats = get_system_stats()
    return render_template('manager_dashboard.html', user=user, stats=stats)


# API для получения данных пользователя
@app.route('/api/user')
@require_auth
@handle_errors
def get_current_user():
    user = get_user_by_id(session['user_id'])
    if not user:
        return jsonify({'error': 'User not found'}), 404
    return jsonify(user)


# API для получения статистики
@app.route('/api/stats')
def get_stats():
    if 'user_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401

    stats = get_system_stats()
    return jsonify(stats)


# API для получения списка пользователей (только для админа)
@app.route('/api/users')
@require_auth
@handle_errors
def get_users():
    # Разрешаем доступ для admin и super_admin
    user_role = session.get('user_role')
    if user_role not in ['admin', 'super_admin']:
        return jsonify({'error': 'Access denied'}), 403

    conn = get_db_connection()
    users = conn.execute('''
        SELECT id, username, email, role, is_active, kpi_score, created_at 
        FROM users 
        ORDER BY created_at DESC
    ''').fetchall()
            # Соединение глобальное, не закрываем

    users_list = [dict(user) for user in users]
    return jsonify(users_list)


# API для получения настроек видимости вкладок пользователя
@app.route('/api/users/<int:user_id>/tab-visibility')
@require_auth
@handle_errors
def get_user_tab_visibility(user_id):
    # Только super_admin может управлять настройками видимости вкладок
    user_role = session.get('user_role')
    if user_role != 'super_admin':
        return jsonify({'error': 'Access denied. Требуется роль super_admin'}), 403
    
    conn = get_db_connection()
    try:
        user_settings = conn.execute('''
            SELECT tab_visibility FROM user_settings WHERE user_id = ?
        ''', (user_id,)).fetchone()
        
        tab_visibility = None
        if user_settings and user_settings['tab_visibility']:
            import json
            tab_visibility = json.loads(user_settings['tab_visibility'])
        
        # Соединение глобальное, не закрываем
        return jsonify({'tab_visibility': tab_visibility})
    except Exception as e:
        # Соединение глобальное, не закрываем
        app.logger.error(f"[TAB VISIBILITY] Ошибка получения настроек: {e}")
        return jsonify({'error': str(e)}), 400


# API для сохранения настроек видимости вкладок пользователя
@app.route('/api/users/<int:user_id>/tab-visibility', methods=['PUT'])
@require_auth
@handle_errors
def save_user_tab_visibility(user_id):
    # Только super_admin может управлять настройками видимости вкладок
    user_role = session.get('user_role')
    if user_role != 'super_admin':
        return jsonify({'error': 'Access denied. Требуется роль super_admin'}), 403
    
    data = request.get_json() or {}
    if 'tab_visibility' not in data:
        return jsonify({'error': 'tab_visibility is required'}), 400
    
    conn = get_db_connection()
    try:
        import json
        tab_visibility_json = json.dumps(data['tab_visibility'])
        
        # Проверяем, существует ли запись user_settings для этого пользователя
        existing = conn.execute('SELECT id FROM user_settings WHERE user_id = ?', (user_id,)).fetchone()
        
        if existing:
            conn.execute('''
                UPDATE user_settings SET tab_visibility = ? WHERE user_id = ?
            ''', (tab_visibility_json, user_id))
        else:
            conn.execute('''
                INSERT INTO user_settings (user_id, tab_visibility) VALUES (?, ?)
            ''', (user_id, tab_visibility_json))
        
        conn.commit()
        
        # Логируем действие
        user_info = conn.execute('SELECT username, role FROM users WHERE id = ?', (user_id,)).fetchone()
        if user_info:
            log_activity(session['user_id'], 'update_tab_visibility', 
                        f'Обновлена видимость вкладок для пользователя: {user_info["username"]} ({user_info["role"]})', 'user', user_id)
        
        # Соединение глобальное, не закрываем
        return jsonify({'success': True, 'message': 'Настройки видимости вкладок сохранены'})
    except Exception as e:
        # Соединение глобальное, не закрываем
        app.logger.error(f"[TAB VISIBILITY] Ошибка сохранения настроек: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 400


# API для получения магазинов
@app.route('/api/shops')
def get_shops():
    if 'user_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401

    conn = get_db_connection()
    user_role = session.get('user_role')

    if user_role in ['admin', 'super_admin']:
        # Админ и супер-админ видят все магазины
        shops = conn.execute('''
            SELECT 
                *, 
                CASE 
                    WHEN client_id IS NOT NULL AND client_secret IS NOT NULL AND user_id IS NOT NULL 
                    THEN 'ok' ELSE 'missing' 
                END AS avito_status
            FROM avito_shops 
            ORDER BY created_at DESC
        ''').fetchall()
        app.logger.info(f'[GET SHOPS] Загружено магазинов для {user_role}: {len(shops)}')
    else:
        # Менеджер видит только назначенные магазины
        shops = conn.execute('''
            SELECT 
                s.*,
                CASE 
                    WHEN s.client_id IS NOT NULL AND s.client_secret IS NOT NULL AND s.user_id IS NOT NULL 
                    THEN 'ok' ELSE 'missing' 
                END AS avito_status
            FROM avito_shops s
            JOIN manager_assignments ma ON s.id = ma.shop_id
            WHERE ma.manager_id = ? AND s.is_active = 1
            ORDER BY s.created_at DESC
        ''', (session['user_id'],)).fetchall()

            # Соединение глобальное, не закрываем

    shops_list = []
    user_role = session.get('user_role')
    for shop in shops:
        d = dict(shop)
        # Скрываем ключи только для менеджеров, админы и super_admin видят все
        if user_role not in ['admin', 'super_admin']:
            d.pop('client_id', None)
            d.pop('client_secret', None)
            d.pop('user_id', None)
        shops_list.append(d)
    return jsonify(shops_list)


# API для получения чатов
def sync_chats_from_avito(shop_id: Optional[int] = None) -> Dict[str, Any]:
    # Логируем путь к базе данных для диагностики
    import os
    db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'osagaming_crm.db')
    app.logger.info(f"[SYNC] Используется база данных: {db_path}")
    app.logger.info(f"[SYNC] База данных существует: {os.path.exists(db_path)}")
    """
    Синхронизация чатов из Avito API для всех магазинов или конкретного магазина
    
    Args:
        shop_id: ID магазина (если None, синхронизирует все магазины с ключами)
    
    Returns:
        Dict с результатами синхронизации
    """
    from avito_api import AvitoAPI
    
    conn = get_db_connection()
    synced_count = 0
    errors = []
    
    try:
        # Получаем магазины с настроенными ключами
        if shop_id:
            shops = conn.execute('''
                SELECT id, name, client_id, client_secret, user_id, is_active
                FROM avito_shops 
                WHERE id = ? AND is_active = 1 AND client_id IS NOT NULL AND client_secret IS NOT NULL AND user_id IS NOT NULL
            ''', (shop_id,)).fetchall()
        else:
            shops = conn.execute('''
                SELECT id, name, client_id, client_secret, user_id, is_active
                FROM avito_shops 
                WHERE is_active = 1 AND client_id IS NOT NULL AND client_secret IS NOT NULL AND user_id IS NOT NULL
            ''').fetchall()
        
        app.logger.info(f"[SYNC] Найдено магазинов для синхронизации: {len(shops)}")
        if shops:
            shop_names = [shop['name'] for shop in shops]
            app.logger.info(f"[SYNC] Магазины: {', '.join(shop_names)}")
        
        for idx, shop in enumerate(shops):
            # Конвертируем sqlite3.Row в dict для безопасного доступа
            shop = dict(shop)
            app.logger.info(f"[SYNC] ========== Начало синхронизации магазина {shop['id']}: {shop['name']} ==========")
            
            # Добавляем задержку между запросами к разным магазинам
            # чтобы избежать rate limiting от Avito API
            if idx > 0:
                import time
                delay = 2  # 2 секунды задержки между магазинами
                app.logger.info(f"[SYNC] Задержка {delay} сек перед синхронизацией магазина {shop['id']}...")
                time.sleep(delay)
            
            try:
                api = AvitoAPI(shop['client_id'], shop['client_secret'], shop_id=str(shop['id']))
                
                # Получаем чаты из Avito API
                offset = 0
                limit = 100
                total_synced = 0
                
                while True:
                    try:
                        response = api.get_chats(user_id=str(shop['user_id']), limit=limit, offset=offset)
                        
                        # Логируем структуру ответа для отладки
                        app.logger.info(f"[SYNC] Ответ от Avito API для магазина {shop['id']}: тип={type(response)}")
                        if isinstance(response, dict):
                            app.logger.info(f"[SYNC] Ключи в ответе: {list(response.keys())}")
                        
                        # Avito API возвращает данные в разных форматах
                        # Проверяем разные варианты структуры ответа
                        if isinstance(response, dict):
                            # Пробуем разные варианты ключей
                            chats_data = response.get('chats', [])
                            if not chats_data:
                                chats_data = response.get('items', [])
                            if not chats_data and 'data' in response:
                                data = response['data']
                                if isinstance(data, dict):
                                    chats_data = data.get('chats', []) or data.get('items', [])
                                elif isinstance(data, list):
                                    chats_data = data
                        elif isinstance(response, list):
                            chats_data = response
                        else:
                            chats_data = []
                        
                        chats_count = len(chats_data) if isinstance(chats_data, list) else 0
                        app.logger.info(f"[SYNC] Извлечено чатов: {chats_count}")
                        
                        # Проверяем метаданные для пагинации
                        if isinstance(response, dict) and 'meta' in response:
                            meta = response['meta']
                            total = meta.get('total', meta.get('count', 0))
                            has_more = meta.get('has_more', False)
                            app.logger.info(f"[SYNC] Метаданные: total={total}, has_more={has_more}, offset={offset}, limit={limit}")
                            if total > 0:
                                app.logger.info(f"[SYNC] Всего чатов в Avito: {total}, будет обработано страниц: {(total + limit - 1) // limit}")
                            elif has_more:
                                # Если has_more=True, но total=0, значит нужно продолжать пагинацию
                                app.logger.info(f"[SYNC] has_more=True, продолжаем пагинацию (total может быть не указан)")
                        
                        if not chats_data or chats_count == 0:
                            app.logger.info(f"[SYNC] Нет чатов для обработки, завершаем пагинацию")
                            break
                        
                        chats_processed = 0
                        chats_created = 0
                        chats_updated = 0
                        chats_errors = 0
                        
                        app.logger.info(f"[SYNC] Начинаем обработку {chats_count} чатов...")
                        
                        for idx, chat_data in enumerate(chats_data):
                            try:
                                # Извлекаем данные чата
                                avito_chat_id = chat_data.get('id')
                                if not avito_chat_id:
                                    if idx < 3:  # Логируем только первые 3 для отладки
                                        app.logger.warning(f"[SYNC] Чат {idx}: нет ID, пропускаем. Данные: {list(chat_data.keys())}")
                                    continue
                                
                                # Преобразуем chat_id в строку для сравнения
                                avito_chat_id_str = str(avito_chat_id)
                                
                                # Проверяем, существует ли чат
                                # Проверяем и по shop_id+chat_id, и просто по chat_id (на случай если shop_id изменился)
                                existing = conn.execute(
                                    'SELECT id, shop_id, chat_id FROM avito_chats WHERE shop_id = ? AND chat_id = ?',
                                    (shop['id'], avito_chat_id_str)
                                ).fetchone()
                                # Конвертируем Row в dict для безопасного доступа
                                if existing:
                                    existing = dict(existing)
                                
                                
                                # Если не нашли по shop_id+chat_id, проверяем только по chat_id
                                if not existing:
                                    existing_by_chat_id = conn.execute(
                                        'SELECT id, shop_id, chat_id FROM avito_chats WHERE chat_id = ?',
                                        (avito_chat_id_str,)
                                    ).fetchone()
                                    if existing_by_chat_id:
                                        existing_by_chat_id = dict(existing_by_chat_id)
                                        app.logger.warning(f"[SYNC] Чат {avito_chat_id_str} найден с другим shop_id: БД={existing_by_chat_id.get('shop_id')}, текущий={shop['id']}")
                                        # Обновляем shop_id если он изменился
                                        conn.execute(
                                            'UPDATE avito_chats SET shop_id = ? WHERE id = ?',
                                            (shop['id'], existing_by_chat_id.get('id'))
                                        )
                                        existing = existing_by_chat_id
                                    else:
                                        app.logger.info(f"[SYNC] Чат {idx}: НЕ найден в БД - будет создан новый чат")
                                
                                # Получаем информацию о пользователе
                                # Avito API может возвращать users как массив или объект
                                users_data = chat_data.get('users', [])
                                if isinstance(users_data, list) and len(users_data) > 0:
                                    user_info = users_data[0] if isinstance(users_data[0], dict) else {}
                                elif isinstance(users_data, dict):
                                    user_info = users_data
                                else:
                                    user_info = {}
                                
                                client_name = user_info.get('name') or user_info.get('profile', {}).get('name', 'Неизвестно')
                                client_phone = user_info.get('phone') or user_info.get('profile', {}).get('phone', '')
                                customer_id = user_info.get('id') or user_info.get('profile', {}).get('id', '')
                                
                                # Получаем последнее сообщение
                                last_message_data = chat_data.get('last_message', {})
                                if isinstance(last_message_data, dict):
                                    # Проверяем разные форматы структуры сообщения
                                    if 'content' in last_message_data:
                                        content = last_message_data['content']
                                        if isinstance(content, dict):
                                            last_message = content.get('text', '') or content.get('message', '')
                                        else:
                                            last_message = str(content)
                                    elif 'text' in last_message_data:
                                        last_message = last_message_data['text']
                                    elif 'message' in last_message_data:
                                        last_message = last_message_data['message']
                                    else:
                                        last_message = str(last_message_data)
                                else:
                                    last_message = ''
                                
                                # Получаем информацию об объявлении
                                # ВАЖНО: Avito API возвращает context.value, а не context.item!
                                # Структура: {"context": {"type": "item", "value": {"id": 123, "url": "..."}}}
                                product_url = None
                                
                                # ВАЖНО: Avito API v3 возвращает context.value, а не context.item!
                                # Структура: {"context": {"type": "item", "value": {"id": 123, "url": "..."}}}
                                context = chat_data.get('context', {})
                                if isinstance(context, dict):
                                    # Приоритет: context.value (API v3), затем context.item (старая версия)
                                    item_data = (context.get('value') or 
                                               context.get('item') or 
                                               context.get('listing') or 
                                               context.get('ad', {}))
                                else:
                                    # Fallback на прямые поля в chat_data
                                    item_data = chat_data.get('item', chat_data.get('listing', chat_data.get('ad', {})))
                                
                                # Логируем структуру для первых чатов и чатов без product_url
                                should_log = idx < 3
                                if not should_log and not product_url:
                                    # Логируем чаты без product_url для диагностики
                                    should_log = True
                                
                                if should_log:
                                    app.logger.info(f"[SYNC] Чат {idx} (chat_id={avito_chat_id_str}): проверяем наличие context/item/listing/ad в chat_data, ключи: {list(chat_data.keys())}")
                                    if 'context' in chat_data:
                                        app.logger.info(f"[SYNC] Чат {idx}: context тип={type(chat_data.get('context'))}, ключи: {list(chat_data.get('context', {}).keys()) if isinstance(chat_data.get('context'), dict) else 'не dict'}")
                                        if isinstance(chat_data.get('context'), dict):
                                            app.logger.info(f"[SYNC] Чат {idx}: context содержимое: {str(chat_data.get('context'))[:500]}")
                                    if item_data:
                                        app.logger.info(f"[SYNC] Чат {idx}: item_data тип={type(item_data)}, значение={str(item_data)[:500]}")
                                    else:
                                        app.logger.warning(f"[SYNC] Чат {idx}: item_data отсутствует! Все ключи chat_data: {list(chat_data.keys())}")
                                
                                # Сохраняем данные объявления из context.value в listing_data
                                listing_data_json = None
                                if isinstance(item_data, dict) and item_data:
                                    # Сохраняем все данные из item_data в listing_data
                                    import json
                                    listing_data_json = json.dumps(item_data, ensure_ascii=False)
                                    app.logger.info(f"[SYNC] Чат {idx}: Сохраняем listing_data из context.value (ключи: {list(item_data.keys())[:10]})")
                                    
                                    # Пробуем разные варианты ключей для URL
                                    # Согласно документации, item может содержать: id, url, или другие поля
                                    item_id = item_data.get('id')
                                    product_url = (item_data.get('url') or 
                                                 item_data.get('link') or 
                                                 item_data.get('href') or
                                                 item_data.get('value') or
                                                 item_data.get('uri'))
                                    
                                    # Если URL не найден, но есть ID, формируем URL из ID
                                    if not product_url and item_id:
                                        item_id_str = str(item_id)
                                        # Формируем URL на основе ID объявления
                                        shop_url_part = shop.get('shop_url', '').split('/')[-1] if shop.get('shop_url') else ''
                                        if shop_url_part:
                                            product_url = f"https://www.avito.ru/{shop_url_part}/items/{item_id_str}"
                                        else:
                                            product_url = f"https://www.avito.ru/items/{item_id_str}"
                                    
                                    # Если URL относительный, делаем его абсолютным
                                    if product_url and isinstance(product_url, str):
                                        if product_url.startswith('/'):
                                            product_url = f"https://www.avito.ru{product_url}"
                                        elif not product_url.startswith('http'):
                                            # Если это ID объявления, формируем URL
                                            shop_url_part = shop.get('shop_url', '').split('/')[-1] if shop.get('shop_url') else ''
                                            if shop_url_part:
                                                product_url = f"https://www.avito.ru/{shop_url_part}/items/{product_url}"
                                            else:
                                                product_url = f"https://www.avito.ru/items/{product_url}"
                                elif isinstance(item_data, str):
                                    # Если item_data - это просто строка (ID или URL)
                                    if item_data.startswith('http'):
                                        product_url = item_data
                                    elif item_data.isdigit():
                                        # Если это ID объявления, формируем URL
                                        shop_url_part = shop.get('shop_url', '').split('/')[-1] if shop.get('shop_url') else ''
                                        if shop_url_part:
                                            product_url = f"https://www.avito.ru/{shop_url_part}/items/{item_data}"
                                        else:
                                            product_url = f"https://www.avito.ru/items/{item_data}"
                                
                                # Также проверяем прямые поля в chat_data (для обратной совместимости)
                                if not product_url:
                                    product_url = (chat_data.get('item_url') or 
                                                 chat_data.get('listing_url') or 
                                                 chat_data.get('ad_url') or
                                                 chat_data.get('product_url'))
                                
                                # Если product_url все еще не найден, пытаемся получить через get_chat_by_id
                                if not product_url and shop.get('client_id') and shop.get('client_secret') and shop.get('user_id'):
                                    try:
                                        from avito_api import AvitoAPI
                                        api = AvitoAPI(
                                            client_id=shop['client_id'],
                                            client_secret=shop['client_secret']
                                        )
                                        # Получаем детальную информацию о чате
                                        chat_details = api.get_chat_by_id(
                                            user_id=shop['user_id'],
                                            chat_id=avito_chat_id_str
                                        )
                                        if isinstance(chat_details, dict):
                                            # ВАЖНО: Avito API v3 возвращает context.value, а не context.item!
                                            # Структура: {"context": {"type": "item", "value": {"id": 123, "url": "..."}}}
                                            detail_context = chat_details.get('context', {})
                                            if isinstance(detail_context, dict):
                                                # Приоритет: context.value (API v3), затем context.item (старая версия)
                                                detail_item = (detail_context.get('value') or 
                                                              detail_context.get('item') or 
                                                              detail_context.get('listing') or 
                                                              detail_context.get('ad', {}))
                                                if isinstance(detail_item, dict) and detail_item:
                                                    # Сохраняем данные из detail_item в listing_data
                                                    if not listing_data_json:
                                                        import json
                                                        listing_data_json = json.dumps(detail_item, ensure_ascii=False)
                                                        app.logger.info(f"[SYNC] Чат {idx}: Сохраняем listing_data из get_chat_by_id context.value (ключи: {list(detail_item.keys())[:10]})")
                                                    
                                                    detail_item_id = detail_item.get('id')
                                                    detail_url = (detail_item.get('url') or 
                                                                 detail_item.get('link') or 
                                                                 detail_item.get('href') or
                                                                 detail_item.get('value') or
                                                                 detail_item.get('uri'))
                                                    if detail_url:
                                                        product_url = detail_url
                                                        if product_url.startswith('/'):
                                                            product_url = f"https://www.avito.ru{product_url}"
                                                        elif not product_url.startswith('http'):
                                                            product_url = f"https://www.avito.ru{product_url}"
                                                        app.logger.info(f"[SYNC] ✅ Чат {idx}: product_url найден через get_chat_by_id context.value (url): {product_url}")
                                                    elif detail_item_id:
                                                        item_id_str = str(detail_item_id)
                                                        shop_url_part = shop.get('shop_url', '').split('/')[-1] if shop.get('shop_url') else ''
                                                        if shop_url_part:
                                                            product_url = f"https://www.avito.ru/{shop_url_part}/items/{item_id_str}"
                                                        else:
                                                            product_url = f"https://www.avito.ru/items/{item_id_str}"
                                                        app.logger.info(f"[SYNC] ✅ Чат {idx}: product_url найден через get_chat_by_id context.value (id): {product_url}")
                                            
                                            # Если не нашли в context, проверяем прямые поля
                                            if not product_url:
                                                product_url = (chat_details.get('item_url') or 
                                                             chat_details.get('listing_url') or 
                                                             chat_details.get('ad_url') or
                                                             chat_details.get('product_url'))
                                                if product_url:
                                                    app.logger.info(f"[SYNC] ✅ Чат {idx}: product_url найден через get_chat_by_id (прямые поля): {product_url}")
                                            
                                            if not product_url:
                                                app.logger.warning(f"[SYNC] ⚠️ Чат {idx}: product_url не найден даже через get_chat_by_id. Ключи chat_details: {list(chat_details.keys())}")
                                                if 'context' in chat_details:
                                                    app.logger.warning(f"[SYNC] ⚠️ Чат {idx}: context = {str(chat_details.get('context'))[:500]}")
                                    except Exception as api_error:
                                        app.logger.warning(f"[SYNC] Чат {idx}: ошибка при попытке получить product_url через get_chat_by_id: {api_error}")
                                
                                if product_url:
                                    app.logger.info(f"[SYNC] Чат {idx} (chat_id={avito_chat_id_str}): найден product_url={product_url}")
                                else:
                                    app.logger.warning(f"[SYNC] Чат {idx} (chat_id={avito_chat_id_str}): product_url НЕ найден. Возможно, чат не связан с объявлением или объявление было удалено.")
                                    # Логируем структуру chat_data для диагностики
                                    if idx < 5:  # Логируем первые 5 чатов без product_url
                                        app.logger.warning(f"[SYNC] Чат {idx}: структура chat_data - ключи: {list(chat_data.keys())}")
                                        if 'context' in chat_data:
                                            app.logger.warning(f"[SYNC] Чат {idx}: context = {str(chat_data.get('context'))[:300]}")
                                
                                # Получаем метаданные
                                unread_count = chat_data.get('unread_count', 0) or chat_data.get('unreadCount', 0)
                                is_blocked = chat_data.get('is_blocked', False) or chat_data.get('isBlocked', False)
                                is_archived = chat_data.get('is_archived', False) or chat_data.get('isArchived', False)
                                
                                # Определяем статус
                                status = 'archived' if is_archived else 'active'
                                if is_blocked:
                                    status = 'blocked'
                                
                                # Определяем приоритет на основе времени последнего сообщения
                                priority = 'normal'
                                if last_message_data and isinstance(last_message_data, dict):
                                    last_message_time = last_message_data.get('created') or last_message_data.get('created_at')
                                    if last_message_time:
                                        try:
                                            if isinstance(last_message_time, (int, float)):
                                                msg_time = datetime.fromtimestamp(last_message_time)
                                            else:
                                                msg_time = datetime.fromisoformat(str(last_message_time).replace('Z', '+00:00'))
                                            time_diff = datetime.now() - msg_time
                                            if time_diff.total_seconds() < 3600:  # Меньше часа
                                                priority = 'urgent'
                                            elif time_diff.total_seconds() < 86400:  # Меньше суток
                                                priority = 'new'
                                        except Exception as time_err:
                                            app.logger.warning(f"[SYNC] Ошибка парсинга времени сообщения: {time_err}")
                                            pass
                                
                                if existing:
                                    # Конвертируем existing в dict если это Row
                                    if not isinstance(existing, dict):
                                        existing = dict(existing)
                                    
                                    # Обновляем существующий чат
                                    try:
                                        existing_id = existing.get('id')
                                        if product_url:
                                            app.logger.info(f"[SYNC] Обновление чата {existing_id} с product_url={product_url}")
                                        
                                        # Формируем SQL запрос с listing_data если есть
                                        update_fields = [
                                            'client_name = ?',
                                            'client_phone = ?',
                                            'customer_id = ?',
                                            'product_url = ?',
                                            'last_message = ?',
                                            'unread_count = ?',
                                            'status = ?',
                                            'priority = ?',
                                            'updated_at = CURRENT_TIMESTAMP'
                                        ]
                                        update_values = [
                                            client_name,
                                            client_phone,
                                            customer_id if customer_id else None,
                                            product_url if product_url else None,
                                            last_message,
                                            unread_count,
                                            status,
                                            priority
                                        ]
                                        
                                        # Добавляем listing_data если есть
                                        if listing_data_json:
                                            update_fields.append('listing_data = ?')
                                            update_values.append(listing_data_json)
                                        
                                        update_values.append(existing_id)
                                        
                                        conn.execute(f'''
                                            UPDATE avito_chats 
                                            SET {', '.join(update_fields)}
                                            WHERE id = ?
                                        ''', tuple(update_values))
                                        chats_updated += 1
                                        total_synced += 1
                                        
                                        # Проверяем, что product_url действительно сохранился
                                        if product_url:
                                            conn.commit()  # Убеждаемся, что изменения сохранены
                                            verify_chat = conn.execute('''
                                                SELECT product_url FROM avito_chats WHERE id = ?
                                            ''', (existing_id,)).fetchone()
                                            if verify_chat:
                                                # Конвертируем sqlite3.Row в dict для безопасного доступа
                                                verify_chat = dict(verify_chat)
                                                saved_url = verify_chat.get('product_url')
                                                app.logger.info(f"[SYNC] Проверка сохранения для чата {existing_id}: product_url в БД = {saved_url}")
                                                if saved_url != product_url:
                                                    app.logger.error(f"[SYNC] ОШИБКА: product_url не совпадает! Ожидалось: {product_url}, Сохранено: {saved_url}")
                                            else:
                                                app.logger.error(f"[SYNC] ОШИБКА: не удалось проверить сохранение product_url для чата {existing_id}")
                                    except Exception as update_err:
                                        app.logger.error(f"[SYNC] Ошибка обновления чата {avito_chat_id_str}: {update_err}", exc_info=True)
                                        if 'chats_errors' in locals():
                                            chats_errors += 1
                                        else:
                                            chats_errors = 1
                                else:
                                    # Создаем новый чат
                                    try:
                                        if product_url:
                                            app.logger.info(f"[SYNC] Создание нового чата с product_url={product_url}")
                                        # Формируем SQL запрос с listing_data если есть
                                        insert_fields = [
                                            'shop_id', 'chat_id', 'customer_id', 'client_name', 'client_phone', 
                                            'product_url', 'last_message', 'unread_count', 'status', 'priority', 
                                            'created_at', 'updated_at'
                                        ]
                                        insert_values = [
                                            shop['id'],
                                            avito_chat_id_str,
                                            customer_id if customer_id else None,
                                            client_name,
                                            client_phone,
                                            product_url if product_url else None,
                                            last_message,
                                            unread_count,
                                            status,
                                            priority,
                                            'CURRENT_TIMESTAMP',
                                            'CURRENT_TIMESTAMP'
                                        ]
                                        
                                        # Добавляем listing_data если есть
                                        if listing_data_json:
                                            insert_fields.append('listing_data')
                                            insert_values.append(listing_data_json)
                                        
                                        # Заменяем CURRENT_TIMESTAMP на ? для параметризованного запроса
                                        placeholders = ['?' for _ in insert_values]
                                        placeholders[-2] = 'CURRENT_TIMESTAMP'  # created_at
                                        placeholders[-1] = 'CURRENT_TIMESTAMP'  # updated_at
                                        
                                        # Убираем CURRENT_TIMESTAMP из значений
                                        final_values = insert_values[:-2]  # Все кроме created_at и updated_at
                                        
                                        cursor = conn.execute(f'''
                                            INSERT INTO avito_chats 
                                                ({', '.join(insert_fields)})
                                            VALUES ({', '.join(placeholders)})
                                        ''', tuple(final_values))
                                        new_chat_id = cursor.lastrowid
                                        chats_created += 1
                                        total_synced += 1
                                        app.logger.info(f"[SYNC] ✅ Создан новый чат: БД_id={new_chat_id}, shop_id={shop['id']}, chat_id={avito_chat_id_str}, client_name={client_name}, product_url={product_url}")
                                        
                                        # Проверяем, что product_url действительно сохранился
                                        if product_url:
                                            verify_chat = conn.execute('''
                                                SELECT product_url FROM avito_chats WHERE id = ?
                                            ''', (new_chat_id,)).fetchone()
                                            if verify_chat:
                                                # Конвертируем sqlite3.Row в dict для безопасного доступа
                                                verify_chat = dict(verify_chat)
                                                app.logger.info(f"[SYNC] Проверка сохранения для нового чата {new_chat_id}: product_url в БД = {verify_chat.get('product_url')}")
                                    except Exception as insert_err:
                                        app.logger.error(f"[SYNC] ❌ Ошибка создания чата {avito_chat_id_str}: {insert_err}", exc_info=True)
                                        app.logger.error(f"[SYNC] Данные чата: shop_id={shop['id']}, client_name={client_name}, customer_id={customer_id}")
                                        if 'chats_errors' in locals():
                                            chats_errors += 1
                                        else:
                                            chats_errors = 1
                                
                                chats_processed += 1
                                
                                # Логируем каждые 10 чатов для отслеживания прогресса
                                if chats_processed % 10 == 0:
                                    app.logger.info(f"[SYNC] Обработано {chats_processed}/{chats_count} чатов (создано: {chats_created}, обновлено: {chats_updated})")
                                
                            except Exception as e:
                                if 'chats_errors' in locals():
                                    chats_errors += 1
                                else:
                                    chats_errors = 1
                                try:
                                    chat_id_str = str(chat_data.get('id', 'unknown'))
                                except:
                                    chat_id_str = 'unknown'
                                app.logger.error(f"[SYNC] Ошибка обработки чата {chat_id_str}: {e}", exc_info=True)
                                errors.append(f"Ошибка обработки чата {chat_id_str}: {str(e)}")
                                continue
                        
                        chats_errors = chats_errors if 'chats_errors' in locals() else 0
                        app.logger.info(f"[SYNC] Страница завершена: обработано={chats_processed}, создано={chats_created}, обновлено={chats_updated}, ошибок={chats_errors}")
                        
                        # Коммитим изменения после каждой страницы
                        conn.commit()
                        # Проверяем реальное количество чатов в БД после коммита
                        actual_count = conn.execute('SELECT COUNT(*) as cnt FROM avito_chats WHERE shop_id = ?', (shop['id'],)).fetchone()
                        if actual_count:
                            actual_count = dict(actual_count)
                            app.logger.info(f"[SYNC] Изменения сохранены в БД (всего синхронизировано: {total_synced}, реально чатов в БД для shop_id={shop['id']}: {actual_count.get('cnt', 0)})")
                        else:
                            app.logger.info(f"[SYNC] Изменения сохранены в БД (всего синхронизировано: {total_synced}, реально чатов в БД для shop_id={shop['id']}: 0)")
                        
                        # Проверяем, есть ли еще чаты
                        # Для v2 API проверяем has_more в метаданных
                        has_more = False
                        if isinstance(response, dict) and 'meta' in response:
                            has_more = response['meta'].get('has_more', False)
                        
                        # Если получили меньше лимита И has_more=False - завершаем
                        if chats_count < limit and not has_more:
                            app.logger.info(f"[SYNC] Получено меньше лимита ({chats_count} < {limit}) и has_more=False, завершаем пагинацию")
                            break
                        elif chats_count == 0 and not has_more:
                            app.logger.info(f"[SYNC] Нет чатов и has_more=False, завершаем пагинацию")
                            break
                        elif has_more:
                            app.logger.info(f"[SYNC] has_more=True, продолжаем пагинацию (offset={offset + limit})")
                        
                        # Защита от бесконечного цикла: если total=0, но продолжаем получать чаты,
                        # ограничиваем максимальное количество страниц (максимум 1000 чатов = 10 страниц)
                        if offset >= 1000:
                            app.logger.warning(f"[SYNC] Достигнут лимит offset={offset}, завершаем пагинацию для защиты от бесконечного цикла")
                            break
                        
                        offset += limit
                        app.logger.info(f"[SYNC] Переходим к следующей странице: offset={offset}")
                        
                    except Exception as e:
                        error_str = str(e)
                        # Проверяем тип ошибки для более детального логирования
                        if '403' in error_str or 'Forbidden' in error_str:
                            app.logger.warning(f"[SYNC] ⚠️  Магазин {shop['id']} ({shop['name']}): 403 Forbidden - ключи не работают или нет доступа")
                            app.logger.warning(f"[SYNC] Пропускаем магазин {shop['id']} и продолжаем с другими магазинами")
                            errors.append(f"Магазин {shop['name']}: 403 Forbidden - ключи не работают или нет доступа")
                        else:
                            app.logger.error(f"[SYNC] Ошибка получения чатов для магазина {shop['name']}: {error_str}")
                            errors.append(f"Ошибка получения чатов для магазина {shop['name']}: {error_str}")
                        break
                
                synced_count += total_synced
                conn.commit()
                app.logger.info(f"[SYNC] ✅ Магазин {shop['id']} ({shop['name']}) синхронизирован: {total_synced} чатов")
                
            except Exception as e:
                error_str = str(e)
                # Проверяем тип ошибки для более детального логирования
                if '403' in error_str or 'Forbidden' in error_str:
                    error_msg = f"Магазин {shop['name']}: 403 Forbidden - ключи не работают или нет доступа"
                    app.logger.warning(f"[SYNC] ⚠️  {error_msg}")
                    app.logger.warning(f"[SYNC] Пропускаем магазин {shop['id']} и продолжаем с другими магазинами")
                else:
                    error_msg = f"Ошибка синхронизации магазина {shop['name']}: {error_str}"
                    app.logger.error(f"[SYNC] ❌ {error_msg}", exc_info=True)
                errors.append(error_msg)
                app.logger.info(f"[SYNC] ========== Ошибка синхронизации магазина {shop['id']}: {shop['name']} ==========")
                continue
        
        app.logger.info(f"[SYNC] ========== Синхронизация завершена ==========")
        app.logger.info(f"[SYNC] Всего синхронизировано чатов: {synced_count}")
        app.logger.info(f"[SYNC] Обработано магазинов: {len(shops)}")
        if errors:
            app.logger.warning(f"[SYNC] Ошибок: {len(errors)}")
            for error in errors:
                app.logger.warning(f"[SYNC]   - {error}")
        else:
            app.logger.info(f"[SYNC] Ошибок нет")
        
        # Автоматически обновляем таймеры для всех чатов после синхронизации
        try:
            app.logger.info(f"[SYNC] 🔄 Начинаем обновление таймеров для всех чатов...")
            from services.messenger_service import MessengerService
            service = MessengerService(conn, None)
            timer_result = service.update_all_response_timers()
            app.logger.info(f"[SYNC] ✅ Таймеры обновлены: обновлено={timer_result['updated']}, ошибок={timer_result['errors']}")
        except Exception as timer_update_error:
            app.logger.warning(f"[SYNC] ⚠️ Ошибка обновления таймеров: {timer_update_error}")
        
        # Автоматически завершаем старые чаты (старше 1 суток)
        try:
            app.logger.info(f"[SYNC] 🔄 Начинаем автозавершение старых чатов...")
            from services.messenger_service import MessengerService
            service = MessengerService(conn, None)
            complete_result = service.auto_complete_old_chats(days=1)
            app.logger.info(f"[SYNC] ✅ Автозавершение завершено: завершено={complete_result['completed']}, ошибок={complete_result['errors']}")
        except Exception as auto_complete_error:
            app.logger.warning(f"[SYNC] ⚠️ Ошибка автозавершения чатов: {auto_complete_error}")
        
        return {
            'success': True,
            'synced_count': synced_count,
            'errors': errors
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'synced_count': synced_count,
            'errors': errors
        }
    finally:
        # Соединение глобальное, не закрываем
        pass


@app.route('/api/chats')
def get_chats():
    """
    Получить список чатов
    """
    app.logger.info(f"[APP.PY/CHATS] Запрос получен через app.route. Session: user_id={session.get('user_id')}")
    # Обертываем всю функцию в try-except, чтобы перехватывать исключения
    # до того, как Flask попытается использовать переменную в handle_user_exception
    try:
        return _get_chats_impl()
    except Exception as flask_exception:
        # Логируем ошибку с полной информацией
        import traceback
        error_traceback = traceback.format_exc()
        app.logger.error(f"[API/CHATS] Критическая ошибка: {flask_exception}")
        app.logger.error(f"[API/CHATS] Traceback: {error_traceback}")
        app.logger.error(f"[API/CHATS] Request args: {dict(request.args)}")
        return jsonify({'error': 'Internal server error', 'message': str(flask_exception)}), 500


def _get_chats_impl():
    """Внутренняя реализация get_chats без конфликта с Flask's handle_user_exception"""
    # Логируем информацию о сессии для диагностики
    session_info = {
        'keys': list(session.keys()),
        'has_user_id': 'user_id' in session,
        'user_id': session.get('user_id'),
        'user_role': session.get('user_role'),
        'permanent': session.get('_permanent', False)
    }
    # Минимальное логирование для производительности
    if 'user_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401

    # НЕ синхронизируем автоматически - это блокирует ответ на 20+ секунд
    # Синхронизация должна вызываться отдельно через /api/chats/sync
    # sync_result = sync_chats_from_avito()
    # if not sync_result.get('success'):
    #     print(f"[WARNING] Ошибка синхронизации чатов: {sync_result.get('error', 'Unknown error')}")

    conn = get_db_connection()
    
    show_pool = request.args.get('pool', 'false').lower() == 'true'
    include_completed = request.args.get('include_completed', 'true').lower() == 'true'
    
    # Параметр для получения только обновленных чатов (для webhook обновлений)
    updated_since = request.args.get('updated_since')
    updated_since_timestamp = None
    if updated_since:
        try:
            # Преобразуем timestamp в datetime, затем в ISO строку для сравнения с БД
            updated_since_dt = datetime.fromtimestamp(int(updated_since) / 1000, tz=timezone.utc)
            updated_since_timestamp = updated_since_dt.isoformat()
        except (ValueError, TypeError, OSError) as ts_error:
            app.logger.warning(f"[API/CHATS] Ошибка преобразования updated_since={updated_since}: {ts_error}")
            updated_since_timestamp = None

    # Проверяем, существуют ли колонки first_name и last_name
    # Пробуем выполнить тестовый запрос с first_name/last_name, чтобы убедиться, что они работают
    has_name_columns = False
    try:
        has_name_columns = check_name_columns(conn)
    except Exception as check_error:
        app.logger.error(f"[API/CHATS] Ошибка проверки колонок: {check_error}", exc_info=True)
        has_name_columns = False  # Используем fallback на username

    # Все пользователи видят все чаты (убраны фильтры по менеджерам)
    # Используем разные запросы в зависимости от наличия колонок
    chats = []  # Инициализируем пустым списком на случай ошибки
    try:
        if show_pool:
            # Показываем только чаты без назначенного менеджера
            if include_completed:
                # Включаем завершенные чаты в пул
                if has_name_columns:
                    chats = conn.execute('''
                    SELECT 
                        c.*, 
                        s.name as shop_name, 
                        COALESCE(
                            NULLIF(TRIM(u.first_name || ' ' || COALESCE(u.last_name, '')), ''),
                            u.username,
                            ''
                        ) as assigned_manager_name,
                        CASE 
                            WHEN s.client_id IS NOT NULL AND s.client_secret IS NOT NULL AND s.user_id IS NOT NULL 
                            THEN 1 ELSE 0 
                        END AS has_avito_creds,
                        CASE 
                            WHEN s.client_id IS NOT NULL AND s.client_secret IS NOT NULL AND s.user_id IS NOT NULL 
                            THEN 'ok' ELSE 'missing' 
                        END AS avito_credentials_status,
                        s.webhook_registered
                    FROM avito_chats c
                    LEFT JOIN avito_shops s ON c.shop_id = s.id
                    LEFT JOIN users u ON c.assigned_manager_id = u.id
                    WHERE c.assigned_manager_id IS NULL
                    ''' + (' AND c.updated_at IS NOT NULL AND c.updated_at > ?' if updated_since_timestamp else '') + '''
                    ORDER BY 
                        CASE WHEN c.response_timer > 0 THEN 0 ELSE 1 END,
                        c.response_timer DESC,
                        c.updated_at DESC
                ''', (updated_since_timestamp,) if updated_since_timestamp else ()).fetchall()
                else:
                    chats = conn.execute('''
                        SELECT 
                            c.*, 
                            s.name as shop_name, 
                            COALESCE(u.username, '') as assigned_manager_name,
                            CASE 
                                WHEN s.client_id IS NOT NULL AND s.client_secret IS NOT NULL AND s.user_id IS NOT NULL 
                                THEN 1 ELSE 0 
                            END AS has_avito_creds,
                            CASE 
                                WHEN s.client_id IS NOT NULL AND s.client_secret IS NOT NULL AND s.user_id IS NOT NULL 
                                THEN 'ok' ELSE 'missing' 
                            END AS avito_credentials_status,
                            s.webhook_registered
                        FROM avito_chats c
                        LEFT JOIN avito_shops s ON c.shop_id = s.id
                        LEFT JOIN users u ON c.assigned_manager_id = u.id
                        WHERE c.assigned_manager_id IS NULL
                        ''' + (' AND c.updated_at IS NOT NULL AND c.updated_at > ?' if updated_since_timestamp else '') + '''
                        ORDER BY 
                            CASE WHEN c.response_timer > 0 THEN 0 ELSE 1 END,
                            c.response_timer DESC,
                            c.updated_at DESC
                    ''', (updated_since_timestamp,) if updated_since_timestamp else ()).fetchall()
            else:
                # Не включаем завершенные чаты в пул
                if has_name_columns:
                    chats = conn.execute('''
                        SELECT 
                            c.*, 
                            s.name as shop_name, 
                            COALESCE(
                                NULLIF(TRIM(u.first_name || ' ' || COALESCE(u.last_name, '')), ''),
                                u.username,
                                ''
                            ) as assigned_manager_name,
                            CASE 
                                WHEN s.client_id IS NOT NULL AND s.client_secret IS NOT NULL AND s.user_id IS NOT NULL 
                                THEN 1 ELSE 0 
                            END AS has_avito_creds,
                            CASE 
                                WHEN s.client_id IS NOT NULL AND s.client_secret IS NOT NULL AND s.user_id IS NOT NULL 
                                THEN 'ok' ELSE 'missing' 
                            END AS avito_credentials_status,
                            s.webhook_registered
                        FROM avito_chats c
                        LEFT JOIN avito_shops s ON c.shop_id = s.id
                        LEFT JOIN users u ON c.assigned_manager_id = u.id
                        WHERE c.assigned_manager_id IS NULL AND c.status != 'completed'
                        ''' + (' AND c.updated_at IS NOT NULL AND c.updated_at > ?' if updated_since_timestamp else '') + '''
                        ORDER BY 
                            CASE WHEN c.response_timer > 0 THEN 0 ELSE 1 END,
                            c.response_timer DESC,
                            c.updated_at DESC
                    ''', (updated_since_timestamp,) if updated_since_timestamp else ()).fetchall()
                else:
                    chats = conn.execute('''
                    SELECT 
                        c.*, 
                        s.name as shop_name, 
                        COALESCE(u.username, '') as assigned_manager_name,
                        CASE 
                            WHEN s.client_id IS NOT NULL AND s.client_secret IS NOT NULL AND s.user_id IS NOT NULL 
                            THEN 1 ELSE 0 
                        END AS has_avito_creds,
                        CASE 
                            WHEN s.client_id IS NOT NULL AND s.client_secret IS NOT NULL AND s.user_id IS NOT NULL 
                            THEN 'ok' ELSE 'missing' 
                        END AS avito_credentials_status,
                        s.webhook_registered
                    FROM avito_chats c
                    LEFT JOIN avito_shops s ON c.shop_id = s.id
                    LEFT JOIN users u ON c.assigned_manager_id = u.id
                    WHERE c.assigned_manager_id IS NULL AND c.status != 'completed'
                    ''' + (' AND c.updated_at > ?' if updated_since_timestamp else '') + '''
                    ORDER BY 
                        CASE WHEN c.response_timer > 0 THEN 0 ELSE 1 END,
                        c.response_timer DESC,
                        c.updated_at DESC
                ''', (updated_since_timestamp,) if updated_since_timestamp else ()).fetchall()
        else:
            # Показываем все чаты (включая завершенные по умолчанию, чтобы фронтенд мог фильтровать)
            if has_name_columns:
                chats = conn.execute('''
                    SELECT 
                        c.*, 
                        s.name as shop_name, 
                        COALESCE(
                            NULLIF(TRIM(u.first_name || ' ' || COALESCE(u.last_name, '')), ''),
                            u.username,
                            ''
                        ) as assigned_manager_name,
                        CASE 
                            WHEN s.client_id IS NOT NULL AND s.client_secret IS NOT NULL AND s.user_id IS NOT NULL 
                            THEN 1 ELSE 0 
                        END AS has_avito_creds,
                        CASE 
                            WHEN s.client_id IS NOT NULL AND s.client_secret IS NOT NULL AND s.user_id IS NOT NULL 
                            THEN 'ok' ELSE 'missing' 
                        END AS avito_credentials_status,
                        s.webhook_registered
                    FROM avito_chats c
                    LEFT JOIN avito_shops s ON c.shop_id = s.id
                    LEFT JOIN users u ON c.assigned_manager_id = u.id
                    ''' + ('WHERE c.updated_at IS NOT NULL AND c.updated_at > ?' if updated_since_timestamp else '') + '''
                    ORDER BY 
                        CASE WHEN c.response_timer > 0 THEN 0 ELSE 1 END,
                        c.response_timer DESC,
                        c.updated_at DESC
                ''', (updated_since_timestamp,) if updated_since_timestamp else ()).fetchall()
            else:
                chats = conn.execute('''
                    SELECT 
                        c.*, 
                        s.name as shop_name, 
                        COALESCE(u.username, '') as assigned_manager_name,
                        CASE 
                            WHEN s.client_id IS NOT NULL AND s.client_secret IS NOT NULL AND s.user_id IS NOT NULL 
                            THEN 1 ELSE 0 
                        END AS has_avito_creds,
                        CASE 
                            WHEN s.client_id IS NOT NULL AND s.client_secret IS NOT NULL AND s.user_id IS NOT NULL 
                            THEN 'ok' ELSE 'missing' 
                        END AS avito_credentials_status,
                        s.webhook_registered
                    FROM avito_chats c
                    LEFT JOIN avito_shops s ON c.shop_id = s.id
                    LEFT JOIN users u ON c.assigned_manager_id = u.id
                    ''' + ('WHERE c.updated_at IS NOT NULL AND c.updated_at > ?' if updated_since_timestamp else '') + '''
                    ORDER BY 
                        CASE WHEN c.response_timer > 0 THEN 0 ELSE 1 END,
                        c.response_timer DESC,
                        c.updated_at DESC
                ''', (updated_since_timestamp,) if updated_since_timestamp else ()).fetchall()
        # Логируем результат после выполнения всех запросов
    except Exception as sql_error:
        app.logger.error(f"[API/CHATS] Ошибка при выполнении SQL запроса: {sql_error}", exc_info=True)
        # Используем простой fallback запрос без first_name/last_name
        app.logger.warning(f"[API/CHATS] Используем fallback запрос на username")
        try:
            if show_pool:
                if include_completed:
                    chats = conn.execute('''
                        SELECT 
                            c.*, 
                            s.name as shop_name, 
                            COALESCE(u.username, '') as assigned_manager_name,
                            CASE 
                                WHEN s.client_id IS NOT NULL AND s.client_secret IS NOT NULL AND s.user_id IS NOT NULL 
                                THEN 1 ELSE 0 
                            END AS has_avito_creds,
                            CASE 
                                WHEN s.client_id IS NOT NULL AND s.client_secret IS NOT NULL AND s.user_id IS NOT NULL 
                                THEN 'ok' ELSE 'missing' 
                            END AS avito_credentials_status,
                            s.webhook_registered
                        FROM avito_chats c
                        LEFT JOIN avito_shops s ON c.shop_id = s.id
                        LEFT JOIN users u ON c.assigned_manager_id = u.id
                        WHERE c.assigned_manager_id IS NULL
                        ''' + (' AND c.updated_at IS NOT NULL AND c.updated_at > ?' if updated_since_timestamp else '') + '''
                        ORDER BY 
                            CASE WHEN c.response_timer > 0 THEN 0 ELSE 1 END,
                            c.response_timer DESC,
                            c.updated_at DESC
                    ''', (updated_since_timestamp,) if updated_since_timestamp else ()).fetchall()
                else:
                    chats = conn.execute('''
                        SELECT 
                            c.*, 
                            s.name as shop_name, 
                            COALESCE(u.username, '') as assigned_manager_name,
                            CASE 
                                WHEN s.client_id IS NOT NULL AND s.client_secret IS NOT NULL AND s.user_id IS NOT NULL 
                                THEN 1 ELSE 0 
                            END AS has_avito_creds,
                            CASE 
                                WHEN s.client_id IS NOT NULL AND s.client_secret IS NOT NULL AND s.user_id IS NOT NULL 
                                THEN 'ok' ELSE 'missing' 
                            END AS avito_credentials_status,
                            s.webhook_registered
                        FROM avito_chats c
                        LEFT JOIN avito_shops s ON c.shop_id = s.id
                        LEFT JOIN users u ON c.assigned_manager_id = u.id
                        WHERE c.assigned_manager_id IS NULL AND c.status != 'completed'
                        ''' + (' AND c.updated_at IS NOT NULL AND c.updated_at > ?' if updated_since_timestamp else '') + '''
                        ORDER BY 
                            CASE WHEN c.response_timer > 0 THEN 0 ELSE 1 END,
                            c.response_timer DESC,
                            c.updated_at DESC
                    ''', (updated_since_timestamp,) if updated_since_timestamp else ()).fetchall()
            else:
                chats = conn.execute('''
                    SELECT 
                        c.*, 
                        s.name as shop_name, 
                        COALESCE(u.username, '') as assigned_manager_name,
                        CASE 
                            WHEN s.client_id IS NOT NULL AND s.client_secret IS NOT NULL AND s.user_id IS NOT NULL 
                            THEN 1 ELSE 0 
                        END AS has_avito_creds,
                        CASE 
                            WHEN s.client_id IS NOT NULL AND s.client_secret IS NOT NULL AND s.user_id IS NOT NULL 
                            THEN 'ok' ELSE 'missing' 
                        END AS avito_credentials_status,
                        s.webhook_registered
                    FROM avito_chats c
                    LEFT JOIN avito_shops s ON c.shop_id = s.id
                    LEFT JOIN users u ON c.assigned_manager_id = u.id
                    ''' + ('WHERE c.updated_at IS NOT NULL AND c.updated_at > ?' if updated_since_timestamp else '') + '''
                    ORDER BY 
                        CASE WHEN c.response_timer > 0 THEN 0 ELSE 1 END,
                        c.response_timer DESC,
                        c.updated_at DESC
                ''', (updated_since_timestamp,) if updated_since_timestamp else ()).fetchall()
        except Exception as fallback_error:
            app.logger.error(f"[API/CHATS] Критическая ошибка даже в fallback запросе: {fallback_error}", exc_info=True)
            return jsonify({'error': 'Internal server error', 'message': str(fallback_error)}), 500

    # Логируем количество результатов SQL запроса
    
    # Проверяем, есть ли вообще чаты в базе данных
    if not chats or len(chats) == 0:
        try:
            total_chats_count = conn.execute('SELECT COUNT(*) as count FROM avito_chats').fetchone()
            total_count = total_chats_count[0] if total_chats_count else 0
            app.logger.warning(f"[API/CHATS] ⚠️ SQL запрос вернул 0 результатов, но в таблице avito_chats всего {total_count} чатов")
            
            # Проверяем, есть ли чаты с назначенными менеджерами
            if show_pool:
                pool_chats_count = conn.execute('SELECT COUNT(*) as count FROM avito_chats WHERE assigned_manager_id IS NULL').fetchone()
                pool_count = pool_chats_count[0] if pool_chats_count else 0
                app.logger.warning(f"[API/CHATS] ⚠️ В пуле (assigned_manager_id IS NULL) должно быть {pool_count} чатов")
        except Exception as count_error:
            app.logger.error(f"[API/CHATS] Ошибка при проверке количества чатов: {count_error}", exc_info=True)

    # ОПТИМИЗАЦИЯ: Вычисляем response_timer для всех чатов одним запросом через JOIN
    # Это намного эффективнее, чем запросы в цикле
    try:
        # Оптимизированное преобразование результатов в список словарей
        chats_list = [dict(chat) if not isinstance(chat, dict) else chat for chat in chats]
        
        chat_ids = [chat.get('id') for chat in chats_list if chat.get('id')]
        response_timers = {}
        
        if chat_ids:
            try:
                # Используем один запрос для получения всех response_timer
                # Находим последнее неотвеченное входящее сообщение для каждого чата
                # Вычисляем время в Python для корректной обработки ISO формата
                from datetime import datetime, timezone
                
                # Используем Python для правильного сравнения timestamp
                # Получаем все входящие и исходящие сообщения для правильного сравнения
                all_messages = conn.execute('''
                    SELECT 
                        chat_id,
                        message_type,
                        timestamp
                    FROM avito_messages
                    WHERE chat_id IN ({})
                    ORDER BY chat_id, timestamp DESC
                '''.format(','.join('?' * len(chat_ids))), 
                chat_ids).fetchall()
                
                # Группируем по чатам и находим последнее неотвеченное сообщение
                from collections import defaultdict
                chat_last_outgoing = {}
                chat_last_unanswered = {}
                
                # Оптимизированная обработка сообщений
                for msg in all_messages:
                    # Быстрая проверка типа без создания dict если не нужно
                    if isinstance(msg, dict):
                        msg_dict = msg
                    else:
                        msg_dict = dict(msg)
                    
                    chat_id = msg_dict.get('chat_id')
                    msg_type = msg_dict.get('message_type')
                    msg_timestamp = msg_dict.get('timestamp')
                    
                    if not chat_id or not msg_timestamp:
                        continue
                    
                    # Находим последнее исходящее сообщение для каждого чата
                    if msg_type == 'outgoing':
                        current = chat_last_outgoing.get(chat_id)
                        if current is None or msg_timestamp > current:
                            chat_last_outgoing[chat_id] = msg_timestamp
                    
                    # Находим последнее входящее сообщение после последнего исходящего
                    elif msg_type == 'incoming':
                        last_outgoing = chat_last_outgoing.get(chat_id)
                        if last_outgoing is None or msg_timestamp > last_outgoing:
                            current_unanswered = chat_last_unanswered.get(chat_id)
                            if current_unanswered is None or msg_timestamp > current_unanswered:
                                chat_last_unanswered[chat_id] = msg_timestamp
                
                # Оптимизированное формирование результата
                timer_query = [
                    {'chat_id': chat_id, 'last_unanswered_time': last_unanswered}
                    for chat_id in chat_ids
                    for last_unanswered in [chat_last_unanswered.get(chat_id)]
                    if last_unanswered
                ]
                
                now = datetime.now(timezone.utc)
                
                for row in timer_query:
                    try:
                        chat_id = row.get('chat_id')
                        last_unanswered_time_str = row.get('last_unanswered_time')
                        
                        if chat_id and last_unanswered_time_str:
                            # Парсим timestamp из ISO формата
                            try:
                                # Пробуем разные форматы ISO
                                if 'T' in str(last_unanswered_time_str):
                                    # ISO формат с T
                                    if '+' in str(last_unanswered_time_str) or str(last_unanswered_time_str).endswith('Z'):
                                        # С часовым поясом
                                        last_time = datetime.fromisoformat(str(last_unanswered_time_str).replace('Z', '+00:00'))
                                    else:
                                        # Без часового пояса - считаем UTC
                                        last_time = datetime.fromisoformat(str(last_unanswered_time_str))
                                        if last_time.tzinfo is None:
                                            last_time = last_time.replace(tzinfo=timezone.utc)
                                else:
                                    # Пробуем другие форматы
                                    try:
                                        last_time = datetime.fromisoformat(str(last_unanswered_time_str))
                                        if last_time.tzinfo is None:
                                            last_time = last_time.replace(tzinfo=timezone.utc)
                                    except:
                                        # Если не получается, используем текущее время
                                        last_time = now
                                
                                # Вычисляем разницу в минутах
                                time_diff = now - last_time
                                timer_minutes = max(0, int(time_diff.total_seconds() / 60))
                                
                                response_timers[chat_id] = timer_minutes
                            except Exception as parse_error:
                                app.logger.warning(f"[API/CHATS] Ошибка парсинга timestamp для чата {chat_id}: {parse_error}, timestamp: {last_unanswered_time_str}")
                                response_timers[chat_id] = 0
                        elif chat_id:
                            response_timers[chat_id] = 0
                    except Exception as timer_error:
                        app.logger.warning(f"[API/CHATS] Ошибка обработки response_timer для чата: {timer_error}")
                        continue
            except Exception as timer_calc_error:
                app.logger.warning(f"[API/CHATS] Ошибка вычисления response_timer: {timer_calc_error}", exc_info=True)
        
        # Обрабатываем каждый чат
        for chat_dict in chats_list:
            try:
                chat_dict['avito_credentials_status'] = 'ok' if chat_dict.pop('has_avito_creds', 0) else 'missing'
                chat_dict['webhook_registered'] = bool(chat_dict.get('webhook_registered', False))
                chat_dict['has_avito_creds'] = chat_dict['avito_credentials_status'] == 'ok'
                
                # Используем предвычисленное значение response_timer из оптимизированного запроса
                # Это ускоряет загрузку чатов в 10-100 раз в зависимости от количества чатов
                chat_dict['response_timer'] = response_timers.get(chat_dict.get('id'), 0)
            except Exception as chat_process_error:
                app.logger.error(f"[API/CHATS] Ошибка обработки чата {chat_dict.get('id', 'unknown')}: {chat_process_error}", exc_info=True)
                # Устанавливаем значения по умолчанию
                chat_dict['avito_credentials_status'] = 'missing'
                chat_dict['webhook_registered'] = False
                chat_dict['has_avito_creds'] = False
                chat_dict['response_timer'] = 0
                continue
    except Exception as process_error:
        app.logger.error(f"[API/CHATS] Критическая ошибка при обработке результатов: {process_error}", exc_info=True)
        return jsonify({'error': 'Internal server error', 'message': str(process_error)}), 500
    
    # Соединение глобальное, не закрываем
    
    return jsonify(chats_list)


# API для обновления таймеров для всех чатов
@app.route('/api/chats/update-timers', methods=['POST'])
@require_auth
@handle_errors
def update_all_timers():
    """Обновить response_timer для всех чатов"""
    try:
        from services.messenger_service import MessengerService
        from avito_api import AvitoAPI
        
        conn = get_db_connection()
        # Создаем фиктивный API объект (не используется для обновления таймеров)
        service = MessengerService(conn, None)
        
        result = service.update_all_response_timers()
        
        app.logger.info(f"[API/UPDATE TIMERS] Обновление таймеров завершено: {result}")
        
        return jsonify({
            'success': True,
            'updated': result['updated'],
            'errors': result['errors']
        }), 200
    except Exception as e:
        app.logger.error(f"[API/UPDATE TIMERS] Ошибка обновления таймеров: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500


# API для автозавершения старых чатов
@app.route('/api/chats/auto-complete', methods=['POST'])
@require_auth
@handle_errors
def auto_complete_chats():
    """Автоматически завершить чаты старше 2 дней"""
    try:
        from services.messenger_service import MessengerService
        from avito_api import AvitoAPI
        
        days = request.json.get('days', 2) if request.is_json else 2
        
        conn = get_db_connection()
        # Создаем фиктивный API объект (не используется для автозавершения)
        service = MessengerService(conn, None)
        
        result = service.auto_complete_old_chats(days=days)
        
        app.logger.info(f"[API/AUTO COMPLETE] Автозавершение завершено: {result}")
        
        return jsonify({
            'success': True,
            'completed': result['completed'],
            'errors': result['errors']
        }), 200
    except Exception as e:
        app.logger.error(f"[API/AUTO COMPLETE] Ошибка автозавершения: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500


# API для ручной синхронизации чатов из Avito
@app.route('/api/chats/sync', methods=['POST'])
def sync_chats():
    # Проверяем аутентификацию вручную для лучшей диагностики
    cookie_header = request.headers.get('Cookie', 'None')
    app.logger.info(f"[API/CHATS/SYNC] Запрос получен. Session keys: {list(session.keys())}, user_id: {session.get('user_id')}")
    app.logger.info(f"[API/CHATS/SYNC] Cookie present: {cookie_header != 'None'}, Cookie length: {len(cookie_header) if cookie_header != 'None' else 0}")
    
    if 'user_id' not in session:
        app.logger.warning(f"[API/CHATS/SYNC] Пользователь не аутентифицирован. Session: {dict(session)}")
        app.logger.warning(f"[API/CHATS/SYNC] Все заголовки: {dict(request.headers)}")
        return jsonify({'error': 'Not authenticated'}), 401
    """
    Ручная синхронизация чатов из Avito API
    """
    try:
        # Пытаемся получить JSON, но не требуем его обязательного наличия
        data = {}
        if request.is_json:
            data = request.get_json() or {}
        elif request.data:
            try:
                data = json.loads(request.data.decode('utf-8')) or {}
            except (json.JSONDecodeError, UnicodeDecodeError):
                data = {}
        
        shop_id = data.get('shop_id')  # Опционально: синхронизировать конкретный магазин
        
        result = sync_chats_from_avito(shop_id=shop_id)
        
        if result.get('success'):
            return jsonify({
                'success': True,
                'message': f'Синхронизировано {result["synced_count"]} новых чатов',
                'synced_count': result['synced_count'],
                'errors': result.get('errors', [])
            }), 200
        else:
            return jsonify({
                'success': False,
                'error': result.get('error', 'Unknown error'),
                'errors': result.get('errors', [])
            }), 500
    except Exception as e:
        app.logger.error(f'Ошибка синхронизации чатов: {e}', exc_info=True)
        return jsonify({
            'success': False,
            'error': f'Ошибка синхронизации: {str(e)}'
        }), 500


# API для обновления чата
@app.route('/api/chats/<int:chat_id>', methods=['PUT'])
def update_chat(chat_id):
    if 'user_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401

    data = request.get_json() or {}
    if not isinstance(data, dict):
        return jsonify({'error': 'Invalid payload'}), 400
    conn = get_db_connection()
    
    try:
        update_fields = []
        update_values = []
        
        if 'status' in data:
            update_fields.append('status = ?')
            update_values.append(data['status'])
        
        if 'priority' in data:
            update_fields.append('priority = ?')
            update_values.append(data['priority'])
        
        if 'assigned_manager_id' in data:
            update_fields.append('assigned_manager_id = ?')
            update_values.append(data['assigned_manager_id'] if data['assigned_manager_id'] else None)
        
        if update_fields:
            update_fields.append('updated_at = CURRENT_TIMESTAMP')
            update_values.append(chat_id)
            
            query = f'UPDATE avito_chats SET {", ".join(update_fields)} WHERE id = ?'
            conn.execute(query, tuple(update_values))
            conn.commit()
        
        # Соединение глобальное, не закрываем
        return jsonify({'success': True}), 200
    except Exception as e:
        # Соединение глобальное, не закрываем
        return jsonify({'error': str(e)}), 400


# Статические файлы
@app.route('/static/<path:filename>')
def serve_static(filename):
    return send_from_directory('../frontend', filename)


# Страница управления чатами
@app.route('/chats')
def chats_page():
    try:
        if 'user_id' not in session:
            return redirect('/login')

        user = get_user_by_id(session['user_id'])
        if not user:
            app.logger.error(f'[CHATS PAGE] Пользователь не найден для user_id: {session.get("user_id")}')
            return redirect('/login')
        
        app.logger.info(f'[CHATS PAGE] Рендеринг страницы чатов для пользователя: {user.get("username", "unknown")}')
        # Отключаем кеширование для страницы чатов, чтобы изменения применялись сразу
        response = make_response(render_template('chats.html', user=user, cache_version=int(time.time())))
        response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '0'
        return response
    except Exception as e:
        app.logger.error(f'[CHATS PAGE] Ошибка при рендеринге страницы чатов: {e}', exc_info=True)
        import traceback
        traceback.print_exc()
        return f'Ошибка загрузки страницы чатов: {str(e)}', 500


# Страница управления магазинами
@app.route('/shops')
def shops_page():
    if 'user_id' not in session:
        return redirect('/login')

    user = get_user_by_id(session['user_id'])
    return render_template('shops.html', user=user)


# Страница доставок
@app.route('/deliveries')
def deliveries_page():
    if 'user_id' not in session:
        return redirect('/login')

    user = get_user_by_id(session['user_id'])
    return render_template('deliveries.html', user=user)


# Страница аналитики
@app.route('/analytics')
def analytics_page():
    if 'user_id' not in session:
        return redirect('/login')

    user = get_user_by_id(session['user_id'])
    # Получаем расширенную статистику для страницы аналитики
    stats = get_system_stats()
    return render_template('analytics.html', user=user, stats=stats)


# Страница выкупа объявлений
@app.route('/buyout')
def buyout_page():
    if 'user_id' not in session:
        return redirect('/login')
    user = get_user_by_id(session['user_id'])
    return render_template('buyout.html', user=user)


# Страница настроек
@app.route('/settings')
def settings_page():
    try:
        if 'user_id' not in session:
            return redirect('/login')

        user = get_user_by_id(session['user_id'])
        if not user:
            app.logger.error(f'[SETTINGS] Пользователь не найден для user_id: {session.get("user_id")}')
            return redirect('/login')
        
        app.logger.info(f'[SETTINGS] Рендеринг страницы настроек для пользователя: {user.get("username", "unknown")}')
        return render_template('settings.html', user=user)
    except Exception as e:
        app.logger.error(f'[SETTINGS] Ошибка при рендеринге страницы настроек: {e}', exc_info=True)
        import traceback
        traceback.print_exc()
        return f'Ошибка загрузки страницы настроек: {str(e)}', 500

# Страница управления менеджерами (только админ)
@app.route('/managers')
@require_auth
@handle_errors
def managers_page():
    user_role = session.get('user_role')
    if user_role not in ['admin', 'super_admin']:
        return redirect('/login')
    user = get_user_by_id(session['user_id'])
    return render_template('managers.html', user=user)

# Страница логов системы (только супер-админ)
@app.route('/system-logs')
@require_auth
@require_role('super_admin')
@handle_errors
def system_logs_page():
    user = get_user_by_id(session['user_id'])
    return render_template('system_logs.html', user=user)

# Страница управления быстрыми ответами
@app.route('/quick-replies')
@require_auth
@handle_errors
def quick_replies_page():
    user = get_user_by_id(session['user_id'])
    return render_template('quick_replies.html', user=user)

# CSS файлы
@app.route('/css/<filename>')
def serve_css(filename):
    return send_from_directory('../frontend/css', filename)


# ==================== МОДУЛЬ МАГАЗИНОВ ====================

# API для создания магазина (только админ и super_admin)
@app.route('/api/shops', methods=['POST'])
@require_auth
@handle_errors
def create_shop():
    # Проверяем права доступа (admin или super_admin)
    user_role = session.get('user_role')
    if user_role not in ['admin', 'super_admin']:
        return jsonify({'error': 'Access denied. Требуется роль admin или super_admin'}), 403
    data = request.get_json()
    
    # Валидация
    if not data:
        return jsonify({'error': 'No data provided'}), 400
    
    name = (data.get('name') or '').strip()
    shop_url = (data.get('shop_url') or '').strip()
    # api_key может быть None, поэтому обрабатываем отдельно
    api_key_value = data.get('api_key')
    api_key = api_key_value.strip() if api_key_value else None
    
    if not name or not shop_url:
        return jsonify({'error': 'Name and shop_url are required'}), 400
    
    # Валидация URL
    if not shop_url.startswith(('http://', 'https://')):
        return jsonify({'error': 'Invalid shop URL'}), 400
    
    conn = get_db_connection()
    try:
        cursor = conn.execute('''
            INSERT INTO avito_shops (name, shop_url, api_key, is_active)
            VALUES (?, ?, ?, ?)
        ''', (name, shop_url, api_key, data.get('is_active', True)))
        shop_id = cursor.lastrowid
        conn.commit()
        # Соединение глобальное, не закрываем
        return jsonify({'success': True, 'id': shop_id}), 201
    except Exception as e:
        # Соединение глобальное, не закрываем
        if 'UNIQUE constraint' in str(e):
            return jsonify({'error': 'Shop with this URL already exists'}), 400
        return jsonify({'error': str(e)}), 400


# API для обновления магазина
@app.route('/api/shops/<int:shop_id>', methods=['GET'])
@require_auth
@handle_errors
def get_shop(shop_id):
    """Получить данные одного магазина"""
    user_role = session.get('user_role')
    if user_role not in ['admin', 'super_admin']:
        return jsonify({'error': 'Access denied'}), 403
    
    conn = get_db_connection()
    try:
        shop = conn.execute('''
            SELECT 
                *, 
                CASE 
                    WHEN client_id IS NOT NULL AND client_secret IS NOT NULL AND user_id IS NOT NULL 
                    THEN 'ok' ELSE 'missing' 
                END AS avito_status
            FROM avito_shops 
            WHERE id = ?
        ''', (shop_id,)).fetchone()
        
        if not shop:
            # Соединение глобальное, не закрываем
            return jsonify({'error': 'Магазин не найден'}), 404
        
        shop_dict = dict(shop)
        # Соединение глобальное, не закрываем
        return jsonify(shop_dict), 200
    except Exception as e:
        # Соединение глобальное, не закрываем
        app.logger.error(f'[GET SHOP] Ошибка: {e}', exc_info=True)
        return jsonify({'error': str(e)}), 500


@app.route('/api/shops/<int:shop_id>', methods=['PUT'])
@require_auth
@handle_errors
def update_shop(shop_id):
    user_role = session.get('user_role')
    if user_role not in ['admin', 'super_admin']:
        return jsonify({'error': 'Access denied'}), 403

    data = request.get_json() or {}
    if not isinstance(data, dict):
        return jsonify({'error': 'Invalid payload'}), 400
    
    name = (data.get('name') or '').strip()
    shop_url = (data.get('shop_url') or '').strip()
    api_key_value = data.get('api_key')
    api_key = api_key_value.strip() if api_key_value else None
    is_active = data.get('is_active', True)
    
    if not name or not shop_url:
        return jsonify({'error': 'Name and shop_url are required'}), 400
    
    if not shop_url.startswith(('http://', 'https://')):
        return jsonify({'error': 'Invalid shop URL'}), 400
    
    conn = get_db_connection()
    try:
        # Проверяем существование магазина
        exists = conn.execute('SELECT id FROM avito_shops WHERE id = ?', (shop_id,)).fetchone()
        if not exists:
            # Соединение глобальное, не закрываем
            return jsonify({'error': 'Магазин не найден'}), 404
        
        conn.execute('''
            UPDATE avito_shops 
            SET name = ?, shop_url = ?, api_key = ?, is_active = ?
            WHERE id = ?
        ''', (name, shop_url, api_key, is_active, shop_id))
        conn.commit()
        # Соединение глобальное, не закрываем
        return jsonify({'success': True}), 200
    except Exception as e:
        # Соединение глобальное, не закрываем
        app.logger.error(f'[UPDATE SHOP] Ошибка: {e}', exc_info=True)
        return jsonify({'error': str(e)}), 400


# API для обновления OAuth ключей магазина (только админ и super_admin)
@app.route('/api/shops/<int:shop_id>/credentials', methods=['PUT'])
@require_auth
@handle_errors
def update_shop_credentials(shop_id):
    # Проверяем права доступа (admin или super_admin)
    user_role = session.get('user_role')
    if user_role not in ['admin', 'super_admin']:
        return jsonify({'error': 'Access denied. Требуется роль admin или super_admin'}), 403
    
    data = request.get_json() or {}
    if not data:
        return jsonify({'error': 'No data provided'}), 400
    
    # Конвертируем значения в строки перед применением strip()
    client_id = str(data.get('client_id') or '').strip()
    client_secret = str(data.get('client_secret') or '').strip()
    user_id = str(data.get('user_id') or '').strip()

    if not client_id or not client_secret or not user_id:
        return jsonify({'error': 'client_id, client_secret и user_id обязательны'}), 400

    conn = get_db_connection()
    try:
        exists = conn.execute('SELECT id, name FROM avito_shops WHERE id = ?', (shop_id,)).fetchone()
        if not exists:
            # Соединение глобальное, не закрываем
            return jsonify({'error': 'Shop not found'}), 404

        # Проверяем текущие значения перед обновлением
        current = conn.execute('''
            SELECT client_id, client_secret, user_id FROM avito_shops WHERE id = ?
        ''', (shop_id,)).fetchone()
        
        app.logger.debug(f"[SHOPS] Сохранение ключей для магазина {shop_id} ({exists['name']})")
        app.logger.debug(f"[SHOPS] Текущие значения: client_id={'*' if current['client_id'] else 'None'}, user_id={current['user_id']}")
        app.logger.debug(f"[SHOPS] Новые значения: client_id={'*' if client_id else 'None'}, user_id={user_id}")

        # Выполняем обновление
        cursor = conn.execute('''
            UPDATE avito_shops
            SET client_id = ?, client_secret = ?, user_id = ?, token_status = NULL, token_checked_at = NULL
            WHERE id = ?
        ''', (client_id, client_secret, user_id, shop_id))
        
        rows_affected = cursor.rowcount
        app.logger.info(f'[UPDATE CREDENTIALS] Обновлено строк: {rows_affected} для магазина {shop_id}')
        
        if rows_affected == 0:
            # Соединение глобальное, не закрываем
            app.logger.error(f'[UPDATE CREDENTIALS] Магазин {shop_id} не найден для обновления')
            return jsonify({'error': 'Магазин не найден'}), 404
        
        conn.commit()
        app.logger.info(f'[UPDATE CREDENTIALS] Изменения зафиксированы в БД для магазина {shop_id}')
        
        # Проверяем, что данные действительно сохранились
        verify = conn.execute('''
            SELECT client_id, client_secret, user_id FROM avito_shops WHERE id = ?
        ''', (shop_id,)).fetchone()
        
        # Нормализуем значения для сравнения (убираем пробелы, приводим к строкам)
        verify_client_id = str(verify['client_id'] or '').strip() if verify['client_id'] else ''
        verify_user_id = str(verify['user_id'] or '').strip() if verify['user_id'] else ''
        expected_client_id = str(client_id).strip()
        expected_user_id = str(user_id).strip()
        
        app.logger.info(f'[UPDATE CREDENTIALS] Проверка сохранения для магазина {shop_id}')
        app.logger.info(f'[UPDATE CREDENTIALS] Ожидалось: client_id={expected_client_id[:10] if expected_client_id else "None"}..., user_id={expected_user_id}')
        app.logger.info(f'[UPDATE CREDENTIALS] Получено: client_id={verify_client_id[:10] if verify_client_id else "None"}..., user_id={verify_user_id}')
        
        if verify_client_id != expected_client_id or verify_user_id != expected_user_id:
            app.logger.error(f'[UPDATE CREDENTIALS] Данные не сохранились! Ожидалось: client_id={expected_client_id[:10] if expected_client_id else "None"}..., user_id={expected_user_id}, получено: client_id={verify_client_id[:10] if verify_client_id else "None"}..., user_id={verify_user_id}')
            # Соединение глобальное, не закрываем
            return jsonify({'error': 'Данные не были сохранены. Попробуйте еще раз.'}), 500
        
        app.logger.info(f'[UPDATE CREDENTIALS] Ключи успешно сохранены для магазина {shop_id}')
        
        # Регистрируем webhook для магазина
        webhook_registered = False
        try:
            from avito_api import AvitoAPI
            import os
            
            # Получаем URL для webhook из переменных окружения или используем osagaming.store
            webhook_url = os.getenv('AVITO_WEBHOOK_URL')
            if not webhook_url:
                # Используем домен osagaming.store
                webhook_url = "https://osagaming.store/webhook/avito"
            
            app.logger.info(f'[UPDATE CREDENTIALS] Регистрация webhook для магазина {shop_id}: {webhook_url}')
            
            api = AvitoAPI(client_id=client_id, client_secret=client_secret)
            webhook_result = api.register_webhook_v3(
                url=webhook_url,
                types=['message', 'chat']
            )
            
            if webhook_result:
                # Обновляем флаг webhook_registered в БД
                conn.execute('''
                    UPDATE avito_shops 
                    SET webhook_registered = 1 
                    WHERE id = ?
                ''', (shop_id,))
                conn.commit()
                webhook_registered = True
                app.logger.info(f'[UPDATE CREDENTIALS] Webhook успешно зарегистрирован для магазина {shop_id}')
            else:
                app.logger.warning(f'[UPDATE CREDENTIALS] Не удалось зарегистрировать webhook для магазина {shop_id}')
        except Exception as webhook_err:
            app.logger.warning(f'[UPDATE CREDENTIALS] Ошибка регистрации webhook: {webhook_err}', exc_info=True)
            # Не прерываем выполнение, если webhook не зарегистрировался
        
        # Синхронизируем чаты сразу после сохранения ключей
        sync_result = {'success': False, 'synced_count': 0}
        try:
            app.logger.info(f'[UPDATE CREDENTIALS] Начинаем синхронизацию чатов для магазина {shop_id}')
            sync_result = sync_chats_from_avito(shop_id=shop_id)
            if sync_result.get('success'):
                synced_count = sync_result.get('synced_count', 0)
                app.logger.info(f'[UPDATE CREDENTIALS] Синхронизировано {synced_count} чатов для магазина {shop_id}')
            else:
                app.logger.warning(f'[UPDATE CREDENTIALS] Ошибка синхронизации чатов: {sync_result.get("error", "Unknown error")}')
        except Exception as sync_err:
            app.logger.warning(f'[UPDATE CREDENTIALS] Ошибка синхронизации чатов: {sync_err}', exc_info=True)
            # Не прерываем выполнение, если синхронизация не удалась
        
        # Логируем действие
        try:
            log_activity(session.get('user_id'), 'update_avito_credentials', 
                        f'Обновлены OAuth ключи для магазина ID: {shop_id} ({exists["name"]})', 'shop', shop_id)
        except Exception as log_err:
            app.logger.warning(f'[UPDATE CREDENTIALS] Не удалось залогировать действие: {log_err}')
        
        # Соединение глобальное, не закрываем
        
        message = 'Ключи успешно сохранены'
        if webhook_registered:
            message += '. Webhook зарегистрирован'
        if sync_result.get('success'):
            message += f'. Синхронизировано {sync_result.get("synced_count", 0)} чатов'
        
        return jsonify({
            'success': True, 
            'message': message,
            'webhook_registered': webhook_registered,
            'chats_synced': sync_result.get('synced_count', 0)
        })
    except Exception as e:
        # Соединение глобальное, не закрываем
        app.logger.error(f'[UPDATE CREDENTIALS] Ошибка сохранения ключей Avito: {e}', exc_info=True)
        import traceback
        traceback.print_exc()
        return jsonify({'error': f'Ошибка сохранения: {str(e)}'}), 500


@app.route('/api/shops/analytics')
@require_auth
@require_role('admin')
@handle_errors
def shops_analytics():
    conn = get_db_connection()
    data = conn.execute('''
        SELECT 
            s.id,
            s.name,
            s.is_active,
            s.token_status,
            s.webhook_registered,
            COUNT(c.id) as total_chats,
            SUM(CASE WHEN c.status = 'active' THEN 1 ELSE 0 END) as active_chats,
            SUM(CASE WHEN c.priority = 'urgent' THEN 1 ELSE 0 END) as urgent_chats,
            AVG(c.response_timer) as avg_response_timer,
            SUM(CASE WHEN c.unread_count > 0 THEN 1 ELSE 0 END) as chats_with_unread
        FROM avito_shops s
        LEFT JOIN avito_chats c ON c.shop_id = s.id
        GROUP BY s.id, s.name, s.is_active, s.token_status, s.webhook_registered
        ORDER BY s.name
    ''').fetchall()
            # Соединение глобальное, не закрываем
    return jsonify([dict(row) for row in data])


# API для удаления магазина
@app.route('/api/shops/<int:shop_id>', methods=['DELETE'])
@require_auth
@handle_errors
def delete_shop(shop_id):
    # Проверяем права доступа (admin или super_admin)
    user_role = session.get('user_role')
    if user_role not in ['admin', 'super_admin']:
        return jsonify({'error': 'Access denied. Требуется роль admin или super_admin'}), 403

    conn = get_db_connection()
    try:
        # Проверяем существование магазина
        shop = conn.execute('SELECT id, name FROM avito_shops WHERE id = ?', (shop_id,)).fetchone()
        if not shop:
            # Соединение глобальное, не закрываем
            return jsonify({'error': 'Shop not found'}), 404
        
        # Удаляем назначения менеджеров на этот магазин
        conn.execute('DELETE FROM manager_assignments WHERE shop_id = ?', (shop_id,))
        
        # Удаляем магазин
        conn.execute('DELETE FROM avito_shops WHERE id = ?', (shop_id,))
        conn.commit()
        
        # Логируем действие
        log_activity(session['user_id'], 'delete_shop', 
                    f'Удален магазин ID: {shop_id} ({shop["name"]})', 'shop', shop_id)
        
        # Соединение глобальное, не закрываем
        return jsonify({'success': True, 'message': 'Магазин успешно удален'}), 200
    except Exception as e:
        # Соединение глобальное, не закрываем
        app.logger.error(f"Ошибка удаления магазина: {e}", exc_info=True)
        return jsonify({'error': f'Ошибка удаления: {str(e)}'}), 400


# API для назначения менеджера на магазин
@app.route('/api/shops/<int:shop_id>/assign', methods=['POST'])
def assign_manager(shop_id):
    if 'user_id' not in session or session.get('user_role') != 'admin':
        return jsonify({'error': 'Access denied'}), 403

    data = request.get_json()
    manager_id = data.get('manager_id')
    
    conn = get_db_connection()
    try:
        conn.execute('''
            INSERT OR IGNORE INTO manager_assignments (manager_id, shop_id)
            VALUES (?, ?)
        ''', (manager_id, shop_id))
        conn.commit()
        # Соединение глобальное, не закрываем
        return jsonify({'success': True}), 200
    except Exception as e:
        # Соединение глобальное, не закрываем
        return jsonify({'error': str(e)}), 400


# API для получения статистики по магазину
@app.route('/api/shops/<int:shop_id>/stats')
def get_shop_stats(shop_id):
    if 'user_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401

    conn = get_db_connection()
    stats = {
        'total_chats': conn.execute('SELECT COUNT(*) as count FROM avito_chats WHERE shop_id = ?', (shop_id,)).fetchone()['count'],
        'active_chats': conn.execute('SELECT COUNT(*) as count FROM avito_chats WHERE shop_id = ? AND status = "active"', (shop_id,)).fetchone()['count'],
        'urgent_chats': conn.execute('SELECT COUNT(*) as count FROM avito_chats WHERE shop_id = ? AND priority = "urgent"', (shop_id,)).fetchone()['count'],
    }
            # Соединение глобальное, не закрываем
    return jsonify(stats)


@app.route('/api/shops/<int:shop_id>/avito/health')
def avito_shop_health(shop_id):
    """Проверка доступности Avito OAuth для конкретного магазина"""
    if 'user_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401

    conn = get_db_connection()
    shop = conn.execute('''
        SELECT id, name, client_id, client_secret, user_id
        FROM avito_shops
        WHERE id = ?
    ''', (shop_id,)).fetchone()
            # Соединение глобальное, не закрываем

    if not shop:
        return jsonify({'error': 'Shop not found'}), 404

    if not shop['client_id'] or not shop['client_secret'] or not shop['user_id']:
        return jsonify({
            'status': 'missing',
            'details': 'Отсутствуют client_id/client_secret/user_id'
        }), 400

    from avito_api import AvitoAPI
    api = AvitoAPI(shop['client_id'], shop['client_secret'], shop_id=str(shop_id))
    health = api.health_check()
    status_code = 200 if health.get('status') == 'ok' else 502
    return jsonify(health), status_code


@app.route('/api/avito/test-send', methods=['POST'])
def avito_test_send():
    """
    Минимальный e2e тест отправки сообщения в Avito.
    Требует admin, shop_id, chat_id и message.
    """
    if 'user_id' not in session or session.get('user_role') != 'admin':
        return jsonify({'error': 'Access denied'}), 403

    data = request.get_json() or {}
    shop_id = data.get('shop_id')
    chat_id = data.get('chat_id')
    message = (data.get('message') or '').strip()

    if not shop_id or not chat_id or not message:
        return jsonify({'error': 'shop_id, chat_id и message обязательны'}), 400

    conn = get_db_connection()
    shop = conn.execute('''
        SELECT id, client_id, client_secret, user_id
        FROM avito_shops
        WHERE id = ?
    ''', (shop_id,)).fetchone()
            # Соединение глобальное, не закрываем

    if not shop:
        return jsonify({'error': 'Shop not found'}), 404

    if not shop['client_id'] or not shop['client_secret'] or not shop['user_id']:
        return jsonify({'error': 'Отсутствуют OAuth ключи для магазина'}), 400

    from avito_api import AvitoAPI
    api = AvitoAPI(shop['client_id'], shop['client_secret'], shop_id=str(shop_id))
    try:
        api.send_message(user_id=shop['user_id'], chat_id=str(chat_id), message=message)
        return jsonify({'success': True})
    except Exception as exc:
        return jsonify({'error': str(exc)}), 502


# API для получения назначенных менеджеров магазина
@app.route('/api/shops/<int:shop_id>/managers')
def get_shop_managers(shop_id):
    if 'user_id' not in session or session.get('user_role') != 'admin':
        return jsonify({'error': 'Access denied'}), 403

    conn = get_db_connection()
    managers = conn.execute('''
        SELECT u.id, u.username, u.email
        FROM users u
        JOIN manager_assignments ma ON u.id = ma.manager_id
        WHERE ma.shop_id = ?
    ''', (shop_id,)).fetchall()
            # Соединение глобальное, не закрываем
    
    return jsonify([dict(manager) for manager in managers])


# ==================== МОДУЛЬ ДОСТАВОК ====================

# API для получения доставок
@app.route('/api/deliveries')
def get_deliveries():
    if 'user_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401

    conn = get_db_connection()
    
    try:
        has_name_cols = check_name_columns(conn)
        if session.get('user_role') == 'admin':
            # Оптимизированный запрос с индексами
            if has_name_cols:
                deliveries = conn.execute('''
                    SELECT d.*, c.client_name, c.client_phone, c.id as chat_id, c.response_timer, 
                           COALESCE(
                               NULLIF(TRIM(u.first_name || ' ' || COALESCE(u.last_name, '')), ''),
                               u.username,
                               'Система'
                           ) as manager_name
                    FROM deliveries d
                    LEFT JOIN avito_chats c ON d.chat_id = c.id
                    LEFT JOIN users u ON d.manager_id = u.id
                    ORDER BY d.updated_at DESC
                    LIMIT 1000
                ''').fetchall()
            else:
                deliveries = conn.execute('''
                    SELECT d.*, c.client_name, c.client_phone, c.id as chat_id, c.response_timer, 
                           COALESCE(u.username, 'Система') as manager_name
                    FROM deliveries d
                    LEFT JOIN avito_chats c ON d.chat_id = c.id
                    LEFT JOIN users u ON d.manager_id = u.id
                    ORDER BY d.updated_at DESC
                    LIMIT 1000
                ''').fetchall()
        else:
            # Оптимизированный запрос для менеджера
            deliveries = conn.execute('''
                SELECT d.*, c.client_name, c.client_phone, c.id as chat_id, c.response_timer
                FROM deliveries d
                LEFT JOIN avito_chats c ON d.chat_id = c.id
                WHERE d.manager_id = ?
                ORDER BY d.updated_at DESC
                LIMIT 1000
            ''', (session['user_id'],)).fetchall()
        
        # Преобразуем в словари и добавляем поддержку нескольких клиентов
        result = []
        for delivery in deliveries:
            delivery_dict = dict(delivery)
            # Если есть несколько клиентов для одной доставки (через связанные чаты)
            # Это можно расширить в будущем для поддержки множественных клиентов
            result.append(delivery_dict)
        
        return jsonify(result)
    finally:
        # Соединение глобальное, не закрываем
        pass


# API для создания доставки
@app.route('/api/deliveries', methods=['POST'])
def create_delivery():
    if 'user_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401

    data = request.get_json() or {}
    if not isinstance(data, dict):
        return jsonify({'error': 'Invalid payload'}), 400
    conn = get_db_connection()
    
    try:
        chat_id = data.get('chat_id')
        cursor = conn.execute('''
            INSERT INTO deliveries (chat_id, manager_id, delivery_status, address, tracking_number, notes)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (chat_id if chat_id else None, session['user_id'], data.get('status', 'processing'),
              data.get('address'), data.get('tracking_number'), data.get('notes')))
        
        # Обновляем приоритет чата на delivery, если chat_id указан
        if chat_id:
            conn.execute('UPDATE avito_chats SET priority = "delivery" WHERE id = ?', (chat_id,))
        
        log_activity(session['user_id'], 'create_delivery', 
                    f'Создана доставка ID: {cursor.lastrowid}', 'delivery', cursor.lastrowid)
        
        conn.commit()
        # Соединение глобальное, не закрываем
        return jsonify({'success': True, 'id': cursor.lastrowid}), 201
    except Exception as e:
        # Соединение глобальное, не закрываем
        return jsonify({'error': str(e)}), 400


# API для обновления статуса доставки
@app.route('/api/deliveries/<int:delivery_id>', methods=['PUT'])
def update_delivery(delivery_id):
    if 'user_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401

    data = request.get_json() or {}
    if not isinstance(data, dict):
        return jsonify({'error': 'Invalid payload'}), 400
    conn = get_db_connection()
    
    try:
        # Валидация статуса (новые упрощенные статусы)
        valid_statuses = ['free', 'in_work', 'on_delivery', 'closed', 'refused']
        new_status = data.get('status')
        if new_status and new_status not in valid_statuses:
            # Соединение глобальное, не закрываем
            return jsonify({'error': 'Invalid status'}), 400
        
        # Обновляем поля доставки
        update_fields = []
        update_values = []
        
        if 'status' in data:
            update_fields.append('delivery_status = ?')
            update_values.append(data.get('status'))
        
        if 'address' in data:
            update_fields.append('address = ?')
            update_values.append(data.get('address'))
        
        if 'tracking_number' in data:
            update_fields.append('tracking_number = ?')
            update_values.append(data.get('tracking_number'))
        
        if 'notes' in data:
            update_fields.append('notes = ?')
            update_values.append(data.get('notes'))
        
        if 'chat_id' in data:
            update_fields.append('chat_id = ?')
            update_values.append(data.get('chat_id') if data.get('chat_id') else None)
        
        if update_fields:
            update_fields.append('updated_at = CURRENT_TIMESTAMP')
            update_values.append(delivery_id)
            query = f'UPDATE deliveries SET {", ".join(update_fields)} WHERE id = ?'
            conn.execute(query, tuple(update_values))
        
        # Логируем изменение
        log_activity(session['user_id'], 'update_delivery', 
                    f'Обновлена доставка ID: {delivery_id}', 'delivery', delivery_id)
        
        conn.commit()
        # Соединение глобальное, не закрываем
        return jsonify({'success': True}), 200
    except Exception as e:
        # Соединение глобальное, не закрываем
        return jsonify({'error': str(e)}), 400


# API для batch обновления статусов доставок
@app.route('/api/deliveries/batch', methods=['PUT'])
@require_auth
@handle_errors
def batch_update_deliveries():
    """Массовое обновление статусов доставок для повышения производительности"""
    data = request.get_json()
    
    if not data or 'updates' not in data:
        return jsonify({'error': 'No updates provided'}), 400
    
    updates = data.get('updates', [])
    if not isinstance(updates, list) or len(updates) == 0:
        return jsonify({'error': 'Updates must be a non-empty array'}), 400
    
    if len(updates) > 100:  # Ограничение на количество обновлений за раз
        return jsonify({'error': 'Too many updates. Maximum 100 at once'}), 400
    
    conn = get_db_connection()
    valid_statuses = ['free', 'in_work', 'on_delivery', 'closed', 'refused']
    updated_count = 0
    
    try:
        for update in updates:
            delivery_id = update.get('id')
            if not delivery_id:
                continue
            
            # Проверка прав доступа для менеджеров
            if session.get('user_role') != 'admin':
                # Менеджер может обновлять только свои доставки
                check = conn.execute(
                    'SELECT manager_id FROM deliveries WHERE id = ?',
                    (delivery_id,)
                ).fetchone()
                if not check or check['manager_id'] != session['user_id']:
                    continue
            
            update_fields = []
            update_values = []
            
            if 'status' in update:
                status = update.get('status')
                if status in valid_statuses:
                    update_fields.append('delivery_status = ?')
                    update_values.append(status)
            
            if 'address' in update:
                update_fields.append('address = ?')
                update_values.append(update.get('address'))
            
            if 'tracking_number' in update:
                update_fields.append('tracking_number = ?')
                update_values.append(update.get('tracking_number'))
            
            if 'notes' in update:
                update_fields.append('notes = ?')
                update_values.append(update.get('notes'))
            
            if update_fields:
                update_fields.append('updated_at = CURRENT_TIMESTAMP')
                update_values.append(delivery_id)
                query = f'UPDATE deliveries SET {", ".join(update_fields)} WHERE id = ?'
                conn.execute(query, tuple(update_values))
                updated_count += 1
        
        conn.commit()
        # Соединение глобальное, не закрываем
        
        # Логируем batch операцию
        log_activity(session['user_id'], 'batch_update_deliveries', 
                    f'Массовое обновление {updated_count} доставок', 'delivery', None)
        
        return jsonify({'success': True, 'updated': updated_count}), 200
    except Exception as e:
        # Соединение глобальное, не закрываем
        return jsonify({'error': str(e)}), 400


# ==================== МОДУЛЬ СООБЩЕНИЙ ====================

# API для получения сообщений чата (оптимизировано с пагинацией)
@app.route('/api/chats/<int:chat_id>/messages')
def get_chat_messages(chat_id):
    # Диагностика сессии
    cookie_header = request.headers.get('Cookie', 'None')
    session_keys = list(session.keys())
    has_user_id = 'user_id' in session
    user_id_value = session.get('user_id')
    
    app.logger.info(f"[API/MESSAGES] Запрос для чата {chat_id}")
    app.logger.info(f"[API/MESSAGES] Cookie present: {cookie_header != 'None'}, Cookie length: {len(cookie_header) if cookie_header != 'None' else 0}")
    app.logger.info(f"[API/MESSAGES] Session keys: {session_keys}, has_user_id: {has_user_id}, user_id: {user_id_value}")
    
    # Пытаемся понять, почему сессия пустая
    if not has_user_id:
        # Проверяем, есть ли вообще какие-то данные в сессии
        if len(session_keys) == 0:
            app.logger.warning(f"[API/MESSAGES] ⚠️ Сессия полностью пустая! Cookie был отправлен, но сессия не расшифрована.")
            app.logger.warning(f"[API/MESSAGES] Возможные причины: SECRET_KEY изменился, сессия истекла, или проблема с декодированием")
        else:
            app.logger.warning(f"[API/MESSAGES] ⚠️ В сессии есть данные ({session_keys}), но нет user_id!")
        
        return jsonify({'error': 'Not authenticated'}), 401

    # Параметры пагинации
    raw_limit = request.args.get('limit', default=50, type=int)
    raw_offset = request.args.get('offset', default=0, type=int)
    limit = max(1, min(raw_limit if raw_limit is not None else 50, 500))  # Максимум 500 сообщений
    offset = max(0, raw_offset if raw_offset is not None else 0)
    before_id = request.args.get('before_id')  # Для загрузки старых сообщений
    sync = request.args.get('sync', 'false').lower() == 'true'
    
    conn = get_db_connection()
    
    # Получаем данные чата для синхронизации
    try:
        chat = conn.execute('''
            SELECT ac.*, s.client_id, s.client_secret, s.user_id
            FROM avito_chats ac
            JOIN avito_shops s ON ac.shop_id = s.id
            WHERE ac.id = ?
        ''', (chat_id,)).fetchone()
        
        if not chat:
            # Соединение глобальное, не закрываем
            return jsonify({'error': 'Chat not found'}), 404
    
        # Преобразуем Row в dict для удобства
        chat_dict = dict(chat)
        
        # Логируем все ключи для диагностики
        app.logger.info(f"[API/MESSAGES] Ключи в chat_dict: {list(chat_dict.keys())}")
        
        # Синхронизация сообщений, если запрошена и есть ключи
        app.logger.info(f"[API/MESSAGES] Запрос сообщений для чата {chat_id}, sync={sync}")
        app.logger.info(f"[API/MESSAGES] client_id={bool(chat_dict.get('client_id'))}, client_secret={bool(chat_dict.get('client_secret'))}, user_id={chat_dict.get('user_id')}")
        app.logger.info(f"[API/MESSAGES] Условие синхронизации: sync={sync}, client_id={bool(chat_dict.get('client_id'))}, client_secret={bool(chat_dict.get('client_secret'))}, user_id={bool(chat_dict.get('user_id'))}")
        
        # Троттлинг: проверяем, когда была последняя синхронизация для этого чата
        import time
        SYNC_COOLDOWN = 5  # Минимум 5 секунд между синхронизациями одного чата
        last_sync_key = f"last_sync_{chat_id}"
        last_sync_time = getattr(get_chat_messages, last_sync_key, 0)
        current_time = time.time()
        time_since_last_sync = current_time - last_sync_time
        
        should_sync = sync and chat_dict.get('client_id') and chat_dict.get('client_secret') and chat_dict.get('user_id')
        
        if should_sync:
            if time_since_last_sync < SYNC_COOLDOWN:
                app.logger.info(f"[API/MESSAGES] ⏸️ Синхронизация пропущена: прошло только {time_since_last_sync:.1f} сек (минимум {SYNC_COOLDOWN} сек)")
                should_sync = False
            else:
                # Обновляем время последней синхронизации
                setattr(get_chat_messages, last_sync_key, current_time)
                app.logger.info(f"[API/MESSAGES] ✅ Синхронизация разрешена: прошло {time_since_last_sync:.1f} сек")
        
        if should_sync:
            app.logger.info(f"[API/MESSAGES] ✅ Условие синхронизации выполнено, начинаем синхронизацию...")
            try:
                avito_chat_id = chat_dict.get('chat_id')
                if not avito_chat_id:
                    app.logger.error(f"[API/MESSAGES] ❌ avito_chat_id не найден в chat_dict! Доступные ключи: {list(chat_dict.keys())}")
                    raise ValueError(f"avito_chat_id не найден для чата {chat_id}")
                app.logger.info(f"[API/MESSAGES] Начинаем синхронизацию сообщений для чата {chat_id}, user_id={chat_dict.get('user_id')}, avito_chat_id={avito_chat_id}")
                from avito_api import AvitoAPI
                from services.messenger_service import MessengerService
                
                api = AvitoAPI(
                    client_id=chat_dict['client_id'],
                    client_secret=chat_dict['client_secret']
                )
                service = MessengerService(conn, api)
                
                # Синхронизируем
                new_messages_count = service.sync_chat_messages(
                    chat_id=chat_id,
                    user_id=chat_dict['user_id'],
                    avito_chat_id=avito_chat_id
                )
                app.logger.info(f"[API/MESSAGES] Синхронизация завершена: {new_messages_count} новых сообщений для чата {chat_id}")
            except Exception as sync_error:
                app.logger.error(f"[API/MESSAGES] Ошибка синхронизации сообщений для чата {chat_id}: {sync_error}", exc_info=True)
                import traceback
                app.logger.error(f"[API/MESSAGES] Traceback: {traceback.format_exc()}")
        else:
            if sync:
                app.logger.warning(f"[API/MESSAGES] ⚠️ Синхронизация запрошена, но не выполнена: client_id={bool(chat_dict.get('client_id'))}, client_secret={bool(chat_dict.get('client_secret'))}, user_id={bool(chat_dict.get('user_id'))}")
            else:
                app.logger.info(f"[API/MESSAGES] Синхронизация не запрошена (sync=false)")
    except Exception as messages_error:
        app.logger.error(f"[API/MESSAGES] Критическая ошибка при получении данных чата {chat_id}: {messages_error}", exc_info=True)
        import traceback
        app.logger.error(f"[API/MESSAGES] Traceback: {traceback.format_exc()}")
        # Соединение глобальное, не закрываем
        return jsonify({'error': 'Internal server error', 'message': str(messages_error)}), 500
    
    # Базовый запрос сообщений из БД
    try:
        has_name_cols = check_name_columns(conn)
        if has_name_cols:
            query = '''
                SELECT m.*, COALESCE(TRIM(u.first_name || ' ' || COALESCE(u.last_name, '')), u.username, 'Система') as manager_name
                FROM avito_messages m
                LEFT JOIN users u ON m.manager_id = u.id
                WHERE m.chat_id = ?
            '''
        else:
            query = '''
                SELECT m.*, COALESCE(u.username, 'Система') as manager_name
                FROM avito_messages m
                LEFT JOIN users u ON m.manager_id = u.id
                WHERE m.chat_id = ?
            '''
        params = [chat_id]
        
        # Если указан before_id, загружаем сообщения до этого ID (для прокрутки вверх)
        if before_id:
            try:
                before_id_int = int(before_id)
                query += ' AND m.id < ?'
                params.append(before_id_int)
            except ValueError:
                pass
        
        query += ' ORDER BY m.timestamp DESC LIMIT ? OFFSET ?'
        params.extend([limit, offset])
        
        messages = conn.execute(query, tuple(params)).fetchall()
        
        # Получаем общее количество сообщений для пагинации
        total_count = conn.execute(
            'SELECT COUNT(*) as count FROM avito_messages WHERE chat_id = ?',
            (chat_id,)
        ).fetchone()['count']
        remaining_count = total_count
        if before_id:
            try:
                remaining_count = conn.execute(
                    'SELECT COUNT(*) as count FROM avito_messages WHERE chat_id = ? AND id < ?',
                    (chat_id, int(before_id))
                ).fetchone()['count']
            except ValueError:
                remaining_count = total_count

        # Логируем открытие чата
        log_activity(session['user_id'], 'open_chat', 
                    f'Открыт чат ID: {chat_id}', 'chat', chat_id)
        
        app.logger.info(f"[API/MESSAGES] Возвращаем {len(messages)} сообщений из {total_count} всего для чата {chat_id}")
        if len(messages) > 0:
            first_msg = dict(messages[0])
            app.logger.info(f"[API/MESSAGES] Первое сообщение: id={first_msg.get('id')}, text={first_msg.get('message_text', '')[:50]}...")
        else:
            app.logger.warning(f"[API/MESSAGES] ⚠️ Нет сообщений для чата {chat_id} (total={total_count})")
            # Проверяем, есть ли last_message в таблице чатов
            chat_info_check = conn.execute(
                'SELECT last_message, chat_id FROM avito_chats WHERE id = ?',
                (chat_id,)
            ).fetchone()
            if chat_info_check and chat_info_check['last_message']:
                app.logger.warning(f"[API/MESSAGES] ⚠️ В avito_chats.last_message есть данные, но в avito_messages нет! chat_id={chat_info_check['chat_id']}")
        
        # Соединение глобальное, не закрываем
        
        # Возвращаем сообщения в правильном порядке (старые первыми для отображения в чате)
        # В БД они отсортированы по timestamp DESC (новые первыми), поэтому переворачиваем
        messages_list = [dict(msg) for msg in reversed(messages)]
        
        return jsonify({
            'messages': messages_list,
            'total': total_count,
            'limit': limit,
            'offset': offset,
            'has_more': (offset + len(messages_list)) < remaining_count
        })
    except Exception as query_error:
        app.logger.error(f"[API/MESSAGES] Ошибка при запросе сообщений для чата {chat_id}: {query_error}", exc_info=True)
        import traceback
        app.logger.error(f"[API/MESSAGES] Traceback: {traceback.format_exc()}")
        # Соединение глобальное, не закрываем
        return jsonify({'error': 'Internal server error', 'message': str(query_error)}), 500


# API для извлечения product_url из сообщений чата
# Перемещен в backend/api/chats_api.py в blueprint chats_bp
# Оставлено для обратной совместимости, но рекомендуется использовать endpoint из blueprint
@app.route('/api/chats/<int:chat_id>/extract-product-url', methods=['POST'])
@require_auth
@handle_errors
def extract_product_url_legacy(chat_id):
    """
    Извлечь product_url для чата (legacy endpoint для обратной совместимости)
    """
    from avito_api import AvitoAPI
    import re
    
    app.logger.info(f"[EXTRACT PRODUCT URL] Запрос на извлечение product_url для чата {chat_id}")
    user_id = session.get('user_id')
    app.logger.info(f"[EXTRACT PRODUCT URL] User ID: {user_id}")
    
    conn = get_db_connection()
    try:
        # Получаем информацию о чате
        chat = conn.execute('''
            SELECT ac.id, ac.chat_id, ac.shop_id, s.client_id, s.client_secret, s.user_id, s.shop_url
            FROM avito_chats ac
            JOIN avito_shops s ON ac.shop_id = s.id
            WHERE ac.id = ?
        ''', (chat_id,)).fetchone()
        
        if not chat:
            app.logger.warning(f"[EXTRACT PRODUCT URL] Чат {chat_id} не найден в базе данных")
            # Соединение глобальное, не закрываем
            return jsonify({
                'success': False,
                'error': 'Chat not found',
                'message': f'Чат с ID {chat_id} не найден в базе данных'
            }), 404
        
        chat_dict = dict(chat)
        product_url = None
        source = None
        
        # Сначала пробуем через API get_chat_by_id
        if chat_dict.get('client_id') and chat_dict.get('client_secret') and chat_dict.get('user_id'):
            try:
                api = AvitoAPI(
                    client_id=chat_dict['client_id'],
                    client_secret=chat_dict['client_secret']
                )
                chat_details = api.get_chat_by_id(
                    user_id=chat_dict['user_id'],
                    chat_id=chat_dict['chat_id']
                )
                
                if isinstance(chat_details, dict):
                    # УЛУЧШЕННЫЙ ПОИСК: Проверяем context.value (актуальная структура API v3)
                    detail_context = chat_details.get('context', {})
                    if isinstance(detail_context, dict):
                        detail_item = (detail_context.get('value') or 
                                      detail_context.get('item') or 
                                      detail_context.get('listing') or 
                                      detail_context.get('ad', {}))
                        
                        if isinstance(detail_item, dict):
                            detail_item_id = detail_item.get('id')
                            detail_url = (detail_item.get('url') or 
                                         detail_item.get('link') or 
                                         detail_item.get('href') or
                                         detail_item.get('value') or
                                         detail_item.get('uri'))
                            if detail_url:
                                product_url = detail_url
                                if product_url.startswith('/'):
                                    product_url = f"https://www.avito.ru{product_url}"
                                elif not product_url.startswith('http'):
                                    product_url = f"https://www.avito.ru{product_url}"
                                source = 'api_context'
                                app.logger.info(f"[EXTRACT PRODUCT URL] Найден через API context.value: {product_url}")
                            elif detail_item_id:
                                item_id_str = str(detail_item_id)
                                shop_url_part = chat_dict.get('shop_url', '').split('/')[-1] if chat_dict.get('shop_url') else ''
                                if shop_url_part:
                                    product_url = f"https://www.avito.ru/{shop_url_part}/items/{item_id_str}"
                                else:
                                    product_url = f"https://www.avito.ru/items/{item_id_str}"
                                source = 'api_context_id'
                                app.logger.info(f"[EXTRACT PRODUCT URL] Найден через API context.value.id: {product_url}")
                    
                    # Если не нашли в context, проверяем прямые поля
                    if not product_url:
                        product_url = (chat_details.get('item_url') or 
                                     chat_details.get('listing_url') or 
                                     chat_details.get('ad_url') or
                                     chat_details.get('product_url'))
                        if product_url:
                            source = 'api_direct'
                            app.logger.info(f"[EXTRACT PRODUCT URL] Найден через API прямые поля: {product_url}")
                    
                    # Стратегия 3: Глубокий поиск в ответе API
                    if not product_url:
                        def find_url_in_dict(d, depth=0, max_depth=3):
                            """Рекурсивный поиск URL в словаре"""
                            if depth > max_depth:
                                return None
                            if isinstance(d, dict):
                                for key, value in d.items():
                                    if isinstance(value, str) and ('avito.ru' in value.lower() or '/items/' in value.lower()):
                                        if re.search(r'items/\d+', value, re.IGNORECASE):
                                            return value
                                    result = find_url_in_dict(value, depth + 1, max_depth)
                                    if result:
                                        return result
                            elif isinstance(d, list):
                                for item in d:
                                    result = find_url_in_dict(item, depth + 1, max_depth)
                                    if result:
                                        return result
                            return None
                        
                        found_url = find_url_in_dict(chat_details)
                        if found_url:
                            product_url = found_url
                            if product_url.startswith('/'):
                                product_url = f"https://www.avito.ru{product_url}"
                            elif not product_url.startswith('http'):
                                product_url = f"https://www.avito.ru{product_url}"
                            source = 'api_deep_search'
                            app.logger.info(f"[EXTRACT PRODUCT URL] Найден через глубокий поиск в API: {product_url}")
            except Exception as api_error:
                app.logger.warning(f"[EXTRACT PRODUCT URL] Ошибка API для чата {chat_id}: {api_error}")
        
            
            # Стратегия 2: Ищем item_id в любых сообщениях
            if not product_url:
                all_messages = conn.execute('''
                    SELECT message_text FROM avito_messages 
                    WHERE chat_id = ? AND message_text IS NOT NULL
                    ORDER BY timestamp DESC LIMIT 100
                ''', (chat_id,)).fetchall()
                
                item_id_pattern = r'\b(\d{7,10})\b'
                potential_item_ids = []
                
                for msg_row in all_messages:
                    msg_text = msg_row['message_text'] or ''
                    if not msg_text:
                        continue
                    
                    matches = re.findall(item_id_pattern, msg_text)
                    for match in matches:
                        if match not in potential_item_ids:
                            potential_item_ids.append(match)
                
                potential_item_ids.sort(key=len, reverse=True)
                for item_id in potential_item_ids[:5]:
                    shop_url_part = chat_dict.get('shop_url', '').split('/')[-1] if chat_dict.get('shop_url') else ''
                    if shop_url_part:
                        test_url = f"https://www.avito.ru/{shop_url_part}/items/{item_id}"
                    else:
                        test_url = f"https://www.avito.ru/items/{item_id}"
                    
                    url_in_messages = any(
                        test_url.lower() in (msg_row['message_text'] or '').lower() 
                        for msg_row in all_messages[:20]
                    )
                    
                    if url_in_messages:
                        product_url = test_url
                        source = 'messages_id'
                        app.logger.info(f"[EXTRACT PRODUCT URL] Найден item_id в сообщениях: {item_id}, URL: {product_url}")
                        break
        
        # Сохраняем найденный product_url
        if product_url:
            conn.execute('''
                UPDATE avito_chats 
                SET product_url = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (product_url, chat_id))
            conn.commit()
            
            app.logger.info(f"[EXTRACT PRODUCT URL] Для чата {chat_id} найден product_url: {product_url} (источник: {source})")
            # Соединение глобальное, не закрываем
            return jsonify({
                'success': True,
                'product_url': product_url,
                'source': source or 'unknown'
            }), 200
        
        # Соединение глобальное, не закрываем
        app.logger.info(f"[EXTRACT PRODUCT URL] Для чата {chat_id} product_url не найден ни в API, ни в сообщениях")
        # Возвращаем 200 с success: false, а не 404, так как это не ошибка маршрута
        return jsonify({
            'success': False,
            'message': 'Product URL not found',
            'error': 'Product URL not found in chat messages or API'
        }), 200
    except Exception as e:
        app.logger.error(f"[EXTRACT PRODUCT URL] Ошибка для чата {chat_id}: {e}", exc_info=True)
        # Соединение глобальное, не закрываем
        return jsonify({
            'success': False,
            'error': str(e),
            'message': f'Ошибка при извлечении product_url: {str(e)}'
        }), 500

# API для получения информации об объявлении из чата
@app.route('/api/chats/<int:chat_id>/listing')
@require_auth
@handle_errors
def get_chat_listing(chat_id):
    """Получить информацию об объявлении, связанном с чатом"""
    from services.chat_listing_service import ChatListingService
    
    user_id = session.get('user_id')
    user_role = session.get('user_role', 'unknown')
    
    app.logger.info(f"[CHAT LISTING] ========== НАЧАЛО ЗАГРУЗКИ ОБЪЯВЛЕНИЯ ДЛЯ ЧАТА ==========")
    app.logger.info(f"[CHAT LISTING] Chat ID: {chat_id}, User ID: {user_id}, Role: {user_role}")
    
    if 'user_id' not in session:
        app.logger.warning(f"[CHAT LISTING] Неавторизованный запрос для чата {chat_id}")
        return jsonify({'error': 'Not authenticated'}), 401
    
    try:
        # Используем сервис для получения информации об объявлении
        try:
            result = ChatListingService.get_chat_listing(
                chat_id=chat_id,
                user_id=user_id,
                user_role=user_role
            )
        except Exception as service_error:
            app.logger.error(f"[CHAT LISTING] Ошибка в сервисе get_chat_listing: {service_error}", exc_info=True)
            import traceback
            app.logger.error(f"[CHAT LISTING] Traceback: {traceback.format_exc()}")
            # Пытаемся получить хотя бы базовую информацию
            try:
                chat_dict, product_url, item_id = ChatListingService.get_chat_listing_info(chat_id)
                return jsonify({
                    'error': f'Service error: {str(service_error)}',
                    'product_url': product_url or '',
                    'item_id': item_id or ''
                }), 500
            except Exception:
                return jsonify({
                    'error': f'Service error: {str(service_error)}',
                    'product_url': None,
                    'item_id': None
                }), 500
        
        # Проверяем результат
        if not result:
            app.logger.error(f"[CHAT LISTING] Сервис вернул None для чата {chat_id}")
            return jsonify({
                'error': 'Service returned empty result',
                'product_url': None,
                'item_id': None
            }), 500
        
        # Если результат содержит ошибку, возвращаем её с соответствующим статусом
        if not result.get('success', True):
            error = result.get('error', 'Unknown error')
            status_code = 400 if 'not found' in error.lower() or 'not configured' in error.lower() else 500
            app.logger.warning(f"[CHAT LISTING] Сервис вернул ошибку: {error}")
            return jsonify(result), status_code
        
        app.logger.info(f"[CHAT LISTING] ========== ЗАГРУЗКА ОБЪЯВЛЕНИЯ ЗАВЕРШЕНА УСПЕШНО ==========")
        return jsonify(result)
        
    except ValueError as ve:
        # Ошибки валидации (чат не найден, нет product_url и т.д.)
        error_msg = str(ve)
        app.logger.warning(f"[CHAT LISTING] Ошибка валидации: {error_msg}")
        
        if error_msg == 'Chat not found':
            return jsonify({'error': 'Chat not found'}), 404
        elif error_msg == 'No product URL for this chat':
            return jsonify({'error': 'No product URL for this chat'}), 404
        elif 'Could not extract item ID' in error_msg:
            return jsonify({'error': 'Could not extract item ID from URL'}), 400
        else:
            return jsonify({'error': error_msg}), 400
            
    except RuntimeError as re:
        # Ошибки конфигурации (OAuth ключи не настроены)
        error_msg = str(re)
        app.logger.error(f"[CHAT LISTING] Ошибка конфигурации: {error_msg}")
        
        # Получаем product_url и item_id для ответа
        chat_dict, product_url, item_id = ChatListingService.get_chat_listing_info(chat_id)
        
        return jsonify({
            'error': 'OAuth keys not configured',
            'product_url': product_url,
            'item_id': item_id
        }), 400
        
    except Exception as e:
        # Прочие ошибки (API ошибки и т.д.)
        app.logger.error(f"[CHAT LISTING] ОШИБКА получения информации об объявлении: {str(e)}", exc_info=True)
        import traceback
        app.logger.error(f"[CHAT LISTING] Traceback: {traceback.format_exc()}")
        
        # Получаем product_url и item_id для ответа
        try:
            chat_dict, product_url, item_id = ChatListingService.get_chat_listing_info(chat_id)
        except Exception as info_error:
            app.logger.warning(f"[CHAT LISTING] Не удалось получить информацию о чате: {info_error}")
            product_url = None
            item_id = None
        
        return jsonify({
            'error': f'Failed to fetch listing info: {str(e)}',
            'product_url': product_url,
            'item_id': item_id,
            'details': str(e)  # Добавляем детали ошибки для отладки
        }), 500


# API для отправки сообщения
@app.route('/api/chats/<int:chat_id>/messages', methods=['POST'])
@require_auth
@handle_errors
def send_message(chat_id):
    data = request.get_json()
    
    if not data:
        return jsonify({'error': 'No data provided'}), 400
    
    message = data.get('message', '').strip()
    
    if not message:
        return jsonify({'error': 'Message is required'}), 400
    
    if len(message) > 5000:
        return jsonify({'error': 'Message too long (max 5000 characters)'}), 400
    
    conn = get_db_connection()
    try:
        # Получаем информацию о чате и магазине
        chat = conn.execute('''
            SELECT c.*, s.client_id, s.client_secret, s.user_id as shop_user_id
            FROM avito_chats c
            LEFT JOIN avito_shops s ON c.shop_id = s.id
            WHERE c.id = ?
        ''', (chat_id,)).fetchone()
        
        if not chat:
            # Соединение глобальное, не закрываем
            return jsonify({'error': 'Chat not found'}), 404
        
        # Преобразуем sqlite3.Row в словарь для безопасного доступа
        chat = dict(chat) if not isinstance(chat, dict) else chat
        
        user = get_user_by_id(session['user_id'])
        if not user:
            # Соединение глобальное, не закрываем
            return jsonify({'error': 'User not found'}), 404
        
        # Отправляем сообщение через Avito API, если есть ключи
        avito_message_sent = False
        avito_error = None
        
        # Проверяем наличие всех необходимых данных для отправки
        app.logger.info(f"[SEND MESSAGE] Проверка условий для отправки:")
        app.logger.info(f"[SEND MESSAGE]   chat_id (БД): {chat_id}")
        app.logger.info(f"[SEND MESSAGE]   client_id: {bool(chat.get('client_id'))} ({chat.get('client_id')})")
        app.logger.info(f"[SEND MESSAGE]   client_secret: {bool(chat.get('client_secret'))} ({'***' if chat.get('client_secret') else None})")
        app.logger.info(f"[SEND MESSAGE]   shop_user_id: {chat.get('shop_user_id')}")
        app.logger.info(f"[SEND MESSAGE]   avito_chat_id: {chat.get('chat_id')}")
        
        if chat.get('client_id') and chat.get('client_secret') and chat.get('shop_user_id') and chat.get('chat_id'):
            app.logger.info(f"[SEND MESSAGE] ✅ Все условия выполнены, начинаем отправку через Avito API")
            app.logger.info(f"[SEND MESSAGE] Начало отправки: chat_id={chat_id}, avito_chat_id={chat.get('chat_id')}, user_id={chat.get('shop_user_id')}, message_len={len(message)}")
            try:
                from avito_api import AvitoAPI
                app.logger.info(f"[SEND MESSAGE] Создание экземпляра AvitoAPI...")
                api = AvitoAPI(
                    client_id=chat.get('client_id'),
                    client_secret=chat.get('client_secret')
                )
                app.logger.info(f"[SEND MESSAGE] ✅ AvitoAPI создан, вызываем api.send_message...")
                app.logger.info(f"[SEND MESSAGE] Параметры: user_id={chat.get('shop_user_id')}, chat_id={chat.get('chat_id')}, message_len={len(message)}")
                avito_result = api.send_message(
                    user_id=str(chat.get('shop_user_id')),
                    chat_id=str(chat.get('chat_id')),
                    message=message
                )
                
                avito_message_sent = True
                app.logger.info(f"[SEND MESSAGE] ✅ Успешно отправлено для чата {chat_id}")
                
            except Exception as e:
                avito_error = str(e)
                # Если ошибка 405, логируем для диагностики
                if '405' in str(e) or 'HTTP 405' in str(e):
                    import traceback
                    app.logger.error("=" * 80)
                    app.logger.error(f"[SEND MESSAGE] ОШИБКА 405 ПРИ ОТПРАВКЕ СООБЩЕНИЯ")
                    app.logger.error(f"[SEND MESSAGE] Chat ID (БД): {chat_id}")
                    app.logger.error(f"[SEND MESSAGE] Avito Chat ID: {chat.get('chat_id')}")
                    app.logger.error(f"[SEND MESSAGE] User ID: {chat.get('shop_user_id')}")
                    app.logger.error(f"[SEND MESSAGE] Ошибка: {e}")
                    app.logger.error(f"[SEND MESSAGE] Тип ошибки: {type(e).__name__}")
                    app.logger.error(f"[SEND MESSAGE] Traceback:\n{traceback.format_exc()}")
                    app.logger.error("=" * 80)
                app.logger.error(f"[SEND MESSAGE] ❌ Ошибка отправки для чата {chat_id}: {e}")
                app.logger.error(f"[SEND MESSAGE] Тип ошибки: {type(e).__name__}")
                import traceback
                app.logger.error(f"[SEND MESSAGE] Traceback: {traceback.format_exc()}")
                # Продолжаем выполнение - сохраним сообщение в БД даже если Avito API не сработал
        else:
            missing = []
            if not chat.get('client_id'):
                missing.append('client_id')
            if not chat.get('client_secret'):
                missing.append('client_secret')
            if not chat.get('shop_user_id'):
                missing.append('shop_user_id')
            if not chat.get('chat_id'):
                missing.append('avito_chat_id')
            app.logger.warning(f"[SEND MESSAGE] ⚠️ Пропуск отправки через Avito API: отсутствуют {', '.join(missing)}")
            app.logger.warning(f"[SEND MESSAGE] Сообщение будет сохранено только в БД")
        
        # Добавляем сообщение в БД с manager_id (для всех пользователей - админов и менеджеров)
        # ВАЖНО: message отправляется в Avito БЕЗ подписи отправителя
        # Подпись (sender_name/manager_name) отображается только в интерфейсе для отслеживания, кто отвечает
        # Клиент в Avito НЕ видит подпись - получает только чистый текст сообщения
        manager_id = session['user_id']  # Сохраняем ID отправителя для всех пользователей
        cursor = conn.execute('''
            INSERT INTO avito_messages (chat_id, message_text, message_type, sender_name, manager_id)
            VALUES (?, ?, 'outgoing', ?, ?)
        ''', (chat_id, message, user['username'], manager_id))
        
        # Обновляем последнее сообщение в чате, назначаем менеджера если чат был в пуле
        # и сбрасываем таймер ответа (response_timer = 0) при отправке сообщения менеджером
        conn.execute('''
            UPDATE avito_chats 
            SET last_message = ?, updated_at = CURRENT_TIMESTAMP,
                assigned_manager_id = COALESCE(assigned_manager_id, ?),
                response_timer = 0
            WHERE id = ?
        ''', (message, manager_id, chat_id))
        
        # Логируем событие
        conn.execute('''
            INSERT INTO analytics_logs (event_type, user_id, chat_id, metadata)
            VALUES ('message_sent', ?, ?, ?)
        ''', (session['user_id'], chat_id, json.dumps({
            'message_length': len(message),
            'avito_sent': avito_message_sent,
            'avito_error': avito_error
        })))
        
        # Логируем действие
        log_activity(session['user_id'], 'send_message', 
                    f'Отправлено сообщение в чат ID: {chat_id}', 'chat', chat_id,
                    {'message_length': len(message), 'avito_sent': avito_message_sent})
        
        conn.commit()
        # Соединение глобальное, не закрываем
        
        # Синхронизируем сообщения после отправки, если сообщение было отправлено через Avito
        if avito_message_sent:
            try:
                from services.messenger_service import MessengerService
                conn_sync = get_db_connection()
                api_sync = AvitoAPI(
                    client_id=chat['client_id'],
                    client_secret=chat['client_secret']
                )
                service = MessengerService(conn_sync, api_sync)
                service.sync_chat_messages(
                    chat_id=chat_id,
                    user_id=str(chat['shop_user_id']),
                    avito_chat_id=str(chat['chat_id'])
                )
                conn_sync.close()
            except Exception as sync_err:
                app.logger.warning(f"Не удалось синхронизировать сообщения после отправки: {sync_err}")
        
        response_data = {'success': True, 'id': cursor.lastrowid}
        if avito_message_sent:
            response_data['avito_sent'] = True
        elif avito_error:
            # Для ошибки 405 добавляем более детальную информацию
            if '405' in str(avito_error) or 'HTTP 405' in str(avito_error):
                response_data['warning'] = f'Сообщение сохранено в БД, но не отправлено в Avito: Ошибка API запроса: {avito_error}'
                response_data['error_type'] = '405'
                response_data['error_details'] = {
                    'chat_id': chat_id,
                    'avito_chat_id': chat.get('chat_id'),
                    'user_id': chat.get('shop_user_id'),
                    'message': 'Метод не разрешен для этого endpoint. Проверьте логи сервера для деталей.'
                }
            else:
                response_data['warning'] = f'Сообщение сохранено в БД, но не отправлено в Avito: {avito_error}'
        
        return jsonify(response_data), 201
    except Exception as e:
        # Соединение глобальное, не закрываем
        app.logger.error(f"Ошибка отправки сообщения: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 400


# API для загрузки изображений
@app.route('/api/upload/image', methods=['POST'])
@require_auth
@handle_errors
def upload_image():
    """Загрузка изображения для отправки в чат"""
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    
    # Проверяем тип файла
    if not file.content_type or not file.content_type.startswith('image/'):
        return jsonify({'error': 'File must be an image'}), 400
    
    # Проверяем размер (24 МБ максимум для изображений)
    file.seek(0, os.SEEK_END)
    file_size = file.tell()
    file.seek(0)
    max_size = 24 * 1024 * 1024  # 24 МБ
    if file_size > max_size:
        return jsonify({'error': f'File too large (max {max_size // 1024 // 1024} MB)'}), 400
    
    try:
        # Сохраняем файл временно
        import tempfile
        import uuid
        temp_dir = os.path.join(os.path.dirname(__file__), 'temp_uploads')
        os.makedirs(temp_dir, exist_ok=True)
        
        file_ext = os.path.splitext(file.filename)[1] or '.jpg'
        temp_filename = f"{uuid.uuid4()}{file_ext}"
        temp_path = os.path.join(temp_dir, temp_filename)
        
        file.save(temp_path)
        
        # Получаем информацию о текущем чате из сессии или параметров
        # Для загрузки изображения нужен user_id магазина
        # Получаем его из последнего выбранного чата или из параметров запроса
        shop_user_id = request.form.get('user_id')
        
        if not shop_user_id:
            # Пытаемся получить из сессии или последнего чата
            conn = get_db_connection()
            try:
                # Получаем последний активный чат пользователя
                last_chat = conn.execute('''
                    SELECT s.user_id as shop_user_id, s.client_id, s.client_secret
                    FROM avito_chats c
                    LEFT JOIN avito_shops s ON c.shop_id = s.id
                    WHERE c.id IN (
                        SELECT id FROM avito_chats 
                        ORDER BY updated_at DESC LIMIT 1
                    )
                    LIMIT 1
                ''').fetchone()
                
                if last_chat:
                    last_chat = dict(last_chat) if not isinstance(last_chat, dict) else last_chat
                    shop_user_id = last_chat.get('shop_user_id')
                    client_id = last_chat.get('client_id')
                    client_secret = last_chat.get('client_secret')
                else:
                    return jsonify({'error': 'No shop user_id found. Please select a chat first.'}), 400
            finally:
                pass  # Соединение глобальное, не закрываем
        else:
            # Получаем client_id и client_secret для этого user_id
            conn = get_db_connection()
            try:
                shop = conn.execute('''
                    SELECT client_id, client_secret, user_id
                    FROM avito_shops
                    WHERE user_id = ?
                    LIMIT 1
                ''', (shop_user_id,)).fetchone()
                
                if not shop:
                    return jsonify({'error': 'Shop not found'}), 404
                
                shop = dict(shop) if not isinstance(shop, dict) else shop
                client_id = shop.get('client_id')
                client_secret = shop.get('client_secret')
            finally:
                pass  # Соединение глобальное, не закрываем
        
        if not client_id or not client_secret:
            return jsonify({'error': 'Avito API credentials not configured for this shop'}), 400
        
        # Загружаем изображение через Avito API
        from avito_api import AvitoAPI
        api = AvitoAPI(client_id, client_secret)
        
        upload_results = api.upload_images(str(shop_user_id), [temp_path])
        
        # Удаляем временный файл
        try:
            os.remove(temp_path)
        except:
            pass
        
        if not upload_results or len(upload_results) == 0:
            return jsonify({'error': 'Failed to upload image'}), 500
        
        upload_result = upload_results[0]
        image_id = upload_result.get('id') or upload_result.get('image_id') or upload_result.get('attachment_id')
        
        if not image_id:
            return jsonify({'error': 'Failed to get image_id from upload response'}), 500
        
        return jsonify({
            'success': True,
            'image_id': str(image_id),
            'upload_result': upload_result
        }), 200
        
    except Exception as e:
        app.logger.error(f"Ошибка загрузки изображения: {e}")
        import traceback
        traceback.print_exc()
        # Удаляем временный файл при ошибке
        try:
            if 'temp_path' in locals():
                os.remove(temp_path)
        except:
            pass
        return jsonify({'error': str(e)}), 500


# API для загрузки медиа файлов (аудио, видео, документы)
@app.route('/api/upload/media', methods=['POST'])
@require_auth
@handle_errors
def upload_media():
    """Загрузка медиа файла для отправки в чат"""
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    
    file_type = request.form.get('file_type', 'document')
    
    # Проверяем размер (50 МБ максимум для медиа)
    file.seek(0, os.SEEK_END)
    file_size = file.tell()
    file.seek(0)
    max_size = 50 * 1024 * 1024  # 50 МБ
    if file_size > max_size:
        return jsonify({'error': f'File too large (max {max_size // 1024 // 1024} MB)'}), 400
    
    try:
        # Сохраняем файл временно
        import tempfile
        import uuid
        temp_dir = os.path.join(os.path.dirname(__file__), 'temp_uploads')
        os.makedirs(temp_dir, exist_ok=True)
        
        file_ext = os.path.splitext(file.filename)[1] or ''
        temp_filename = f"{uuid.uuid4()}{file_ext}"
        temp_path = os.path.join(temp_dir, temp_filename)
        
        file.save(temp_path)
        
        # Получаем user_id магазина
        shop_user_id = request.form.get('user_id')
        
        if not shop_user_id:
            conn = get_db_connection()
            try:
                last_chat = conn.execute('''
                    SELECT s.user_id as shop_user_id, s.client_id, s.client_secret
                    FROM avito_chats c
                    LEFT JOIN avito_shops s ON c.shop_id = s.id
                    WHERE c.id IN (
                        SELECT id FROM avito_chats 
                        ORDER BY updated_at DESC LIMIT 1
                    )
                    LIMIT 1
                ''').fetchone()
                
                if last_chat:
                    last_chat = dict(last_chat) if not isinstance(last_chat, dict) else last_chat
                    shop_user_id = last_chat.get('shop_user_id')
                    client_id = last_chat.get('client_id')
                    client_secret = last_chat.get('client_secret')
                else:
                    return jsonify({'error': 'No shop user_id found. Please select a chat first.'}), 400
            finally:
                pass
        else:
            conn = get_db_connection()
            try:
                shop = conn.execute('''
                    SELECT client_id, client_secret, user_id
                    FROM avito_shops
                    WHERE user_id = ?
                    LIMIT 1
                ''', (shop_user_id,)).fetchone()
                
                if not shop:
                    return jsonify({'error': 'Shop not found'}), 404
                
                shop = dict(shop) if not isinstance(shop, dict) else shop
                client_id = shop.get('client_id')
                client_secret = shop.get('client_secret')
            finally:
                pass
        
        if not client_id or not client_secret:
            return jsonify({'error': 'Avito API credentials not configured for this shop'}), 400
        
        # Загружаем медиа через Avito API
        from avito_api import AvitoAPI
        api = AvitoAPI(client_id, client_secret)
        
        upload_result = api.upload_media(str(shop_user_id), temp_path, file_type=file_type)
        
        # Удаляем временный файл
        try:
            os.remove(temp_path)
        except:
            pass
        
        attachment_id = upload_result.get('attachment_id') or upload_result.get('id')
        
        if not attachment_id:
            return jsonify({'error': 'Failed to get attachment_id from upload response'}), 500
        
        return jsonify({
            'success': True,
            'attachment_id': str(attachment_id),
            'upload_result': upload_result
        }), 200
        
    except Exception as e:
        app.logger.error(f"Ошибка загрузки медиа: {e}")
        import traceback
        traceback.print_exc()
        try:
            if 'temp_path' in locals():
                os.remove(temp_path)
        except:
            pass
        return jsonify({'error': str(e)}), 500


# API для отправки изображения в чат
@app.route('/api/chats/<int:chat_id>/messages/image', methods=['POST'])
@require_auth
@handle_errors
def send_image_message(chat_id):
    """Отправка изображения в чат"""
    data = request.get_json()
    
    if not data:
        return jsonify({'error': 'No data provided'}), 400
    
    image_id = data.get('image_id')
    if not image_id:
        return jsonify({'error': 'image_id is required'}), 400
    
    conn = get_db_connection()
    try:
        # Получаем информацию о чате и магазине
        chat = conn.execute('''
            SELECT c.*, s.client_id, s.client_secret, s.user_id as shop_user_id
            FROM avito_chats c
            LEFT JOIN avito_shops s ON c.shop_id = s.id
            WHERE c.id = ?
        ''', (chat_id,)).fetchone()
        
        if not chat:
            return jsonify({'error': 'Chat not found'}), 404
        
        chat = dict(chat) if not isinstance(chat, dict) else chat
        
        if not chat.get('client_id') or not chat.get('client_secret'):
            return jsonify({'error': 'Avito API credentials not configured for this shop'}), 400
        
        # Отправляем изображение через Avito API
        from avito_api import AvitoAPI
        api = AvitoAPI(chat['client_id'], chat['client_secret'])
        
        result = api.send_image_message_direct(
            user_id=str(chat['shop_user_id']),
            chat_id=str(chat['chat_id']),
            image_id=str(image_id)
        )
        
        # Сохраняем сообщение в БД
        cursor = conn.execute('''
            INSERT INTO avito_messages 
            (chat_id, message_text, sender_type, created_at, avito_message_id, avito_sent)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            chat_id,
            '[Изображение]',
            'manager',
            datetime.now(timezone.utc),
            result.get('id') or result.get('message_id'),
            True
        ))
        
        conn.commit()
        
        # Логируем действие
        log_activity(session['user_id'], 'send_image', 
                    f'Отправлено изображение в чат ID: {chat_id}', 'chat', chat_id)
        
        return jsonify({
            'success': True,
            'id': cursor.lastrowid,
            'avito_sent': True,
            'result': result
        }), 201
        
    except Exception as e:
        app.logger.error(f"Ошибка отправки изображения: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 400
    finally:
        pass  # Соединение глобальное, не закрываем


# API для получения шаблонов ответов
@app.route('/api/templates')
def get_templates():
    if 'user_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401

    conn = get_db_connection()
    templates = conn.execute('''
        SELECT id, name, content, category, created_by, is_active, created_at 
        FROM message_templates 
        WHERE is_active = 1 
        ORDER BY category, name
    ''').fetchall()
            # Соединение глобальное, не закрываем
    
    return jsonify([dict(template) for template in templates])


# API для получения быстрых ответов
@app.route('/api/quick-replies')
def get_quick_replies():
    if 'user_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401

    conn = get_db_connection()
    replies = conn.execute('''
        SELECT id, shortcut, message, created_by, is_active, created_at 
        FROM quick_replies 
        WHERE is_active = 1 
        ORDER BY shortcut
    ''').fetchall()
            # Соединение глобальное, не закрываем
    
    return jsonify([dict(reply) for reply in replies])

# API для получения всех быстрых ответов (включая неактивные)
@app.route('/api/quick-replies/all')
@require_auth
@handle_errors
def get_all_quick_replies():
    """Получение всех быстрых ответов (для управления)"""
    conn = get_db_connection()
    replies = conn.execute('''
        SELECT id, shortcut, message, created_by, is_active, created_at 
        FROM quick_replies 
        ORDER BY is_active DESC, shortcut
    ''').fetchall()
            # Соединение глобальное, не закрываем
    
    return jsonify([dict(reply) for reply in replies])

# API для создания быстрого ответа
@app.route('/api/quick-replies', methods=['POST'])
@require_auth
@handle_errors
def create_quick_reply():
    """Создание быстрого ответа"""
    data = request.get_json()
    
    if not data:
        return jsonify({'error': 'No data provided'}), 400
    
    shortcut = data.get('shortcut', '').strip()
    message = data.get('message', '').strip()
    
    if not shortcut or not message:
        return jsonify({'error': 'Shortcut and message are required'}), 400
    
    # Убираем "/" если он есть в начале
    if shortcut.startswith('/'):
        shortcut = shortcut[1:]
    
    conn = get_db_connection()
    try:
        cursor = conn.execute('''
            INSERT INTO quick_replies (shortcut, message, created_by, is_active)
            VALUES (?, ?, ?, ?)
        ''', (shortcut, message, session['user_id'], True))
        reply_id = cursor.lastrowid
        
        log_activity(session['user_id'], 'create_quick_reply', 
                    f'Создан быстрый ответ: {shortcut}', 'quick_reply', reply_id)
        
        conn.commit()
        # Соединение глобальное, не закрываем
        return jsonify({'success': True, 'id': reply_id}), 201
    except Exception as e:
        # Соединение глобальное, не закрываем
        if 'UNIQUE constraint' in str(e):
            return jsonify({'error': 'Quick reply with this shortcut already exists'}), 400
        return jsonify({'error': str(e)}), 400

# API для обновления быстрого ответа
@app.route('/api/quick-replies/<int:reply_id>', methods=['PUT'])
@require_auth
@handle_errors
def update_quick_reply(reply_id):
    """Обновление быстрого ответа"""
    data = request.get_json()
    conn = get_db_connection()
    
    try:
        update_fields = []
        update_values = []
        
        if 'shortcut' in data:
            shortcut = data['shortcut'].strip()
            # Убираем "/" если он есть в начале
            if shortcut.startswith('/'):
                shortcut = shortcut[1:]
            update_fields.append('shortcut = ?')
            update_values.append(shortcut)
        
        if 'message' in data:
            update_fields.append('message = ?')
            update_values.append(data['message'].strip())
        
        if 'is_active' in data:
            update_fields.append('is_active = ?')
            update_values.append(data['is_active'])
        
        if update_fields:
            update_values.append(reply_id)
            query = f'UPDATE quick_replies SET {", ".join(update_fields)} WHERE id = ?'
            conn.execute(query, tuple(update_values))
            
            log_activity(session['user_id'], 'update_quick_reply', 
                        f'Обновлен быстрый ответ ID: {reply_id}', 'quick_reply', reply_id)
            
            conn.commit()
        
        # Соединение глобальное, не закрываем
        return jsonify({'success': True}), 200
    except Exception as e:
        # Соединение глобальное, не закрываем
        return jsonify({'error': str(e)}), 400

# API для удаления быстрого ответа
@app.route('/api/quick-replies/<int:reply_id>', methods=['DELETE'])
@require_auth
@handle_errors
def delete_quick_reply(reply_id):
    """Удаление быстрого ответа (деактивация)"""
    conn = get_db_connection()
    try:
        conn.execute('UPDATE quick_replies SET is_active = 0 WHERE id = ?', (reply_id,))
        
        log_activity(session['user_id'], 'delete_quick_reply', 
                    f'Удален быстрый ответ ID: {reply_id}', 'quick_reply', reply_id)
        
        conn.commit()
        # Соединение глобальное, не закрываем
        return jsonify({'success': True}), 200
    except Exception as e:
        # Соединение глобальное, не закрываем
        return jsonify({'error': str(e)}), 400


# ==================== МОДУЛЬ АНАЛИТИКИ ====================

# API для получения аналитики
@app.route('/api/analytics')
def get_analytics():
    if 'user_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401

    conn = get_db_connection()
    user_id = session['user_id']
    role = session.get('user_role')
    
    # Статистика ответов
    if role == 'admin':
        response_stats = conn.execute('''
            SELECT 
                AVG(response_timer) as avg_response_time,
                COUNT(*) as total_chats,
                SUM(CASE WHEN priority = 'urgent' THEN 1 ELSE 0 END) as urgent_count
            FROM avito_chats
        ''').fetchone()
    else:
        response_stats = conn.execute('''
            SELECT 
                AVG(response_timer) as avg_response_time,
                COUNT(*) as total_chats,
                SUM(CASE WHEN priority = 'urgent' THEN 1 ELSE 0 END) as urgent_count
            FROM avito_chats
            WHERE assigned_manager_id = ?
        ''', (user_id,)).fetchone()
    
    # KPI менеджеров
    if role == 'admin':
        kpi_stats = conn.execute('''
            SELECT u.id, u.username, u.kpi_score, 
                   COUNT(DISTINCT c.id) as total_chats,
                   AVG(c.response_timer) as avg_response_time
            FROM users u
            LEFT JOIN avito_chats c ON u.id = c.assigned_manager_id
            WHERE u.role = 'manager'
            GROUP BY u.id
        ''').fetchall()
    else:
        kpi_stats = conn.execute('''
            SELECT u.id, u.username, u.kpi_score, 
                   COUNT(DISTINCT c.id) as total_chats,
                   AVG(c.response_timer) as avg_response_time
            FROM users u
            LEFT JOIN avito_chats c ON u.id = c.assigned_manager_id
            WHERE u.id = ?
            GROUP BY u.id
        ''', (user_id,)).fetchall()
    
    # Конверсия в заказы
    conversion_stats = conn.execute('''
        SELECT 
            COUNT(DISTINCT c.id) as total_chats,
            COUNT(DISTINCT o.id) as total_orders,
            ROUND(COUNT(DISTINCT o.id) * 100.0 / COUNT(DISTINCT c.id), 2) as conversion_rate
        FROM avito_chats c
        LEFT JOIN client_orders o ON c.id = o.chat_id
    ''').fetchone()
    
            # Соединение глобальное, не закрываем
    
    return jsonify({
        'response_stats': dict(response_stats),
        'kpi_stats': [dict(stat) for stat in kpi_stats],
        'conversion_stats': dict(conversion_stats)
    })


# ==================== МОДУЛЬ АВТОМАТИЗАЦИИ ====================

# API для получения правил автоматизации
@app.route('/api/automation')
def get_automation_rules():
    if 'user_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401

    conn = get_db_connection()
    rules = conn.execute('''
        SELECT id, name, trigger_type, trigger_condition, action_type, action_data, is_active, created_by, created_at
        FROM automation_rules 
        WHERE is_active = 1 
        ORDER BY created_at DESC
    ''').fetchall()
            # Соединение глобальное, не закрываем
    
    return jsonify([dict(rule) for rule in rules])


# API для создания правила автоматизации
@app.route('/api/automation', methods=['POST'])
def create_automation_rule():
    if 'user_id' not in session or session.get('user_role') != 'admin':
        return jsonify({'error': 'Access denied'}), 403

    data = request.get_json()
    conn = get_db_connection()
    
    try:
        cursor = conn.execute('''
            INSERT INTO automation_rules (name, trigger_type, trigger_condition, action_type, action_data, created_by)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (data.get('name'), data.get('trigger_type'), json.dumps(data.get('trigger_condition')),
              data.get('action_type'), json.dumps(data.get('action_data')), session['user_id']))
        conn.commit()
        # Соединение глобальное, не закрываем
        return jsonify({'success': True, 'id': cursor.lastrowid}), 201
    except Exception as e:
        # Соединение глобальное, не закрываем
        return jsonify({'error': str(e)}), 400


# ==================== МОДУЛЬ WEBHOOK ====================

# Тестовый endpoint для проверки доступности webhook
@app.route('/webhook/avito', methods=['GET'])
def webhook_test():
    """Тестовый endpoint для проверки доступности webhook"""
    return jsonify({
        'status': 'ok',
        'message': 'Webhook endpoint is accessible',
        'url': 'https://osagaming.store/webhook/avito',
        'timestamp': datetime.now(timezone.utc).isoformat()
    }), 200

@app.route('/webhook/avito', methods=['POST'])
def avito_webhook():
    """
    Обработчик webhook от Авито

    Получает уведомления о новых сообщениях, изменениях в чатах и т.д.
    Автоматически синхронизирует чаты при получении уведомлений.
    """
    try:
        from avito_api import AvitoAPI

        # Получаем данные webhook
        data = request.get_json()
        if not data:
            return jsonify({'error': 'No data'}), 400

        app.logger.info(f"[WEBHOOK] Получен webhook от Авито: {data}")

        # Проверяем подпись если есть secret
        signature = request.headers.get('X-Signature')
        if signature:
            # Получаем client_secret из магазина
            # Это нужно для проверки подписи
            pass

        # Обрабатываем структуру webhook v3 согласно документации
        # Структура: { "id": "...", "version": "3", "timestamp": "...", "payload": { "type": "...", "value": {...} } }
        payload = data.get('payload', {})
        if not payload:
            # Fallback на старую структуру для обратной совместимости
            event_type = data.get('type')
            event_data = data.get('data', {})
        else:
            # Новая структура v3
            event_type = payload.get('type')
            event_data = payload.get('value', {})
            
            # Нормализуем тип события
            if event_type in ['new_message', 'message']:
                event_type = 'message'
            elif event_type in ['chat_update', 'chat']:
                event_type = 'chat'
        
        app.logger.info(f"[WEBHOOK] Тип события: {event_type}, данные: {event_data}")

        conn = get_db_connection()
        
        if event_type in ['message', 'new_message']:
            # Новое сообщение
            # Согласно документации v3: payload.value.chat_id, payload.value.user_id
            avito_chat_id = event_data.get('chat_id')
            user_id = event_data.get('user_id')
            message_id = event_data.get('id')
            message_text = None
            
            # Получаем текст сообщения из разных возможных структур
            if 'content' in event_data:
                content = event_data.get('content', {})
                if isinstance(content, dict):
                    message_text = content.get('text') or content.get('message')
                else:
                    message_text = str(content)
            elif 'text' in event_data:
                message_text = event_data.get('text')
            elif 'message' in event_data:
                message_data = event_data.get('message', {})
                if isinstance(message_data, dict):
                    message_text = message_data.get('text') or message_data.get('content', {}).get('text')
            
            app.logger.info(f"[WEBHOOK] Обработка сообщения: chat_id={avito_chat_id}, user_id={user_id}, message_id={message_id}")

            # Находим магазин по user_id
            shop = conn.execute('''
                SELECT id, client_id, client_secret, user_id 
                FROM avito_shops 
                WHERE user_id = ?
            ''', (user_id,)).fetchone()
            
            if shop:
                shop_dict = dict(shop)
                # Находим чат в базе
                chat = conn.execute('''
                    SELECT id FROM avito_chats 
                    WHERE shop_id = ? AND chat_id = ?
                ''', (shop_dict['id'], avito_chat_id)).fetchone()
                
                if not chat:
                    # Если чат не найден, синхронизируем все чаты для этого магазина
                    # чтобы создать новый чат, если он появился
                    app.logger.info(f"[WEBHOOK] Чат {avito_chat_id} не найден в БД, синхронизируем все чаты магазина {shop_dict['id']}")
                    try:
                        sync_result = sync_chats_from_avito(shop_id=shop_dict['id'])
                        app.logger.info(f"[WEBHOOK] Синхронизированы чаты для магазина {shop_dict['id']} после webhook: создано/обновлено {sync_result.get('synced_count', 0)} чатов")
                        
                        # После синхронизации проверяем, появился ли чат
                        chat = conn.execute('''
                            SELECT id FROM avito_chats 
                            WHERE shop_id = ? AND chat_id = ?
                        ''', (shop_dict['id'], avito_chat_id)).fetchone()
                        
                        if chat:
                            app.logger.info(f"[WEBHOOK] Новый чат {avito_chat_id} успешно создан после синхронизации (БД ID: {chat['id']})")
                            # Синхронизируем сообщения для нового чата
                            try:
                                from services.messenger_service import MessengerService
                                api = AvitoAPI(client_id=shop_dict['client_id'], client_secret=shop_dict['client_secret'])
                                service = MessengerService(conn, api)
                                new_messages_count = service.sync_chat_messages(
                                    chat_id=chat['id'],
                                    user_id=str(shop_dict['user_id']),
                                    avito_chat_id=avito_chat_id
                                )
                                app.logger.info(f"[WEBHOOK] Синхронизировано {new_messages_count} сообщений для нового чата {avito_chat_id}")
                            except Exception as msg_sync_err:
                                app.logger.error(f"[WEBHOOK] Ошибка синхронизации сообщений для нового чата: {msg_sync_err}", exc_info=True)
                    except Exception as sync_err:
                        app.logger.error(f"[WEBHOOK] Ошибка синхронизации чатов: {sync_err}", exc_info=True)
                elif chat:
                    # Синхронизируем сообщения для этого чата используя MessengerService
                    try:
                        from services.messenger_service import MessengerService
                        
                        api = AvitoAPI(client_id=shop_dict['client_id'], client_secret=shop_dict['client_secret'])
                        service = MessengerService(conn, api)
                        
                        # Синхронизируем сообщения для чата
                        new_messages_count = service.sync_chat_messages(
                            chat_id=chat['id'],
                            user_id=str(shop_dict['user_id']),
                            avito_chat_id=avito_chat_id
                        )
                        
                        # Получаем последнее сообщение для обновления last_message
                        last_message_row = conn.execute('''
                            SELECT message_text FROM avito_messages 
                            WHERE chat_id = ? 
                            ORDER BY timestamp DESC LIMIT 1
                        ''', (chat['id'],)).fetchone()
                        
                        last_message_text = last_message_row['message_text'] if last_message_row else None
                        
                        # Обновляем updated_at, unread_count и last_message для чата
                        # Используем более точный расчет непрочитанных сообщений
                        conn.execute('''
                            UPDATE avito_chats 
                            SET updated_at = CURRENT_TIMESTAMP,
                                unread_count = (
                                    SELECT COUNT(*) FROM avito_messages 
                                    WHERE chat_id = ? AND message_type = 'incoming' 
                                    AND id > COALESCE((
                                        SELECT MAX(id) FROM avito_messages 
                                        WHERE chat_id = ? AND message_type = 'outgoing'
                                    ), 0)
                                ),
                                last_message = COALESCE(?, last_message),
                                last_message_time = (
                                    SELECT MAX(timestamp) FROM avito_messages 
                                    WHERE chat_id = ?
                                )
                            WHERE id = ?
                        ''', (chat['id'], chat['id'], last_message_text, chat['id'], chat['id']))
                        
                        conn.commit()
                        app.logger.info(f"[WEBHOOK] Синхронизировано {new_messages_count} новых сообщений для чата {avito_chat_id} (БД ID: {chat['id']})")
                        
                        # Принудительно обновляем timestamp для более быстрого обнаружения изменений
                        conn.execute('''
                            UPDATE avito_chats 
                            SET updated_at = datetime('now', 'localtime')
                            WHERE id = ?
                        ''', (chat['id'],))
                        conn.commit()
                        
                        # Обновляем счетчик непрочитанных для всех менеджеров, которые видят этот чат
                        conn.execute('''
                            UPDATE avito_chats 
                            SET unread_count = (
                                SELECT COUNT(*) FROM avito_messages 
                                WHERE chat_id = ? AND message_type = 'incoming' 
                                AND id > COALESCE((
                                    SELECT MAX(id) FROM avito_messages 
                                    WHERE chat_id = ? AND message_type = 'outgoing'
                                ), 0)
                            )
                            WHERE id = ?
                        ''', (chat['id'], chat['id'], chat['id']))
                        conn.commit()
                        
                    except Exception as sync_err:
                        app.logger.error(f"[WEBHOOK] Ошибка синхронизации сообщений для чата {avito_chat_id}: {sync_err}", exc_info=True)
                        conn.rollback()

            # Логируем в базу
            log_activity(
                user_id='system',
                action_type='webhook_message',
                action_description=f'Новое сообщение в чате {avito_chat_id}',
                target_type='chat',
                target_id=avito_chat_id if 'chat' in locals() else None,
                metadata={'webhook_data': data}
            )
            
            # Возвращаем успешный ответ
            return jsonify({
                'status': 'ok',
                'message': 'Webhook processed successfully',
                'chat_id': avito_chat_id,
                'synced_messages': new_messages_count if 'new_messages_count' in locals() else 0
            }), 200

        elif event_type in ['chat', 'chat_update']:
            # Изменение в чате - синхронизируем конкретный чат или все чаты магазина
            # Согласно документации v3: payload.value.chat_id, payload.value.user_id
            avito_chat_id = event_data.get('chat_id')
            user_id = event_data.get('user_id')
            
            app.logger.info(f"[WEBHOOK] Обработка обновления чата: chat_id={avito_chat_id}, user_id={user_id}")
            
            if user_id:
                try:
                    shop = conn.execute('''
                        SELECT id, client_id, client_secret, user_id FROM avito_shops WHERE user_id = ?
                    ''', (user_id,)).fetchone()
                    
                    if shop:
                        shop_dict = dict(shop)
                        
                        # Если указан конкретный chat_id, синхронизируем только его
                        if avito_chat_id:
                            chat = conn.execute('''
                                SELECT id FROM avito_chats 
                                WHERE shop_id = ? AND chat_id = ?
                            ''', (shop_dict['id'], avito_chat_id)).fetchone()
                            
                            if chat:
                                # Для обновления конкретного чата используем полную синхронизацию
                                # которая правильно сохранит listing_data из context.value
                                try:
                                    sync_result = sync_chats_from_avito(shop_id=shop_dict['id'])
                                    app.logger.info(f"[WEBHOOK] Синхронизирован чат {avito_chat_id} для магазина {shop_dict['id']}: создано/обновлено {sync_result.get('synced_count', 0)} чатов")
                                except Exception as chat_sync_err:
                                    app.logger.error(f"[WEBHOOK] Ошибка синхронизации чата {avito_chat_id}: {chat_sync_err}", exc_info=True)
                            else:
                                # Чат не найден - возможно это новый чат, синхронизируем все чаты магазина
                                app.logger.info(f"[WEBHOOK] Чат {avito_chat_id} не найден в БД, синхронизируем все чаты магазина {shop_dict['id']} для создания нового")
                                try:
                                    sync_result = sync_chats_from_avito(shop_id=shop_dict['id'])
                                    app.logger.info(f"[WEBHOOK] Синхронизированы чаты для магазина {shop_dict['id']} после webhook: создано/обновлено {sync_result.get('synced_count', 0)} чатов")
                                except Exception as sync_err:
                                    app.logger.error(f"[WEBHOOK] Ошибка синхронизации чатов: {sync_err}", exc_info=True)
                        else:
                            # Если chat_id не указан, синхронизируем все чаты магазина
                            sync_result = sync_chats_from_avito(shop_id=shop_dict['id'])
                            app.logger.info(f"[WEBHOOK] Синхронизированы все чаты для магазина {shop_dict['id']} после webhook: создано/обновлено {sync_result.get('synced_count', 0)} чатов")
                except Exception as sync_err:
                    app.logger.error(f"[WEBHOOK] Ошибка синхронизации чатов после webhook: {sync_err}", exc_info=True)

            log_activity(
                user_id='system',
                action_type='webhook_chat_update',
                action_description=f'Обновление чата {avito_chat_id or "всех чатов"}',
                target_type='chat',
                target_id=avito_chat_id,
                metadata={'webhook_data': data}
            )
        
        else:
            # Неизвестный тип события - логируем для отладки
            app.logger.warning(f"[WEBHOOK] Неизвестный тип события: {event_type}, данные: {data}")
            log_activity(
                user_id='system',
                action_type='webhook_unknown',
                action_description=f'Неизвестное событие webhook: {event_type}',
                target_type='system',
                target_id=None,
                metadata={'webhook_data': data}
            )

        # Соединение глобальное, не закрываем
        return jsonify({'status': 'ok'}), 200

    except Exception as e:
        app.logger.error(f"Ошибка обработки webhook: {e}", exc_info=True)
        return jsonify({'error': 'Internal error'}), 500

# ==================== МОДУЛЬ KPI И ШТРАФОВ ====================

# API для получения KPI менеджера
@app.route('/api/kpi/<int:user_id>')
def get_manager_kpi(user_id):
    if 'user_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401

    # Проверяем права доступа
    if session.get('user_role') != 'admin' and session['user_id'] != user_id:
        return jsonify({'error': 'Access denied'}), 403

    conn = get_db_connection()
    
    # Получаем настройки KPI
    kpi_settings = conn.execute('SELECT id, parameter_name, weight, min_value, penalty_amount, bonus_amount, created_at FROM kpi_settings').fetchall()
    
    # Получаем историю KPI
    kpi_history = conn.execute('''
        SELECT id, user_id, period_start, period_end, response_time_avg, conversion_rate, 
               customer_satisfaction, messages_per_chat, total_score, bonus_amount, penalty_amount, created_at
        FROM kpi_history 
        WHERE user_id = ? 
        ORDER BY period_end DESC 
        LIMIT 12
    ''', (user_id,)).fetchall()
    
    # Получаем текущие показатели
    user = conn.execute('SELECT kpi_score FROM users WHERE id = ?', (user_id,)).fetchone()
    
            # Соединение глобальное, не закрываем
    
    return jsonify({
        'settings': [dict(setting) for setting in kpi_settings],
        'history': [dict(record) for record in kpi_history],
        'current_score': user['kpi_score'] if user else 0
    })


# ==================== МОДУЛЬ НАСТРОЕК СИСТЕМЫ ====================

# API для получения настроек системы
@app.route('/api/settings')
def get_system_settings():
    if 'user_id' not in session or session.get('user_role') != 'admin':
        return jsonify({'error': 'Access denied'}), 403

    conn = get_db_connection()
    settings = conn.execute('SELECT id, setting_key, setting_value, setting_type, description, updated_at FROM system_settings').fetchall()
            # Соединение глобальное, не закрываем
    
    settings_dict = {}
    for setting in settings:
        value = setting['setting_value']
        if setting['setting_type'] == 'number':
            value = float(value) if '.' in value else int(value)
        elif setting['setting_type'] == 'boolean':
            value = value.lower() == 'true'
        elif setting['setting_type'] == 'json':
            value = json.loads(value)
        settings_dict[setting['setting_key']] = value
    
    return jsonify(settings_dict)


# API для обновления настроек системы
@app.route('/api/settings', methods=['PUT'])
def update_system_settings():
    # Разрешаем доступ для admin и super_admin
    user_role = session.get('user_role')
    if user_role not in ['admin', 'super_admin']:
        return jsonify({'error': 'Access denied'}), 403

    data = request.get_json() or {}
    if not isinstance(data, dict):
        return jsonify({'error': 'Invalid payload'}), 400
    conn = get_db_connection()
    
    try:
        for key, value in data.items():
            setting_type = 'string'
            if isinstance(value, bool):
                setting_type = 'boolean'
                value = 'true' if value else 'false'
            elif isinstance(value, (int, float)):
                setting_type = 'number'
                value = str(value)
            elif isinstance(value, dict):
                setting_type = 'json'
                value = json.dumps(value)
            
            conn.execute('''
                UPDATE system_settings 
                SET setting_value = ?, setting_type = ?, updated_at = CURRENT_TIMESTAMP
                WHERE setting_key = ?
            ''', (str(value), setting_type, key))
        
        conn.commit()
        # Соединение глобальное, не закрываем
        return jsonify({'success': True}), 200
    except Exception as e:
        # Соединение глобальное, не закрываем
        return jsonify({'error': str(e)}), 400


# API для получения профиля пользователя
@app.route('/api/user/profile')
@require_auth
@handle_errors
def get_user_profile():
    """Получение профиля текущего пользователя"""
    if 'user_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401

    conn = get_db_connection()
    try:
        has_name_cols = check_name_columns(conn)
        if has_name_cols:
            user = conn.execute('''
                SELECT id, username, email, first_name, last_name, role, is_active, created_at
                FROM users
                WHERE id = ?
            ''', (session['user_id'],)).fetchone()
        else:
            user = conn.execute('''
                SELECT id, username, email, role, is_active, created_at
                FROM users
                WHERE id = ?
            ''', (session['user_id'],)).fetchone()
        
        if not user:
            return jsonify({'error': 'User not found'}), 404
        
        user_dict = dict(user)
        # Соединение глобальное, не закрываем
        return jsonify(user_dict), 200
    except Exception as e:
        app.logger.error(f'[GET USER PROFILE] Ошибка: {e}', exc_info=True)
        # Соединение глобальное, не закрываем
        return jsonify({'error': str(e)}), 500


# API для обновления профиля пользователя
@app.route('/api/user/profile', methods=['PUT'])
@require_auth
@handle_errors
def update_user_profile():
    """Обновление профиля текущего пользователя"""
    if 'user_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401

    data = request.get_json() or {}
    if not isinstance(data, dict):
        return jsonify({'error': 'Invalid payload'}), 400

    username = (data.get('username') or '').strip()
    email = (data.get('email') or '').strip()
    first_name = (data.get('first_name') or '').strip() or None
    last_name = (data.get('last_name') or '').strip() or None

    if not username or not email:
        return jsonify({'error': 'Username and email are required'}), 400

    # Проверка формата email
    import re
    email_pattern = r'^[^\s@]+@[^\s@]+\.[^\s@]+$'
    if not re.match(email_pattern, email):
        return jsonify({'error': 'Invalid email format'}), 400

    conn = get_db_connection()
    try:
        # Проверяем, не занят ли email другим пользователем
        existing = conn.execute('''
            SELECT id FROM users WHERE email = ? AND id != ?
        ''', (email, session['user_id'])).fetchone()
        
        if existing:
            return jsonify({'error': 'Email already in use'}), 400

        # Проверяем, не занят ли username другим пользователем
        existing_username = conn.execute('''
            SELECT id FROM users WHERE username = ? AND id != ?
        ''', (username, session['user_id'])).fetchone()
        
        if existing_username:
            return jsonify({'error': 'Username already in use'}), 400

        # Обновляем профиль
        has_name_cols = check_name_columns(conn)
        if has_name_cols:
            conn.execute('''
                UPDATE users 
                SET username = ?, email = ?, first_name = ?, last_name = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (username, email, first_name, last_name, session['user_id']))
        else:
            conn.execute('''
                UPDATE users 
                SET username = ?, email = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (username, email, session['user_id']))
        
        conn.commit()
        
        # Логируем изменение профиля
        log_activity(session['user_id'], 'update_profile',
                    f'Пользователь обновил профиль: username={username}, email={email}', 'user', session['user_id'])
        
        # Соединение глобальное, не закрываем
        return jsonify({'success': True, 'message': 'Profile updated successfully'}), 200
    except Exception as e:
        conn.rollback()
        app.logger.error(f'[UPDATE USER PROFILE] Ошибка: {e}', exc_info=True)
        # Соединение глобальное, не закрываем
        return jsonify({'error': str(e)}), 500


# API для получения настроек пользователя (тема и т.д.)
@app.route('/api/user/settings')
def get_user_settings_api():
    if 'user_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401

    settings = get_user_settings(session['user_id'])
    return jsonify(settings if settings else {})


# API для обновления настроек пользователя
@app.route('/api/user/settings', methods=['PUT'])
def update_user_settings():
    if 'user_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401

    data = request.get_json() or {}
    if not isinstance(data, dict):
        return jsonify({'error': 'Invalid payload'}), 400
    conn = get_db_connection()
    
    try:
        # Проверяем существование настроек
        existing = conn.execute('SELECT id FROM user_settings WHERE user_id = ?', (session['user_id'],)).fetchone()
        
        if existing:
            conn.execute('''
                UPDATE user_settings 
                SET theme = ?, colors = ?, sound_alerts = ?, push_notifications = ?
                WHERE user_id = ?
            ''', (data.get('theme'), json.dumps(data.get('colors', {})), 
                  data.get('sound_alerts', True), data.get('push_notifications', True), session['user_id']))
        else:
            conn.execute('''
                INSERT INTO user_settings (user_id, theme, colors, sound_alerts, push_notifications)
                VALUES (?, ?, ?, ?, ?)
            ''', (session['user_id'], data.get('theme', 'dark'), json.dumps(data.get('colors', {})),
                  data.get('sound_alerts', True), data.get('push_notifications', True)))
        
        conn.commit()
        # Соединение глобальное, не закрываем
        return jsonify({'success': True}), 200
    except Exception as e:
        # Соединение глобальное, не закрываем
        return jsonify({'error': str(e)}), 400


# ==================== НОВЫЕ ФУНКЦИИ ====================

# API для экспорта данных в CSV
# ==================== ЭКСПОРТ ДАННЫХ ====================

@app.route('/api/export/<data_type>')
@require_auth
@handle_errors
def export_data(data_type):
    """Экспорт данных в CSV формат"""
    import csv
    from io import StringIO
    
    conn = get_db_connection()
    output = StringIO()
    writer = csv.writer(output)
    
    if data_type == 'chats':
        if session.get('user_role') == 'admin':
            chats = conn.execute('''
                SELECT c.*, s.name as shop_name 
                FROM avito_chats c
                LEFT JOIN avito_shops s ON c.shop_id = s.id
                ORDER BY c.created_at DESC
            ''').fetchall()
        else:
            chats = conn.execute('''
                SELECT c.*, s.name as shop_name 
                FROM avito_chats c
                JOIN avito_shops s ON c.shop_id = s.id
                JOIN manager_assignments ma ON s.id = ma.shop_id
                WHERE ma.manager_id = ?
                ORDER BY c.created_at DESC
            ''', (session['user_id'],)).fetchall()
        
        writer.writerow(['ID', 'Магазин', 'Клиент', 'Телефон', 'Приоритет', 'Статус', 'Последнее сообщение', 'Создан'])
        for chat in chats:
            writer.writerow([
                chat['id'], chat['shop_name'], chat['client_name'], 
                chat['client_phone'], chat['priority'], chat['status'],
                chat['last_message'], chat['created_at']
            ])
    
    elif data_type == 'clients':
        clients = conn.execute('SELECT id, name, phone, email, notes, total_orders, total_spent, is_blacklisted, created_at, updated_at FROM clients ORDER BY created_at DESC').fetchall()
        writer.writerow(['ID', 'Имя', 'Телефон', 'Email', 'Заказов', 'Потрачено', 'Создан'])
        for client in clients:
            writer.writerow([
                client['id'], client['name'], client['phone'], 
                client['email'] or '', client['total_orders'], 
                client['total_spent'], client['created_at']
            ])
    
    elif data_type == 'deliveries':
        """
        Экспорт доставок в CSV формат
        
        Экспортирует все доставки с полной информацией:
        - Для администраторов: все доставки
        - Для менеджеров: только свои доставки
        """
        has_name_cols = check_name_columns(conn)
        if session.get('user_role') == 'admin':
            if has_name_cols:
                deliveries = conn.execute('''
                    SELECT d.*, c.client_name, c.client_phone, 
                           COALESCE(
                               NULLIF(TRIM(u.first_name || ' ' || COALESCE(u.last_name, '')), ''),
                               u.username,
                               'Система'
                           ) as manager_name
                    FROM deliveries d
                    LEFT JOIN avito_chats c ON d.chat_id = c.id
                    LEFT JOIN users u ON d.manager_id = u.id
                    ORDER BY d.created_at DESC
                ''').fetchall()
            else:
                deliveries = conn.execute('''
                    SELECT d.*, c.client_name, c.client_phone, 
                           COALESCE(u.username, 'Система') as manager_name
                    FROM deliveries d
                    LEFT JOIN avito_chats c ON d.chat_id = c.id
                    LEFT JOIN users u ON d.manager_id = u.id
                    ORDER BY d.created_at DESC
                ''').fetchall()
        else:
            deliveries = conn.execute('''
                SELECT d.*, c.client_name, c.client_phone
                FROM deliveries d
                LEFT JOIN avito_chats c ON d.chat_id = c.id
                WHERE d.manager_id = ?
                ORDER BY d.created_at DESC
            ''', (session['user_id'],)).fetchall()
        
        # Заголовки CSV файла
        writer.writerow([
            'ID', 'Клиент', 'Телефон', 'Адрес', 'Трек-номер', 
            'Статус', 'Менеджер', 'Заметки', 'Создано', 'Обновлено'
        ])
        
        # Данные доставок
        for delivery in deliveries:
            writer.writerow([
                delivery['id'],
                delivery.get('client_name', ''),
                delivery.get('client_phone', ''),
                delivery.get('address', ''),
                delivery.get('tracking_number', ''),
                delivery.get('delivery_status', ''),
                delivery.get('manager_name', '') if session.get('user_role') == 'admin' else '',
                delivery.get('notes', ''),
                delivery.get('created_at', ''),
                delivery.get('updated_at', '')
            ])
    
    elif data_type == 'analytics':
        if session.get('user_role') != 'admin':
            # Соединение глобальное, не закрываем
            return jsonify({'error': 'Access denied'}), 403
        
        logs = conn.execute('''
            SELECT id, event_type, user_id, chat_id, shop_id, metadata, created_at 
            FROM analytics_logs 
            ORDER BY created_at DESC 
            LIMIT 10000
        ''').fetchall()
        writer.writerow(['ID', 'Тип события', 'Пользователь', 'Чат', 'Магазин', 'Метаданные', 'Дата'])
        for log in logs:
            writer.writerow([
                log['id'], log['event_type'], log['user_id'], 
                log['chat_id'] or '', log['shop_id'] or '',
                log['metadata'] or '', log['created_at']
            ])
    
            # Соединение глобальное, не закрываем
    
    output.seek(0)
    from flask import Response
    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': f'attachment; filename={data_type}_export_{datetime.now().strftime("%Y%m%d")}.csv'}
    )


# API для получения уведомлений
@app.route('/api/notifications')
@require_auth
@handle_errors
def get_notifications():
    """Получение уведомлений для пользователя"""
    conn = get_db_connection()
    
    # Получаем срочные чаты
    if session.get('user_role') == 'admin':
        urgent_chats = conn.execute('''
            SELECT COUNT(*) as count FROM avito_chats 
            WHERE priority = 'urgent' AND status != 'completed'
        ''').fetchone()['count']
    else:
        urgent_chats = conn.execute('''
            SELECT COUNT(*) as count FROM avito_chats c
            JOIN avito_shops s ON c.shop_id = s.id
            JOIN manager_assignments ma ON s.id = ma.shop_id
            WHERE ma.manager_id = ? AND c.priority = 'urgent' AND c.status != 'completed'
        ''', (session['user_id'],)).fetchone()['count']
    
    # Получаем непрочитанные чаты
    if session.get('user_role') == 'admin':
        unread_chats = conn.execute('''
            SELECT COUNT(*) as count FROM avito_chats 
            WHERE unread_count > 0 AND status != 'completed'
        ''').fetchone()['count']
    else:
        unread_chats = conn.execute('''
            SELECT COUNT(*) as count FROM avito_chats c
            JOIN avito_shops s ON c.shop_id = s.id
            JOIN manager_assignments ma ON s.id = ma.shop_id
            WHERE ma.manager_id = ? AND c.unread_count > 0 AND c.status != 'completed'
        ''', (session['user_id'],)).fetchone()['count']
    
            # Соединение глобальное, не закрываем
    
    notifications = []
    if urgent_chats > 0:
        notifications.append({
            'type': 'urgent',
            'title': f'{urgent_chats} срочных чатов',
            'message': f'Требуется немедленное внимание',
            'count': urgent_chats
        })
    
    if unread_chats > 0:
        notifications.append({
            'type': 'unread',
            'title': f'{unread_chats} непрочитанных чатов',
            'message': f'Новые сообщения требуют ответа',
            'count': unread_chats
        })
    
    return jsonify(notifications)


# API для получения графиков аналитики
@app.route('/api/analytics/charts')
@require_auth
@handle_errors
def get_analytics_charts():
    """Получение данных для графиков"""
    conn = get_db_connection()
    user_id = session['user_id']
    role = session.get('user_role')
    
    # График чатов по дням (последние 30 дней)
    if role == 'admin':
        daily_chats = conn.execute('''
            SELECT DATE(created_at) as date, COUNT(*) as count
            FROM avito_chats
            WHERE created_at >= datetime('now', '-30 days')
            GROUP BY DATE(created_at)
            ORDER BY date
        ''').fetchall()
    else:
        daily_chats = conn.execute('''
            SELECT DATE(c.created_at) as date, COUNT(*) as count
            FROM avito_chats c
            JOIN avito_shops s ON c.shop_id = s.id
            JOIN manager_assignments ma ON s.id = ma.shop_id
            WHERE ma.manager_id = ? AND c.created_at >= datetime('now', '-30 days')
            GROUP BY DATE(c.created_at)
            ORDER BY date
        ''', (user_id,)).fetchall()
    
    # График по приоритетам
    if role == 'admin':
        priority_stats = conn.execute('''
            SELECT priority, COUNT(*) as count
            FROM avito_chats
            WHERE status != 'completed'
            GROUP BY priority
        ''').fetchall()
    else:
        priority_stats = conn.execute('''
            SELECT c.priority, COUNT(*) as count
            FROM avito_chats c
            JOIN avito_shops s ON c.shop_id = s.id
            JOIN manager_assignments ma ON s.id = ma.shop_id
            WHERE ma.manager_id = ? AND c.status != 'completed'
            GROUP BY c.priority
        ''', (user_id,)).fetchall()
    
    # График активности по часам
    if role == 'admin':
        hourly_activity = conn.execute('''
            SELECT strftime('%H', created_at) as hour, COUNT(*) as count
            FROM analytics_logs
            WHERE created_at >= datetime('now', '-7 days')
            GROUP BY hour
            ORDER BY hour
        ''').fetchall()
    else:
        hourly_activity = conn.execute('''
            SELECT strftime('%H', created_at) as hour, COUNT(*) as count
            FROM analytics_logs
            WHERE user_id = ? AND created_at >= datetime('now', '-7 days')
            GROUP BY hour
            ORDER BY hour
        ''', (user_id,)).fetchall()
    
            # Соединение глобальное, не закрываем
    
    return jsonify({
        'daily_chats': [{'date': str(row['date']), 'count': row['count']} for row in daily_chats],
        'priority_stats': [{'priority': row['priority'], 'count': row['count']} for row in priority_stats],
        'hourly_activity': [{'hour': int(row['hour']), 'count': row['count']} for row in hourly_activity]
    })


# API для поиска (улучшенный)
@app.route('/api/search')
@require_auth
@handle_errors
def search():
    """Универсальный поиск"""
    query = request.args.get('q', '').strip()
    search_type = request.args.get('type', 'all')  # all, chats, clients, shops
    
    if not query or len(query) < 2:
        return jsonify({'error': 'Query too short'}), 400
    
    conn = get_db_connection()
    results = {
        'chats': [],
        'clients': [],
        'shops': []
    }
    
    if search_type in ('all', 'chats'):
        if session.get('user_role') == 'admin':
            chats = conn.execute('''
                SELECT c.*, s.name as shop_name 
                FROM avito_chats c
                LEFT JOIN avito_shops s ON c.shop_id = s.id
                WHERE c.client_name LIKE ? OR c.client_phone LIKE ? OR c.last_message LIKE ?
                ORDER BY c.updated_at DESC
                LIMIT 50
            ''', (f'%{query}%', f'%{query}%', f'%{query}%')).fetchall()
        else:
            chats = conn.execute('''
                SELECT c.*, s.name as shop_name 
                FROM avito_chats c
                JOIN avito_shops s ON c.shop_id = s.id
                JOIN manager_assignments ma ON s.id = ma.shop_id
                WHERE ma.manager_id = ? AND (c.client_name LIKE ? OR c.client_phone LIKE ? OR c.last_message LIKE ?)
                ORDER BY c.updated_at DESC
                LIMIT 50
            ''', (session['user_id'], f'%{query}%', f'%{query}%', f'%{query}%')).fetchall()
        results['chats'] = [dict(chat) for chat in chats]
    
    if search_type in ('all', 'clients'):
        clients = conn.execute('''
            SELECT id, name, phone, email, notes, total_orders, total_spent, is_blacklisted, created_at, updated_at 
            FROM clients
            WHERE name LIKE ? OR phone LIKE ? OR email LIKE ?
            ORDER BY updated_at DESC
            LIMIT 50
        ''', (f'%{query}%', f'%{query}%', f'%{query}%')).fetchall()
        results['clients'] = [dict(client) for client in clients]
    
    if search_type in ('all', 'shops') and session.get('user_role') == 'admin':
        shops = conn.execute('''
            SELECT id, name, shop_url, api_key, is_active, created_at, client_id, client_secret, user_id, webhook_registered, token_checked_at, token_status
            FROM avito_shops
            WHERE name LIKE ? OR shop_url LIKE ?
            ORDER BY created_at DESC
            LIMIT 50
        ''', (f'%{query}%', f'%{query}%')).fetchall()
        results['shops'] = [dict(shop) for shop in shops]
    
            # Соединение глобальное, не закрываем
    return jsonify(results)


# ==================== МОДУЛЬ УПРАВЛЕНИЯ МЕНЕДЖЕРАМИ ====================

# API для создания менеджера (только админ)
@app.route('/api/managers', methods=['POST'])
@require_auth
@handle_errors
def create_manager():
    """Создание нового менеджера админом с генерацией одноразового пароля"""
    try:
        # Проверяем права: админ может создавать менеджеров, супер-админ может создавать всех
        user_role = session.get('user_role')
        if user_role not in ['admin', 'super_admin']:
            app.logger.warning(f'Попытка создания пользователя без прав: {user_role}')
            return jsonify({'error': 'Access denied'}), 403

        data = request.get_json()
        app.logger.info(f'[CREATE USER] Получены данные: {data}')
        
        if not data:
            app.logger.warning('[CREATE USER] Нет данных в запросе')
            return jsonify({'error': 'No data provided'}), 400
        
        username = data.get('username', '').strip()
        email = data.get('email', '').strip()
        salary = float(data.get('salary', 0) or 0)
        role = data.get('role', 'manager')  # Позволяем выбирать роль

        # Супер-админ может создавать админов, обычный админ - только менеджеров
        if role not in ['manager', 'admin']:
            role = 'manager'
        if role == 'admin' and user_role != 'super_admin':
            app.logger.warning(f'[CREATE USER] Попытка создать админа обычным админом')
            return jsonify({'error': 'Only super admin can create admin accounts'}), 403
        
        if not username or not email:
            app.logger.warning(f'[CREATE USER] Не заполнены обязательные поля: username={bool(username)}, email={bool(email)}')
            return jsonify({'error': 'Username and email are required'}), 400
        
        if not validate_email(email):
            app.logger.warning(f'[CREATE USER] Неверный формат email: {email}')
            return jsonify({'error': 'Invalid email format'}), 400
        
        conn = get_db_connection()
        
        # Проверяем, не существует ли уже пользователь с таким email
        existing_user = conn.execute(
            'SELECT id, username, email, is_active FROM users WHERE email = ?',
            (email,)
        ).fetchone()
        
        if existing_user:
            existing_dict = dict(existing_user)
            app.logger.warning(f'[CREATE USER] Попытка создать пользователя с существующим email: {email}')
            status_text = 'активен' if existing_dict.get('is_active') else 'неактивен'
            return jsonify({
                'error': f'Пользователь с email {email} уже существует (ID: {existing_dict.get("id")}, статус: {status_text})'
            }), 400
        
        from auth import generate_temp_password, hash_password
        
        # Генерируем одноразовый пароль для нового пользователя
        temp_password = generate_temp_password()
        hashed_password = hash_password(temp_password)
        
        first_name = (data.get('first_name') or '').strip() or None
        last_name = (data.get('last_name') or '').strip() or None
        
        try:
            has_name_cols = check_name_columns(conn)
            if has_name_cols:
                cursor = conn.execute('''
                    INSERT INTO users (username, email, password, temp_password, role, salary, is_active, created_by, password_changed, first_name, last_name)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (username, email, hashed_password, temp_password, role, salary, True, session['user_id'], False, first_name, last_name))
            else:
                cursor = conn.execute('''
                    INSERT INTO users (username, email, password, temp_password, role, salary, is_active, created_by, password_changed)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (username, email, hashed_password, temp_password, role, salary, True, session['user_id'], False))
            manager_id = cursor.lastrowid
            
            # Логируем действие
            log_activity(session['user_id'], 'create_user',
                        f'Создан пользователь: {username} ({email}) с ролью {role}', 'user', manager_id)
            
            conn.commit()
            # Соединение глобальное, не закрываем

            app.logger.info(f'[CREATE USER] Пользователь успешно создан: ID={manager_id}, username={username}, email={email}')
            return jsonify({
                'success': True,
                'id': manager_id,
                'temp_password': temp_password,
                'message': f'Пользователь создан. Одноразовый пароль: {temp_password}'
            }), 201
        except Exception as e:
            app.logger.error(f'[CREATE USER] Ошибка при создании пользователя в БД: {e}', exc_info=True)
            # Соединение глобальное, не закрываем
            if 'UNIQUE constraint' in str(e):
                return jsonify({'error': 'User with this email already exists'}), 400
            return jsonify({'error': str(e)}), 400
    except Exception as e:
        app.logger.error(f'[CREATE USER] Неожиданная ошибка: {e}', exc_info=True)
        return jsonify({'error': f'Internal server error: {str(e)}'}), 500

# API для обновления менеджера
@app.route('/api/managers/<int:manager_id>', methods=['PUT'])
@require_auth
@handle_errors
def update_manager(manager_id):
    """Обновление данных менеджера или админа (только для админов и суперадминов)"""
    # Проверяем права доступа: только админ и суперадмин могут редактировать
    user_role = session.get('user_role')
    if user_role not in ['admin', 'super_admin']:
        app.logger.warning(f'Попытка редактирования пользователя без прав: {user_role}')
        return jsonify({'error': 'Access denied. Only admin and super_admin can edit users'}), 403
    
    data = request.get_json()
    conn = get_db_connection()
    
    try:
        # Проверяем, существует ли пользователь
        target_user = conn.execute('SELECT id, role FROM users WHERE id = ?', (manager_id,)).fetchone()
        if not target_user:
            return jsonify({'error': 'User not found'}), 404
        
        target_user_dict = dict(target_user)
        target_role = target_user_dict.get('role')
        
        # Суперадмин может редактировать всех, обычный админ - только менеджеров
        if target_role == 'admin' and user_role != 'super_admin':
            return jsonify({'error': 'Only super_admin can edit admin accounts'}), 403
        
        update_fields = []
        update_values = []
        
        if 'username' in data:
            update_fields.append('username = ?')
            update_values.append(data['username'])
        
        if 'email' in data:
            if not validate_email(data['email']):
                return jsonify({'error': 'Invalid email format'}), 400
            
            # Проверяем, не используется ли этот email другим пользователем
            existing_user = conn.execute(
                'SELECT id, username, email FROM users WHERE email = ? AND id != ?',
                (data['email'], manager_id)
            ).fetchone()
            
            if existing_user:
                existing_dict = dict(existing_user)
                return jsonify({
                    'error': f'Email {data["email"]} уже используется другим пользователем (ID: {existing_dict.get("id")}, имя: {existing_dict.get("username")})'
                }), 400
            
            update_fields.append('email = ?')
            update_values.append(data['email'])
        
        if 'first_name' in data:
            first_name = (data['first_name'] or '').strip() or None
            update_fields.append('first_name = ?')
            update_values.append(first_name)
        
        if 'last_name' in data:
            last_name = (data['last_name'] or '').strip() or None
            update_fields.append('last_name = ?')
            update_values.append(last_name)
        
        if 'password' in data and data['password']:
            if len(data['password']) < 6:
                return jsonify({'error': 'Password must be at least 6 characters'}), 400
            from auth import hash_password
            hashed_password = hash_password(data['password'])
            update_fields.append('password = ?')
            update_values.append(hashed_password)
        
        if 'salary' in data:
            update_fields.append('salary = ?')
            update_values.append(data['salary'])
        
        if 'is_active' in data:
            update_fields.append('is_active = ?')
            update_values.append(data['is_active'])
        
        if 'role' in data:
            # Суперадмин может менять роль, обычный админ - нет
            if user_role == 'super_admin':
                new_role = data['role']
                if new_role in ['manager', 'admin']:
                    update_fields.append('role = ?')
                    update_values.append(new_role)
            # Обычный админ не может менять роль
        
        if update_fields:
            update_fields.append('updated_at = CURRENT_TIMESTAMP')
            update_values.append(manager_id)
            # Убираем ограничение по роли, так как теперь можем редактировать и админов
            query = f'UPDATE users SET {", ".join(update_fields)} WHERE id = ?'
            conn.execute(query, tuple(update_values))
            
            # Логируем действие
            log_activity(session['user_id'], 'update_manager', 
                        f'Обновлен пользователь ID: {manager_id} (роль: {target_role})', 'user', manager_id)
            
            conn.commit()
        
        # Соединение глобальное, не закрываем
        return jsonify({'success': True}), 200
    except Exception as e:
        conn.rollback()
        app.logger.error(f'[UPDATE MANAGER] Ошибка: {e}', exc_info=True)
        # Соединение глобальное, не закрываем
        return jsonify({'error': str(e)}), 400

# API для удаления менеджера или админа
@app.route('/api/managers/<int:manager_id>', methods=['DELETE'])
@require_auth
@handle_errors
def delete_manager(manager_id):
    """Удаление пользователя (деактивация)"""
    user_role = session.get('user_role')
    current_user_id = session.get('user_id')
    
    # Проверяем права: только админы могут удалять
    if user_role not in ['admin', 'super_admin']:
        return jsonify({'error': 'Access denied'}), 403
    
    # Нельзя удалить самого себя
    if manager_id == current_user_id:
        return jsonify({'error': 'Нельзя удалить свой собственный аккаунт'}), 400
    
    conn = get_db_connection()
    try:
        # Получаем информацию о пользователе, которого хотим удалить
        user_to_delete = conn.execute('''
            SELECT id, username, email, role, is_active
            FROM users
            WHERE id = ?
        ''', (manager_id,)).fetchone()
        
        if not user_to_delete:
            return jsonify({'error': 'Пользователь не найден'}), 404
        
        user_dict = dict(user_to_delete)
        target_role = user_dict.get('role')
        target_username = user_dict.get('username', 'Unknown')
        
        # Супер-админ может удалять всех (менеджеров и админов)
        # Обычный админ может удалять только менеджеров
        if user_role == 'admin' and target_role != 'manager':
            return jsonify({'error': 'Только супер-админ может удалять администраторов'}), 403
        
        # Деактивируем пользователя
        conn.execute('UPDATE users SET is_active = 0 WHERE id = ?', (manager_id,))
        
        # Логируем действие
        role_text = 'администратор' if target_role == 'admin' else 'менеджер'
        log_activity(current_user_id, 'delete_user', 
                    f'Деактивирован {role_text}: {target_username} (ID: {manager_id})', 'user', manager_id)
        
        conn.commit()
        # Соединение глобальное, не закрываем
        return jsonify({
            'success': True,
            'message': f'Пользователь {target_username} успешно деактивирован'
        }), 200
    except Exception as e:
        app.logger.error(f'[DELETE USER] Ошибка удаления пользователя {manager_id}: {e}', exc_info=True)
        # Соединение глобальное, не закрываем
        return jsonify({'error': str(e)}), 400

# API для сброса пароля пользователя (только для админа/супер админа)
@app.route('/api/managers/<int:manager_id>/reset-password', methods=['POST'])
@require_auth
@handle_errors
def reset_user_password(manager_id):
    """Сброс пароля пользователя с генерацией нового одноразового пароля"""
    user_role = session.get('user_role')
    current_user_id = session.get('user_id')
    
    # Проверяем права: только админы могут сбрасывать пароли
    if user_role not in ['admin', 'super_admin']:
        return jsonify({'error': 'Access denied'}), 403
    
    # Нельзя сбросить пароль самому себе
    if manager_id == current_user_id:
        return jsonify({'error': 'Нельзя сбросить пароль своему собственному аккаунту'}), 400
    
    conn = get_db_connection()
    try:
        # Получаем информацию о пользователе, которому сбрасываем пароль
        user_to_reset = conn.execute('''
            SELECT id, username, email, role, is_active
            FROM users
            WHERE id = ?
        ''', (manager_id,)).fetchone()
        
        if not user_to_reset:
            return jsonify({'error': 'Пользователь не найден'}), 404
        
        user_dict = dict(user_to_reset)
        target_role = user_dict.get('role')
        target_username = user_dict.get('username', 'Unknown')
        
        # Супер-админ может сбрасывать пароли всем (менеджерам и админам)
        # Обычный админ может сбрасывать пароли только менеджерам
        if user_role == 'admin' and target_role != 'manager':
            return jsonify({'error': 'Только супер-админ может сбрасывать пароли администраторам'}), 403
        
        # Проверяем, что пользователь активен
        if not user_dict.get('is_active'):
            return jsonify({'error': 'Нельзя сбросить пароль неактивному пользователю'}), 400
        
        # Генерируем новый одноразовый пароль
        from auth import generate_temp_password, hash_password
        
        temp_password = generate_temp_password()
        hashed_password = hash_password(temp_password)
        
        # Обновляем пароль в БД
        conn.execute('''
            UPDATE users
            SET password = ?, temp_password = ?, password_changed = 0
            WHERE id = ?
        ''', (hashed_password, temp_password, manager_id))
        
        # Логируем действие
        role_text = 'администратору' if target_role == 'admin' else 'менеджеру'
        log_activity(current_user_id, 'reset_password', 
                    f'Сброшен пароль {role_text}: {target_username} (ID: {manager_id})', 'user', manager_id)
        
        conn.commit()
        # Соединение глобальное, не закрываем
        
        app.logger.info(f'[RESET PASSWORD] Пароль сброшен для пользователя ID={manager_id}, username={target_username}')
        return jsonify({
            'success': True,
            'temp_password': temp_password,
            'message': f'Пароль успешно сброшен для пользователя {target_username}. Новый одноразовый пароль: {temp_password}'
        }), 200
    except Exception as e:
        app.logger.error(f'[RESET PASSWORD] Ошибка сброса пароля пользователя {manager_id}: {e}', exc_info=True)
        # Соединение глобальное, не закрываем
        return jsonify({'error': str(e)}), 400

# ==================== МОДУЛЬ ГРАФИКА РАБОТЫ ====================

# API для получения графика работы пользователя
@app.route('/api/work-schedules/<int:user_id>')
@require_auth
@handle_errors
def get_work_schedule(user_id):
    """Получение графика работы пользователя"""
    # Пользователь может видеть свой график, админ - любой
    if session.get('user_role') != 'admin' and session['user_id'] != user_id:
        return jsonify({'error': 'Access denied'}), 403
    
    conn = get_db_connection()
    schedules = conn.execute('''
        SELECT id, user_id, day_of_week, start_time, end_time, is_working_day, created_at, updated_at 
        FROM work_schedules 
        WHERE user_id = ?
        ORDER BY day_of_week
    ''', (user_id,)).fetchall()
            # Соединение глобальное, не закрываем
    
    return jsonify([dict(schedule) for schedule in schedules])

# API для получения всех графиков работы (только для админа)
@app.route('/api/work-schedules')
@require_auth
@require_role('admin')
@handle_errors
def get_all_work_schedules():
    """Получение всех графиков работы (только админ)"""
    conn = get_db_connection()
    schedules = conn.execute('''
        SELECT ws.*, u.username, u.email, u.role
        FROM work_schedules ws
        JOIN users u ON ws.user_id = u.id
        ORDER BY u.username, ws.day_of_week
    ''').fetchall()
            # Соединение глобальное, не закрываем
    
    return jsonify([dict(schedule) for schedule in schedules])

# API для создания/обновления графика работы (только админ)
@app.route('/api/work-schedules', methods=['POST', 'PUT'])
@require_auth
@require_role('admin')
@handle_errors
def save_work_schedule():
    """Создание или обновление графика работы (только админ)"""
    data = request.get_json()
    
    if not data:
        return jsonify({'error': 'No data provided'}), 400
    
    user_id = data.get('user_id')
    day_of_week = data.get('day_of_week')
    start_time = data.get('start_time')
    end_time = data.get('end_time')
    is_working_day = data.get('is_working_day', True)
    
    if user_id is None or day_of_week is None:
        return jsonify({'error': 'user_id and day_of_week are required'}), 400
    
    if is_working_day and (not start_time or not end_time):
        return jsonify({'error': 'start_time and end_time are required for working days'}), 400
    
    conn = get_db_connection()
    try:
        # Проверяем существование записи
        existing = conn.execute('''
            SELECT id FROM work_schedules 
            WHERE user_id = ? AND day_of_week = ?
        ''', (user_id, day_of_week)).fetchone()
        
        if existing:
            # Обновляем существующую запись
            conn.execute('''
                UPDATE work_schedules 
                SET start_time = ?, end_time = ?, is_working_day = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (start_time if is_working_day else None, 
                  end_time if is_working_day else None, 
                  is_working_day, existing['id']))
        else:
            # Создаем новую запись
            conn.execute('''
                INSERT INTO work_schedules (user_id, day_of_week, start_time, end_time, is_working_day)
                VALUES (?, ?, ?, ?, ?)
            ''', (user_id, day_of_week, 
                  start_time if is_working_day else None, 
                  end_time if is_working_day else None, 
                  is_working_day))
        
        # Логируем действие
        log_activity(session['user_id'], 'update_work_schedule', 
                    f'Обновлен график работы для пользователя ID: {user_id}, день: {day_of_week}', 
                    'work_schedule', user_id)
        
        conn.commit()
        # Соединение глобальное, не закрываем
        return jsonify({'success': True}), 200
    except Exception as e:
        # Соединение глобальное, не закрываем
        return jsonify({'error': str(e)}), 400

# API для массового обновления графика работы (только админ)
@app.route('/api/work-schedules/bulk', methods=['PUT'])
@require_auth
@require_role('admin')
@handle_errors
def bulk_update_work_schedules():
    """Массовое обновление графика работы (только админ)"""
    data = request.get_json()
    
    if not data or 'schedules' not in data:
        return jsonify({'error': 'schedules array is required'}), 400
    
    user_id = data.get('user_id')
    if not user_id:
        return jsonify({'error': 'user_id is required'}), 400
    
    conn = get_db_connection()
    try:
        # Удаляем старые записи для этого пользователя
        conn.execute('DELETE FROM work_schedules WHERE user_id = ?', (user_id,))
        
        # Добавляем новые записи
        for schedule in data['schedules']:
            day_of_week = schedule.get('day_of_week')
            is_working_day = schedule.get('is_working_day', True)
            start_time = schedule.get('start_time') if is_working_day else None
            end_time = schedule.get('end_time') if is_working_day else None
            
            if day_of_week is not None:
                conn.execute('''
                    INSERT INTO work_schedules (user_id, day_of_week, start_time, end_time, is_working_day)
                    VALUES (?, ?, ?, ?, ?)
                ''', (user_id, day_of_week, start_time, end_time, is_working_day))
        
        # Логируем действие
        log_activity(session['user_id'], 'bulk_update_work_schedule', 
                    f'Массово обновлен график работы для пользователя ID: {user_id}', 
                    'work_schedule', user_id)
        
        conn.commit()
        # Соединение глобальное, не закрываем
        return jsonify({'success': True}), 200
    except Exception as e:
        # Соединение глобальное, не закрываем
        return jsonify({'error': str(e)}), 400

# API для получения менеджеров, назначенных на день недели
@app.route('/api/day-managers/<int:day_of_week>')
@require_auth
@handle_errors
def get_day_managers(day_of_week):
    """Получение менеджеров, назначенных на день недели"""
    conn = get_db_connection()
    managers = conn.execute('''
        SELECT dma.*, u.username, u.email, u.id as manager_id
        FROM day_manager_assignments dma
        JOIN users u ON dma.manager_id = u.id
        WHERE dma.day_of_week = ?
        ORDER BY dma.start_time, u.username
    ''', (day_of_week,)).fetchall()
            # Соединение глобальное, не закрываем
    
    return jsonify([dict(manager) for manager in managers])

# API для получения назначений менеджеров на дни недели (доступно для всех авторизованных)
@app.route('/api/day-managers/all')
@require_auth
@handle_errors
def get_all_day_managers_public():
    """Получение всех назначений менеджеров на дни недели (для просмотра)"""
    conn = get_db_connection()
    assignments = conn.execute('''
        SELECT dma.*, u.username, u.email, u.id as manager_id
        FROM day_manager_assignments dma
        JOIN users u ON dma.manager_id = u.id
        ORDER BY dma.day_of_week, dma.start_time, u.username
    ''').fetchall()
            # Соединение глобальное, не закрываем
    
    return jsonify([dict(assignment) for assignment in assignments])

# API для получения всех назначений менеджеров на дни недели
@app.route('/api/day-managers')
@require_auth
@require_role('admin')
@handle_errors
def get_all_day_managers():
    """Получение всех назначений менеджеров на дни недели (только админ)"""
    conn = get_db_connection()
    assignments = conn.execute('''
        SELECT dma.*, u.username, u.email
        FROM day_manager_assignments dma
        JOIN users u ON dma.manager_id = u.id
        ORDER BY dma.day_of_week, dma.start_time, u.username
    ''').fetchall()
            # Соединение глобальное, не закрываем
    
    return jsonify([dict(assignment) for assignment in assignments])

# API для назначения менеджера на день недели
@app.route('/api/day-managers', methods=['POST'])
@require_auth
@require_role('admin')
@handle_errors
def assign_day_manager():
    """Назначение менеджера на день недели (только админ)"""
    data = request.get_json()
    
    if not data:
        return jsonify({'error': 'No data provided'}), 400
    
    day_of_week = data.get('day_of_week')
    manager_id = data.get('manager_id')
    start_time = data.get('start_time')
    end_time = data.get('end_time')
    
    if day_of_week is None or manager_id is None:
        return jsonify({'error': 'day_of_week and manager_id are required'}), 400
    
    conn = get_db_connection()
    try:
        # Проверяем существование записи
        existing = conn.execute('''
            SELECT id FROM day_manager_assignments 
            WHERE day_of_week = ? AND manager_id = ?
        ''', (day_of_week, manager_id)).fetchone()
        
        if existing:
            # Обновляем существующую запись
            conn.execute('''
                UPDATE day_manager_assignments 
                SET start_time = ?, end_time = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (start_time, end_time, existing['id']))
        else:
            # Создаем новую запись
            conn.execute('''
                INSERT INTO day_manager_assignments (day_of_week, manager_id, start_time, end_time)
                VALUES (?, ?, ?, ?)
            ''', (day_of_week, manager_id, start_time, end_time))
        
        # Логируем действие
        log_activity(session['user_id'], 'assign_day_manager', 
                    f'Назначен менеджер ID: {manager_id} на день недели: {day_of_week}', 
                    'day_manager_assignment', manager_id)
        
        conn.commit()
        # Соединение глобальное, не закрываем
        return jsonify({'success': True}), 200
    except Exception as e:
        # Соединение глобальное, не закрываем
        return jsonify({'error': str(e)}), 400

# API для удаления назначения менеджера на день недели
@app.route('/api/day-managers/<int:assignment_id>', methods=['DELETE'])
@require_auth
@require_role('admin')
@handle_errors
def remove_day_manager(assignment_id):
    """Удаление назначения менеджера на день недели (только админ)"""
    conn = get_db_connection()
    try:
        conn.execute('DELETE FROM day_manager_assignments WHERE id = ?', (assignment_id,))
        
        # Логируем действие
        log_activity(session['user_id'], 'remove_day_manager', 
                    f'Удалено назначение менеджера ID: {assignment_id}', 
                    'day_manager_assignment', assignment_id)
        
        conn.commit()
        # Соединение глобальное, не закрываем
        return jsonify({'success': True}), 200
    except Exception as e:
        # Соединение глобальное, не закрываем
        return jsonify({'error': str(e)}), 400

# API для массового обновления назначений менеджеров на дни недели
@app.route('/api/day-managers/bulk', methods=['PUT'])
@require_auth
@require_role('admin')
@handle_errors
def bulk_update_day_managers():
    """Массовое обновление назначений менеджеров на дни недели (только админ)"""
    data = request.get_json()
    
    if not data or 'assignments' not in data:
        return jsonify({'error': 'assignments array is required'}), 400
    
    conn = get_db_connection()
    try:
        # Удаляем все старые назначения
        conn.execute('DELETE FROM day_manager_assignments')
        
        # Добавляем новые назначения
        for assignment in data['assignments']:
            day_of_week = assignment.get('day_of_week')
            manager_id = assignment.get('manager_id')
            start_time = assignment.get('start_time')
            end_time = assignment.get('end_time')
            
            if day_of_week is not None and manager_id is not None:
                conn.execute('''
                    INSERT INTO day_manager_assignments (day_of_week, manager_id, start_time, end_time)
                    VALUES (?, ?, ?, ?)
                ''', (day_of_week, manager_id, start_time, end_time))
        
        # Логируем действие
        log_activity(session['user_id'], 'bulk_update_day_managers', 
                    'Массово обновлены назначения менеджеров на дни недели', 
                    'day_manager_assignment', None)
        
        conn.commit()
        # Соединение глобальное, не закрываем
        return jsonify({'success': True}), 200
    except Exception as e:
        # Соединение глобальное, не закрываем
        return jsonify({'error': str(e)}), 400

# ==================== МОДУЛЬ СМЕН ====================

# API для открытия смены
@app.route('/api/shifts/start', methods=['POST'])
@require_auth
@require_role('manager')
@handle_errors
def start_shift():
    """Открытие смены менеджером"""
    from datetime import datetime, time
    
    conn = get_db_connection()
    manager_id = session['user_id']
    today = datetime.now().date()
    now = datetime.now()
    
    # Проверяем, не открыта ли уже смена на сегодня
    existing_shift = conn.execute('''
        SELECT id, manager_id, shift_date, shift_start_time, shift_end_time, is_late, late_minutes, status, created_at, updated_at 
        FROM shifts 
        WHERE manager_id = ? AND shift_date = ? AND status = "active"
    ''', (manager_id, today)).fetchone()
    
    if existing_shift:
        # Соединение глобальное, не закрываем
        return jsonify({'error': 'Shift already started today'}), 400
    
    # Проверяем опоздание (смена должна начаться до 10:00)
    shift_deadline = datetime.combine(today, time(10, 0))
    is_late = now > shift_deadline
    late_minutes = 0
    
    if is_late:
        late_minutes = int((now - shift_deadline).total_seconds() / 60)
    
    try:
        cursor = conn.execute('''
            INSERT INTO shifts (manager_id, shift_date, shift_start_time, is_late, late_minutes, status)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (manager_id, today, now, is_late, late_minutes, 'active'))
        shift_id = cursor.lastrowid
        
        # Если опоздание, создаем штраф
        if is_late:
            penalty_amount = 500  # Штраф за опоздание
            conn.execute('''
                INSERT INTO penalties (manager_id, shift_id, penalty_type, penalty_amount, reason, created_by)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (manager_id, shift_id, 'late_shift', penalty_amount, 
                  f'Опоздание на {late_minutes} минут', session.get('user_id')))
        
        # Логируем действие
        log_activity(manager_id, 'start_shift', 
                    f'Открыта смена (опоздание: {late_minutes} мин)' if is_late else 'Открыта смена',
                    'shift', shift_id, {'is_late': is_late, 'late_minutes': late_minutes})
        
        conn.commit()
        # Соединение глобальное, не закрываем
        
        return jsonify({
            'success': True, 
            'shift_id': shift_id,
            'is_late': is_late,
            'late_minutes': late_minutes
        }), 201
    except Exception as e:
        # Соединение глобальное, не закрываем
        return jsonify({'error': str(e)}), 400

# API для закрытия смены
@app.route('/api/shifts/end', methods=['POST'])
@require_auth
@require_role('manager')
@handle_errors
def end_shift():
    """Закрытие смены менеджером"""
    conn = get_db_connection()
    manager_id = session['user_id']
    today = datetime.now().date()
    now = datetime.now()
    
    try:
        shift = conn.execute('''
            SELECT id, manager_id, shift_date, shift_start_time, shift_end_time, is_late, late_minutes, status, created_at, updated_at
            FROM shifts 
            WHERE manager_id = ? AND shift_date = ? AND status = "active"
        ''', (manager_id, today)).fetchone()
        
        if not shift:
            # Соединение глобальное, не закрываем
            return jsonify({'error': 'No active shift found'}), 404
        
        conn.execute('''
            UPDATE shifts 
            SET shift_end_time = ?, status = "completed", updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (now, shift['id']))
        
        # Логируем действие
        log_activity(manager_id, 'end_shift', 'Закрыта смена', 'shift', shift['id'])
        
        conn.commit()
        # Соединение глобальное, не закрываем
        return jsonify({'success': True}), 200
    except Exception as e:
        # Соединение глобальное, не закрываем
        return jsonify({'error': str(e)}), 400

# API для получения текущей смены
@app.route('/api/shifts/current')
@require_auth
@require_role('manager')
@handle_errors
def get_current_shift():
    """Получение текущей активной смены"""
    conn = get_db_connection()
    manager_id = session['user_id']
    today = datetime.now().date()
    
    shift = conn.execute('''
        SELECT id, manager_id, shift_date, shift_start_time, shift_end_time, is_late, late_minutes, status, created_at, updated_at 
        FROM shifts 
        WHERE manager_id = ? AND shift_date = ? AND status = "active"
    ''', (manager_id, today)).fetchone()
    
            # Соединение глобальное, не закрываем
    
    if shift:
        return jsonify({'shift': dict(shift)})
    return jsonify({'shift': None})

# API для получения всех смен (админ)
@app.route('/api/shifts')
@require_auth
@handle_errors
def get_shifts():
    """Получение всех смен"""
    conn = get_db_connection()
    
    has_name_cols = check_name_columns(conn)
    if session.get('user_role') == 'admin':
        if has_name_cols:
            shifts = conn.execute('''
                SELECT s.*, COALESCE(TRIM(u.first_name || ' ' || COALESCE(u.last_name, '')), u.username, 'Система') as manager_name
                FROM shifts s
                JOIN users u ON s.manager_id = u.id
                ORDER BY s.shift_date DESC, s.shift_start_time DESC
            ''').fetchall()
        else:
            shifts = conn.execute('''
                SELECT s.*, COALESCE(u.username, 'Система') as manager_name
                FROM shifts s
                JOIN users u ON s.manager_id = u.id
                ORDER BY s.shift_date DESC, s.shift_start_time DESC
            ''').fetchall()
    else:
        shifts = conn.execute('''
            SELECT id, manager_id, shift_date, shift_start_time, shift_end_time, is_late, late_minutes, status, created_at, updated_at 
            FROM shifts 
            WHERE manager_id = ?
            ORDER BY shift_date DESC, shift_start_time DESC
        ''', (session['user_id'],)).fetchall()
    
            # Соединение глобальное, не закрываем
    return jsonify([dict(shift) for shift in shifts])

# ==================== МОДУЛЬ ШТРАФОВ ====================

# API для получения штрафов
@app.route('/api/penalties')
@require_auth
@handle_errors
def get_penalties():
    """Получение штрафов"""
    conn = get_db_connection()
    
    has_name_cols = check_name_columns(conn)
    if session.get('user_role') == 'admin':
        if has_name_cols:
            penalties = conn.execute('''
                SELECT p.*, 
                       COALESCE(
                           NULLIF(TRIM(u.first_name || ' ' || COALESCE(u.last_name, '')), ''),
                           u.username,
                           'Система'
                       ) as manager_name,
                       COALESCE(
                           NULLIF(TRIM(u2.first_name || ' ' || COALESCE(u2.last_name, '')), ''),
                           u2.username,
                           'Система'
                       ) as created_by_name
                FROM penalties p
                LEFT JOIN users u ON p.manager_id = u.id
                LEFT JOIN users u2 ON p.created_by = u2.id
                ORDER BY p.created_at DESC
            ''').fetchall()
        else:
            penalties = conn.execute('''
                SELECT p.*, 
                       COALESCE(u.username, 'Система') as manager_name,
                       COALESCE(u2.username, 'Система') as created_by_name
                FROM penalties p
                LEFT JOIN users u ON p.manager_id = u.id
                LEFT JOIN users u2 ON p.created_by = u2.id
                ORDER BY p.created_at DESC
            ''').fetchall()
    else:
        # Для менеджера показываем только его штрафы
        if has_name_cols:
            penalties = conn.execute('''
                SELECT p.*, 
                       COALESCE(
                           NULLIF(TRIM(u2.first_name || ' ' || COALESCE(u2.last_name, '')), ''),
                           u2.username,
                           'Система'
                       ) as created_by_name
                FROM penalties p
                JOIN users u ON p.manager_id = u.id
                LEFT JOIN users u2 ON p.created_by = u2.id
                WHERE p.manager_id = ?
                ORDER BY p.created_at DESC
            ''', (session['user_id'],)).fetchall()
        else:
            penalties = conn.execute('''
                SELECT p.*, COALESCE(u2.username, 'Система') as created_by_name
                FROM penalties p
                LEFT JOIN users u2 ON p.created_by = u2.id
                WHERE p.manager_id = ?
                ORDER BY p.created_at DESC
            ''', (session['user_id'],)).fetchall()
    
            # Соединение глобальное, не закрываем
    return jsonify([dict(penalty) for penalty in penalties])

# API для создания штрафа (админ)
@app.route('/api/penalties', methods=['POST'])
@require_auth
@require_role('admin')
@handle_errors
def create_penalty():
    """Создание штрафа админом"""
    data = request.get_json()
    
    if not data:
        return jsonify({'error': 'No data provided'}), 400
    
    manager_id = data.get('manager_id')
    penalty_type = data.get('penalty_type', 'other')
    penalty_amount = data.get('penalty_amount', 0)
    reason = data.get('reason', '')
    shift_id = data.get('shift_id')
    
    if not manager_id or not penalty_amount:
        return jsonify({'error': 'Manager ID and penalty amount are required'}), 400
    
    conn = get_db_connection()
    try:
        cursor = conn.execute('''
            INSERT INTO penalties (manager_id, shift_id, penalty_type, penalty_amount, reason, created_by)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (manager_id, shift_id, penalty_type, penalty_amount, reason, session['user_id']))
        penalty_id = cursor.lastrowid
        
        # Логируем действие
        log_activity(session['user_id'], 'create_penalty', 
                    f'Создан штраф: {penalty_amount} руб. для менеджера ID: {manager_id}',
                    'penalty', penalty_id, {'manager_id': manager_id, 'amount': penalty_amount})
        
        conn.commit()
        # Соединение глобальное, не закрываем
        return jsonify({'success': True, 'id': penalty_id}), 201
    except Exception as e:
        # Соединение глобальное, не закрываем
        return jsonify({'error': str(e)}), 400

# ==================== МОДУЛЬ ЛОГОВ ДЕЙСТВИЙ ====================

# API для получения логов действий
@app.route('/api/activity-logs')
@require_auth
@require_role('admin')
@handle_errors
def get_activity_logs():
    """Получение логов действий (только админ)"""
    manager_id = request.args.get('manager_id', type=int)
    limit = request.args.get('limit', 100, type=int)
    
    conn = get_db_connection()
    
    if manager_id:
        logs = conn.execute('''
            SELECT al.*, u.username
            FROM activity_logs al
            JOIN users u ON al.user_id = u.id
            WHERE al.user_id = ?
            ORDER BY al.created_at DESC
            LIMIT ?
        ''', (manager_id, limit)).fetchall()
    else:
        logs = conn.execute('''
            SELECT al.*, u.username
            FROM activity_logs al
            JOIN users u ON al.user_id = u.id
            ORDER BY al.created_at DESC
            LIMIT ?
        ''', (limit,)).fetchall()
    
            # Соединение глобальное, не закрываем
    return jsonify([dict(log) for log in logs])

# API для получения списка менеджеров (для фильтра)
@app.route('/api/managers/list')
@require_auth
@handle_errors
def get_managers_list():
    """Получение списка пользователей для управления (только для админов и суперадминов)"""
    user_role = session.get('user_role')
    if user_role not in ['admin', 'super_admin']:
        return jsonify({'error': 'Access denied'}), 403

    conn = get_db_connection()

    has_name_cols = check_name_columns(conn)
    
    # Супер-админ видит всех, админ - только менеджеров
    if user_role == 'super_admin':
        if has_name_cols:
            managers = conn.execute('''
                SELECT id, username, email, role, is_active, temp_password, password_changed, created_at, first_name, last_name
                FROM users
                WHERE role IN ('manager', 'admin')
                ORDER BY role, username
            ''').fetchall()
        else:
            managers = conn.execute('''
                SELECT id, username, email, role, is_active, temp_password, password_changed, created_at
                FROM users
                WHERE role IN ('manager', 'admin')
                ORDER BY role, username
            ''').fetchall()
    else:
        if has_name_cols:
            managers = conn.execute('''
                SELECT id, username, email, role, is_active, temp_password, password_changed, created_at, first_name, last_name
                FROM users
                WHERE role = 'manager'
                ORDER BY username
            ''').fetchall()
        else:
            managers = conn.execute('''
                SELECT id, username, email, role, is_active, temp_password, password_changed, created_at
                FROM users
                WHERE role = 'manager'
                ORDER BY username
            ''').fetchall()

            # Соединение глобальное, не закрываем
    return jsonify([dict(m) for m in managers])

# API для смены пароля пользователя
@app.route('/api/user/change-password', methods=['POST'])
@require_auth
@handle_errors
def change_user_password():
    """Смена пароля пользователя"""
    data = request.get_json()

    if not data:
        return jsonify({'error': 'No data provided'}), 400

    current_password = data.get('current_password', '').strip()
    new_password = data.get('new_password', '').strip()
    confirm_password = data.get('confirm_password', '').strip()

    if not new_password or not confirm_password:
        return jsonify({'error': 'New password and confirmation are required'}), 400

    if new_password != confirm_password:
        return jsonify({'error': 'Passwords do not match'}), 400

    if len(new_password) < 6:
        return jsonify({'error': 'Password must be at least 6 characters'}), 400

    from auth import authenticate_user, update_user_password

    user = get_user_by_id(session['user_id'])
    if not user:
        return jsonify({'error': 'User not found'}), 404

    # Проверяем, это первый вход (нужно ли требовать текущий пароль)
    is_first_login = not user.get('password_changed', True)

    # Проверяем текущий пароль только если это не первый вход
    if not is_first_login:
        user_email = user['email']
        auth_result = authenticate_user(user_email, current_password)
        if not auth_result:
            return jsonify({'error': 'Current password is incorrect'}), 400

    # Обновляем пароль
    if update_user_password(session['user_id'], new_password):
        # Логируем изменение пароля
        log_activity(session['user_id'], 'change_password',
                    'Пользователь изменил пароль при первом входе' if is_first_login else 'Пользователь изменил пароль', 'user', session['user_id'])
        return jsonify({'success': True, 'message': 'Password changed successfully'})
    else:
        return jsonify({'error': 'Failed to change password'}), 500

# API для получения логов действий пользователей
@app.route('/api/user-activity-logs')
@require_auth
@handle_errors
def get_user_activity_logs():
    """Получение логов действий пользователей"""
    user_role = session.get('user_role')
    if user_role not in ['admin', 'super_admin']:
        return jsonify({'error': 'Access denied'}), 403

    user_id = request.args.get('user_id', type=int)
    limit = request.args.get('limit', 100, type=int)
    offset = request.args.get('offset', 0, type=int)

    conn = get_db_connection()

    # Супер-админ видит все логи, админ - только логи менеджеров
    if user_role == 'super_admin':
        if user_id:
            logs = conn.execute('''
                SELECT al.*, u.username
                FROM activity_logs al
                JOIN users u ON al.user_id = u.id
                WHERE al.user_id = ?
                ORDER BY al.created_at DESC
                LIMIT ? OFFSET ?
            ''', (user_id, limit, offset)).fetchall()
        else:
            logs = conn.execute('''
                SELECT al.*, u.username
                FROM activity_logs al
                JOIN users u ON al.user_id = u.id
                ORDER BY al.created_at DESC
                LIMIT ? OFFSET ?
            ''', (limit, offset)).fetchall()
    else:
        # Админ видит логи менеджеров
        if user_id:
            logs = conn.execute('''
                SELECT al.*, u.username
                FROM activity_logs al
                JOIN users u ON al.user_id = u.id
                WHERE al.user_id = ? AND u.role = 'manager'
                ORDER BY al.created_at DESC
                LIMIT ? OFFSET ?
            ''', (user_id, limit, offset)).fetchall()
        else:
            logs = conn.execute('''
                SELECT al.*, u.username
                FROM activity_logs al
                JOIN users u ON al.user_id = u.id
                WHERE u.role = 'manager'
                ORDER BY al.created_at DESC
                LIMIT ? OFFSET ?
            ''', (limit, offset)).fetchall()

            # Соединение глобальное, не закрываем
    return jsonify([dict(log) for log in logs])

# ==================== МОДУЛЬ ПУЛА ЧАТОВ ====================

# API для взятия чата из пула
@app.route('/api/chats/<int:chat_id>/take', methods=['POST'])
@require_auth
@handle_errors
def take_chat_from_pool(chat_id):
    """Взять чат из пула (доступно для менеджеров и админов)"""
    conn = get_db_connection()
    user_id = session['user_id']
    user_role = session.get('user_role', 'manager')
    
    try:
        # Проверяем что чат существует
        chat = conn.execute('''
            SELECT assigned_manager_id, status FROM avito_chats WHERE id = ?
        ''', (chat_id,)).fetchone()
        
        if not chat:
            app.logger.warning(f"[TAKE CHAT] Чат {chat_id} не найден")
            return jsonify({'error': 'Chat not found', 'code': 'NOT_FOUND'}), 404
        
        # Проверяем что чат не завершен и не заблокирован
        if chat['status'] == 'completed':
            app.logger.warning(f"[TAKE CHAT] Попытка взять завершенный чат {chat_id}")
            return jsonify({'error': 'Cannot take completed chat', 'code': 'COMPLETED'}), 400
        
        if chat['status'] == 'blocked':
            app.logger.warning(f"[TAKE CHAT] Попытка взять заблокированный чат {chat_id}")
            return jsonify({'error': 'Cannot take blocked chat', 'code': 'BLOCKED'}), 400
        
        # Проверяем что чат в пуле (не назначен)
        if chat['assigned_manager_id'] is not None:
            app.logger.warning(f"[TAKE CHAT] Чат {chat_id} уже назначен менеджеру {chat['assigned_manager_id']}")
            return jsonify({
                'error': 'Chat is already assigned to another manager', 
                'code': 'ALREADY_ASSIGNED'
            }), 400
        
        # Назначаем чат пользователю (менеджеру или админу)
        conn.execute('''
            UPDATE avito_chats 
            SET assigned_manager_id = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (user_id, chat_id))
        
        # Логируем действие
        log_activity(user_id, 'take_chat', 
                    f'Взят чат из пула ID: {chat_id}', 'chat', chat_id)
        
        conn.commit()
        app.logger.info(f"[TAKE CHAT] Чат {chat_id} успешно взят пользователем {user_id} ({user_role})")
        return jsonify({'success': True, 'message': 'Chat taken successfully'}), 200
    except Exception as e:
        app.logger.error(f"[TAKE CHAT] Ошибка при взятии чата {chat_id}: {str(e)}", exc_info=True)
        return jsonify({'error': str(e), 'code': 'INTERNAL_ERROR'}), 500

# API для массового назначения чатов (batch)
@app.route('/api/chats/batch-take', methods=['POST'])
@require_auth
@handle_errors
def batch_take_chats():
    """Массовое назначение чатов текущему пользователю"""
    user_id = session['user_id']
    user_role = session.get('user_role', 'manager')
    
    data = request.get_json()
    if not data or 'chat_ids' not in data:
        return jsonify({'error': 'chat_ids required'}), 400
    
    chat_ids = data['chat_ids']
    if not isinstance(chat_ids, list) or len(chat_ids) == 0:
        return jsonify({'error': 'chat_ids must be a non-empty array'}), 400
    
    # Ограничиваем количество чатов за один запрос
    if len(chat_ids) > 100:
        return jsonify({'error': 'Maximum 100 chats per batch'}), 400
    
    conn = get_db_connection()
    results = {'success': [], 'errors': []}
    
    try:
        # Получаем информацию о чатах одним запросом
        placeholders = ','.join(['?'] * len(chat_ids))
        chats = conn.execute(f'''
            SELECT id, assigned_manager_id, status 
            FROM avito_chats 
            WHERE id IN ({placeholders})
        ''', chat_ids).fetchall()
        
        chats_dict = {chat['id']: chat for chat in chats}
        valid_chat_ids = []
        
        # Проверяем каждый чат
        for chat_id in chat_ids:
            chat_id = int(chat_id)
            chat = chats_dict.get(chat_id)
            
            if not chat:
                results['errors'].append({'chat_id': chat_id, 'error': 'NOT_FOUND'})
                continue
            
            if chat['status'] == 'completed':
                results['errors'].append({'chat_id': chat_id, 'error': 'COMPLETED'})
                continue
            
            if chat['status'] == 'blocked':
                results['errors'].append({'chat_id': chat_id, 'error': 'BLOCKED'})
                continue
            
            if chat['assigned_manager_id'] is not None:
                results['errors'].append({'chat_id': chat_id, 'error': 'ALREADY_ASSIGNED'})
                continue
            
            valid_chat_ids.append(chat_id)
        
        # Массовое обновление всех валидных чатов одним запросом
        if valid_chat_ids:
            valid_placeholders = ','.join(['?'] * len(valid_chat_ids))
            conn.execute(f'''
                UPDATE avito_chats 
                SET assigned_manager_id = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id IN ({valid_placeholders})
            ''', [user_id] + valid_chat_ids)
            
            # Логируем действие для каждого чата
            for chat_id in valid_chat_ids:
                log_activity(user_id, 'take_chat', 
                           f'Взят чат из пула ID: {chat_id}', 'chat', chat_id)
                results['success'].append(chat_id)
        
        conn.commit()
        app.logger.info(f"[BATCH TAKE] Обработано {len(valid_chat_ids)} чатов из {len(chat_ids)}")
        return jsonify({
            'success': True,
            'taken': results['success'],
            'errors': results['errors'],
            'count': len(results['success'])
        }), 200
        
    except Exception as e:
        conn.rollback()
        app.logger.error(f"[BATCH TAKE] Ошибка: {str(e)}", exc_info=True)
        return jsonify({'error': str(e), 'code': 'INTERNAL_ERROR'}), 500

# API для возврата чата в пул
@app.route('/api/chats/<int:chat_id>/return', methods=['POST'])
@require_auth
@handle_errors
def return_chat_to_pool(chat_id):
    """Вернуть чат в пул (доступно для менеджеров и админов)"""
    conn = get_db_connection()
    user_id = session['user_id']
    user_role = session.get('user_role', 'manager')
    
    try:
        # Проверяем что чат существует
        chat = conn.execute('''
            SELECT assigned_manager_id, status FROM avito_chats WHERE id = ?
        ''', (chat_id,)).fetchone()
        
        if not chat:
            app.logger.warning(f"[RETURN CHAT] Чат {chat_id} не найден")
            return jsonify({'error': 'Chat not found', 'code': 'NOT_FOUND'}), 404
        
        # Проверяем что чат назначен этому пользователю (или админ может вернуть любой чат)
        assigned_to = chat['assigned_manager_id']
        if assigned_to is None:
            app.logger.warning(f"[RETURN CHAT] Чат {chat_id} уже в пуле")
            return jsonify({'error': 'Chat is already in pool', 'code': 'ALREADY_IN_POOL'}), 400
        
        # Менеджер может вернуть только свой чат, админ - любой
        if user_role != 'admin' and assigned_to != user_id:
            app.logger.warning(f"[RETURN CHAT] Попытка вернуть чужой чат {chat_id} (назначен {assigned_to}, пытается {user_id})")
            return jsonify({
                'error': 'Chat is not assigned to you', 
                'code': 'NOT_ASSIGNED_TO_YOU'
            }), 403
        
        # Возвращаем чат в пул
        conn.execute('''
            UPDATE avito_chats 
            SET assigned_manager_id = NULL, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (chat_id,))
        
        # Логируем действие (с обработкой возможной ошибки)
        try:
            log_activity(user_id, 'return_chat', 
                    f'Возвращен чат в пул ID: {chat_id}', 'chat', chat_id)
        except Exception as log_error:
            app.logger.warning(f"[RETURN CHAT] Ошибка логирования: {log_error}")
        
        conn.commit()
        app.logger.info(f"[RETURN CHAT] Чат {chat_id} успешно возвращен в пул пользователем {user_id} ({user_role})")
        return jsonify({'success': True, 'message': 'Chat returned to pool successfully'}), 200
    except Exception as e:
        app.logger.error(f"[RETURN CHAT] Ошибка при возврате чата {chat_id}: {str(e)}", exc_info=True)
        return jsonify({'error': str(e), 'code': 'INTERNAL_ERROR'}), 500

# API для массового возврата всех чатов в пул
@app.route('/api/chats/return-all', methods=['POST'])
@require_auth
@handle_errors
def return_all_chats_to_pool():
    """Вернуть все чаты текущего пользователя в пул"""
    conn = get_db_connection()
    user_id = session['user_id']
    user_role = session.get('user_role', 'manager')
    
    try:
        # Менеджер может вернуть только свои чаты, админ - все чаты
        if user_role in ['admin', 'super_admin']:
            # Админ возвращает все чаты в пул
            result = conn.execute('''
                UPDATE avito_chats 
                SET assigned_manager_id = NULL, updated_at = CURRENT_TIMESTAMP
                WHERE assigned_manager_id IS NOT NULL
                AND status != 'completed'
                AND status != 'blocked'
            ''')
        else:
            # Менеджер возвращает только свои чаты
            result = conn.execute('''
                UPDATE avito_chats 
                SET assigned_manager_id = NULL, updated_at = CURRENT_TIMESTAMP
                WHERE assigned_manager_id = ?
                AND status != 'completed'
                AND status != 'blocked'
            ''', (user_id,))
        
        returned_count = result.rowcount
        
        # Логируем действие
        try:
            log_activity(user_id, 'return_all_chats', 
                        f'Возвращено {returned_count} чатов в пул', 'chat', None)
        except Exception as log_error:
            app.logger.warning(f"[RETURN ALL CHATS] Ошибка логирования: {log_error}")
        
        conn.commit()
        app.logger.info(f"[RETURN ALL CHATS] Возвращено {returned_count} чатов в пул пользователем {user_id} ({user_role})")
        return jsonify({
            'success': True, 
            'message': f'Возвращено {returned_count} чатов в пул',
            'count': returned_count
        }), 200
    except Exception as e:
        app.logger.error(f"[RETURN ALL CHATS] Ошибка при возврате всех чатов: {str(e)}", exc_info=True)
        return jsonify({'error': str(e), 'code': 'INTERNAL_ERROR'}), 500

# API для переноса клиента по флагам доставки
@app.route('/api/deliveries/<int:delivery_id>/move', methods=['POST'])
@require_auth
@handle_errors
def move_delivery_status(delivery_id):
    """Перенос доставки на следующий статус"""
    data = request.get_json() or {}
    if not isinstance(data, dict):
        return jsonify({'error': 'Invalid payload'}), 400
    new_status = data.get('status')
    
    if not new_status:
        return jsonify({'error': 'Status is required'}), 400
    
    valid_statuses = ['free', 'in_work', 'on_delivery', 'closed', 'refused']
    if new_status not in valid_statuses:
        return jsonify({'error': 'Invalid status'}), 400
    
    conn = get_db_connection()
    try:
        conn.execute('''
            UPDATE deliveries 
            SET delivery_status = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (new_status, delivery_id))
        
        # Логируем действие
        delivery = conn.execute('SELECT chat_id FROM deliveries WHERE id = ?', (delivery_id,)).fetchone()
        if delivery:
            log_activity(session['user_id'], 'move_delivery', 
                        f'Перенесена доставка на статус: {new_status}', 'delivery', delivery_id)
        
        conn.commit()
        # Соединение глобальное, не закрываем
        return jsonify({'success': True}), 200
    except Exception as e:
        # Соединение глобальное, не закрываем
        return jsonify({'error': str(e)}), 400

# API для удаления доставки (только админ)
@app.route('/api/deliveries/<int:delivery_id>', methods=['DELETE'])
@require_auth
@require_role('admin')
@handle_errors
def delete_delivery(delivery_id):
    """Удаление доставки (только админ)"""
    conn = get_db_connection()
    try:
        delivery = conn.execute('SELECT id, chat_id, manager_id, delivery_status, address, tracking_number, notes, created_at, updated_at FROM deliveries WHERE id = ?', (delivery_id,)).fetchone()
        if not delivery:
            # Соединение глобальное, не закрываем
            return jsonify({'error': 'Delivery not found'}), 404
        
        conn.execute('DELETE FROM deliveries WHERE id = ?', (delivery_id,))
        
        log_activity(session['user_id'], 'delete_delivery', 
                    f'Удалена доставка ID: {delivery_id}', 'delivery', delivery_id)
        
        conn.commit()
        # Соединение глобальное, не закрываем
        return jsonify({'success': True}), 200
    except Exception as e:
        # Соединение глобальное, не закрываем
        return jsonify({'error': str(e)}), 400

# ==================== АВТОМАТИЧЕСКАЯ СИНХРОНИЗАЦИЯ И ВЕБХУКИ ====================

def start_background_sync():
    """Запуск фоновой синхронизации чатов"""
    import threading
    import time
    import os
    
    def sync_worker():
        """Рабочий поток для периодической синхронизации"""
        SYNC_INTERVAL = int(os.environ.get('SYNC_INTERVAL', 300))  # По умолчанию 5 минут
        
        # Ждем немного перед первой синхронизацией, чтобы приложение успело запуститься
        time.sleep(10)
        
        app.logger.info(f"[AUTO SYNC] Запуск автоматической синхронизации (интервал: {SYNC_INTERVAL} сек)")
        
        while True:
            try:
                app.logger.info("[AUTO SYNC] Начало автоматической синхронизации чатов...")
                result = sync_chats_from_avito()
                if result.get('success'):
                    app.logger.info(f"[AUTO SYNC] Синхронизация завершена: {result.get('synced_count', 0)} чатов")
                else:
                    app.logger.warning(f"[AUTO SYNC] Ошибка синхронизации: {result.get('error', 'Unknown')}")
            except Exception as e:
                app.logger.error(f"[AUTO SYNC] Критическая ошибка синхронизации: {e}", exc_info=True)
            
            time.sleep(SYNC_INTERVAL)
    
    # Запускаем в отдельном потоке
    sync_thread = threading.Thread(target=sync_worker, daemon=True, name="AutoSyncThread")
    sync_thread.start()
    app.logger.info("[AUTO SYNC] Фоновая синхронизация запущена")

def register_webhooks_for_all_shops():
    """Регистрация вебхуков для всех активных магазинов при старте"""
    import os
    from avito_api import AvitoAPI
    
    try:
        conn = get_db_connection()
        
        # Получаем все активные магазины с OAuth ключами
        shops = conn.execute('''
            SELECT id, name, client_id, client_secret, user_id, webhook_registered
            FROM avito_shops
            WHERE is_active = 1
            AND client_id IS NOT NULL
            AND client_secret IS NOT NULL
            AND user_id IS NOT NULL
        ''').fetchall()
        
        if not shops:
            app.logger.info("[WEBHOOK REGISTRATION] Нет активных магазинов для регистрации вебхуков")
            return
        
        # Получаем URL для webhook из переменных окружения или формируем
        webhook_url = os.getenv('AVITO_WEBHOOK_URL')
        if not webhook_url:
            # Формируем URL из переменных окружения или используем дефолтный
            host = os.getenv('WEBHOOK_HOST', 'localhost:5000')
            scheme = 'https' if os.getenv('WEBHOOK_HTTPS', 'false').lower() == 'true' else 'http'
            webhook_url = f"{scheme}://{host}/webhook/avito"
        
        app.logger.info(f"[WEBHOOK REGISTRATION] Регистрация вебхуков для {len(shops)} магазинов: {webhook_url}")
        
        registered = 0
        failed = 0
        
        for shop in shops:
            try:
                # Пропускаем, если вебхук уже зарегистрирован
                if shop['webhook_registered']:
                    app.logger.debug(f"[WEBHOOK REGISTRATION] Магазин {shop['name']} уже имеет зарегистрированный вебхук")
                    continue
                
                api = AvitoAPI(client_id=shop['client_id'], client_secret=shop['client_secret'])
                webhook_result = api.register_webhook_v3(
                    url=webhook_url,
                    types=['message', 'chat']
                )
                
                if webhook_result:
                    conn.execute('''
                        UPDATE avito_shops 
                        SET webhook_registered = 1 
                        WHERE id = ?
                    ''', (shop['id'],))
                    conn.commit()
                    registered += 1
                    app.logger.info(f"[WEBHOOK REGISTRATION] ✅ Вебхук зарегистрирован для магазина {shop['name']}")
                else:
                    failed += 1
                    app.logger.warning(f"[WEBHOOK REGISTRATION] ⚠️ Не удалось зарегистрировать вебхук для магазина {shop['name']}")
                    
            except Exception as e:
                failed += 1
                app.logger.error(f"[WEBHOOK REGISTRATION] Ошибка регистрации вебхука для магазина {shop['name']}: {e}", exc_info=True)
        
        app.logger.info(f"[WEBHOOK REGISTRATION] Завершено: зарегистрировано {registered}, ошибок {failed}")
        
    except Exception as e:
        app.logger.error(f"[WEBHOOK REGISTRATION] Критическая ошибка: {e}", exc_info=True)

# Запускаем фоновые задачи при импорте модуля (для Passenger)
# Это выполнится при запуске через passenger_wsgi.py
# Используем флаг, чтобы не запускать дважды
if not hasattr(app, '_background_tasks_started'):
    try:
        app.logger.info("[INIT] Запуск фоновых задач (вебхуки и синхронизация)...")
        # Регистрируем вебхуки при старте
        register_webhooks_for_all_shops()
        # Запускаем автоматическую синхронизацию
        start_background_sync()
        app._background_tasks_started = True
        app.logger.info("[INIT] Фоновые задачи запущены")
    except Exception as e:
        app.logger.error(f"[INIT] Ошибка запуска фоновых задач: {e}", exc_info=True)

# Запускаем сервер (только при прямом запуске python app.py)
if __name__ == '__main__':
    print("[START] Запускаем CRM систему...")
    from database import _DB_PATH
    print(f"[INFO] База данных: {_DB_PATH}")
    print("[INFO] Тестовые пользователи:")
    print("   Админ: admin@osagaming.com / admin123")
    print("   Менеджер: dannnnnbb@gmail.com / manager123")
    print("[INFO] Сервер доступен по адресу: http://localhost:5000")
    
    app.run(debug=True, port=5000)