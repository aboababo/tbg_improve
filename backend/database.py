"""
OSAGAMING CRM - Модуль работы с базой данных
============================================

Этот файл содержит:
- Функции инициализации базы данных
- Создание всех необходимых таблиц
- Создание индексов для оптимизации производительности
- Добавление тестовых данных
- Функции для получения соединения с БД

База данных: SQLite (osagaming_crm.db)
Автор: OSAGAMING Development Team
Версия: 2.0
"""

import sqlite3
import json
import os
from datetime import datetime
import hashlib

# Определяем путь к базе данных относительно файла database.py
# Это гарантирует, что база данных всегда находится в папке backend/
# Используем os.path.abspath для получения абсолютного пути независимо от рабочей директории
_DB_DIR = os.path.dirname(os.path.abspath(__file__))
_DB_PATH = os.path.join(_DB_DIR, 'osagaming_crm.db')

# Нормализуем путь (убираем двойные слеши и т.д.)
_DB_PATH = os.path.normpath(_DB_PATH)


def init_database():
    """
    Инициализация базы данных для CRM системы
    
    Создает все необходимые таблицы, индексы и добавляет тестовые данные.
    Эта функция вызывается при старте приложения и гарантирует, что
    база данных готова к работе.
    
    Процесс инициализации:
    1. Создание таблиц (если не существуют)
    2. Добавление недостающих колонок (миграции)
    3. Создание индексов для оптимизации
    4. Добавление тестовых данных (если БД пустая)
    5. Добавление дефолтных настроек системы
    
    Таблицы:
        - users: Пользователи системы (админы и менеджеры)
        - avito_shops: Магазины на Авито
        - avito_chats: Чаты с клиентами
        - avito_messages: Сообщения в чатах
        - deliveries: Доставки товаров
        - work_schedules: График работы менеджеров
        - И другие...
    
    Returns:
        None (функция не возвращает значение, но выводит статус в консоль)
    """
    # Убеждаемся, что директория существует
    db_dir = os.path.dirname(_DB_PATH)
    if not os.path.exists(db_dir):
        try:
            os.makedirs(db_dir, mode=0o755, exist_ok=True)
        except Exception as e:
            raise RuntimeError(
                f"Cannot create database directory: {db_dir}\n"
                f"Error: {e}\n"
                f"Current working directory: {os.getcwd()}\n"
                f"Please create it manually: mkdir -p {db_dir} && chmod 755 {db_dir}"
            ) from e
    
    # Проверяем права на запись в директорию
    if not os.access(db_dir, os.W_OK):
        raise RuntimeError(
            f"No write permission for database directory: {db_dir}\n"
            f"Current working directory: {os.getcwd()}\n"
            f"Please run: chmod 755 {db_dir}"
        )
    
    # Подключаемся к базе данных SQLite
    # Если файл не существует, он будет создан автоматически
    try:
        conn = sqlite3.connect(_DB_PATH, timeout=10.0)
    except sqlite3.OperationalError as e:
        error_msg = str(e).lower()
        if "unable to open database file" in error_msg:
            raise RuntimeError(
                f"Cannot open database file: {_DB_PATH}\n"
                f"Directory: {db_dir}\n"
                f"Directory exists: {os.path.exists(db_dir)}\n"
                f"Directory writable: {os.access(db_dir, os.W_OK)}\n"
                f"Current working directory: {os.getcwd()}\n"
                f"Please check permissions: chmod 755 {db_dir}"
            ) from e
        raise
    cursor = conn.cursor()
    
    # json уже импортирован глобально в начале файла (строка 18)
    # ВАЖНО: Не добавляйте локальный import json внутри этой функции!

    # ==================== СОЗДАНИЕ ТАБЛИЦ ====================
    
    # Таблица пользователей с ролями
    # Хранит информацию о всех пользователях системы (администраторы и менеджеры)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,           -- Уникальный идентификатор пользователя
        username TEXT NOT NULL,                         -- Имя пользователя для отображения
        email TEXT UNIQUE NOT NULL,                     -- Email (уникальный, используется для входа)
        password TEXT NOT NULL,                         -- Хеш пароля (SHA256)
        role TEXT DEFAULT 'manager',                     -- Роль: 'manager', 'admin', 'super_admin'
        is_active BOOLEAN DEFAULT 1,                    -- Активен ли аккаунт (можно деактивировать)
        salary DECIMAL(10,2) DEFAULT 0,                  -- Зарплата менеджера
        kpi_score DECIMAL(5,2) DEFAULT 0,                -- KPI балл (0-100)
        temp_password TEXT,                              -- Одноразовый пароль (для новых менеджеров)
        password_changed BOOLEAN DEFAULT 0,              -- Изменил ли менеджер пароль после первого входа
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Дата создания аккаунта
        created_by INTEGER,                              -- Кто создал аккаунт
        settings TEXT DEFAULT '{}'                      -- JSON строка с настройками пользователя
    )
    ''')

    # Добавляем недостающие колонки для пользователей
    try:
        cursor.execute('ALTER TABLE users ADD COLUMN temp_password TEXT')
    except Exception:
        pass
    try:
        cursor.execute('ALTER TABLE users ADD COLUMN password_changed BOOLEAN DEFAULT 0')
    except Exception:
        pass
    try:
        cursor.execute('ALTER TABLE users ADD COLUMN created_by INTEGER')
    except Exception:
        pass
    # Добавляем недостающие колонки для пользователей
    try:
        cursor.execute('ALTER TABLE users ADD COLUMN first_name TEXT')
        conn.commit()
        print("[MIGRATION] ✅ Добавлена колонка first_name в таблицу users")
    except sqlite3.OperationalError as e:
        error_msg = str(e).lower()
        # Колонка уже существует - это нормально
        if "duplicate column name" in error_msg or "already exists" in error_msg:
            print("[MIGRATION] ℹ️ Колонка first_name уже существует")
        else:
            print(f"[MIGRATION] ⚠️ Предупреждение при добавлении first_name: {e}")
    except Exception as e:
        print(f"[MIGRATION] ⚠️ Неожиданная ошибка при добавлении first_name: {e}")
    
    try:
        cursor.execute('ALTER TABLE users ADD COLUMN last_name TEXT')
        conn.commit()
        print("[MIGRATION] ✅ Добавлена колонка last_name в таблицу users")
    except sqlite3.OperationalError as e:
        error_msg = str(e).lower()
        # Колонка уже существует - это нормально
        if "duplicate column name" in error_msg or "already exists" in error_msg:
            print("[MIGRATION] ℹ️ Колонка last_name уже существует")
        else:
            print(f"[MIGRATION] ⚠️ Предупреждение при добавлении last_name: {e}")
    except Exception as e:
        print(f"[MIGRATION] ⚠️ Неожиданная ошибка при добавлении last_name: {e}")

    # Таблица магазинов Авито
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS avito_shops (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        shop_url TEXT UNIQUE NOT NULL,
        api_key TEXT,
        is_active BOOLEAN DEFAULT 1,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # Добавляем недостающие колонки для OAuth авторизации магазинов
    try:
        cursor.execute('ALTER TABLE avito_shops ADD COLUMN client_id TEXT')
    except Exception:
        pass
    try:
        cursor.execute('ALTER TABLE avito_shops ADD COLUMN client_secret TEXT')
    except Exception:
        pass
    try:
        cursor.execute('ALTER TABLE avito_shops ADD COLUMN user_id INTEGER')
    except Exception:
        pass
    try:
        cursor.execute('ALTER TABLE avito_shops ADD COLUMN webhook_registered BOOLEAN')
    except Exception:
        pass
    try:
        cursor.execute('ALTER TABLE avito_shops ADD COLUMN token_checked_at TIMESTAMP')
    except Exception:
        pass
    try:
        cursor.execute('ALTER TABLE avito_shops ADD COLUMN token_status TEXT')
    except Exception:
        pass
    # Признак зарегистрированного вебхука (может отсутствовать в старых БД)
    try:
        cursor.execute('ALTER TABLE avito_shops ADD COLUMN webhook_registered BOOLEAN DEFAULT 0')
    except Exception:
        pass

    # Таблица назначений менеджеров на магазины
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS manager_assignments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        manager_id INTEGER,
        shop_id INTEGER,
        assigned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (manager_id) REFERENCES users (id),
        FOREIGN KEY (shop_id) REFERENCES avito_shops (id)
    )
    ''')

    # Таблица чатов
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS avito_chats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        shop_id INTEGER,
        chat_id TEXT UNIQUE NOT NULL,
        client_name TEXT,
        client_phone TEXT,
        product_url TEXT,
        last_message TEXT,
        priority TEXT DEFAULT 'new', -- urgent/new/active/waiting/delivery
        status TEXT DEFAULT 'active',
        unread_count INTEGER DEFAULT 0,
        response_timer INTEGER DEFAULT 0, -- время в минутах
        customer_id TEXT, -- идентификатор клиента в Avito
        assigned_manager_id INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (shop_id) REFERENCES avito_shops (id),
        FOREIGN KEY (assigned_manager_id) REFERENCES users (id)
    )
    ''')

    # Колонка customer_id могла отсутствовать в ранних версиях
    try:
        cursor.execute('ALTER TABLE avito_chats ADD COLUMN customer_id TEXT')
    except Exception:
        pass
    
    # Колонка listing_data для хранения данных объявления из синхронизации
    try:
        cursor.execute('ALTER TABLE avito_chats ADD COLUMN listing_data TEXT')
    except Exception:
        pass

    # Таблица сообщений
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS avito_messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id INTEGER,
        message_text TEXT NOT NULL,
        message_type TEXT DEFAULT 'incoming', -- incoming/outgoing
        sender_name TEXT,
        manager_id INTEGER, -- ID менеджера, отправившего сообщение
        is_read BOOLEAN DEFAULT 0,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (chat_id) REFERENCES avito_chats (id),
        FOREIGN KEY (manager_id) REFERENCES users (id)
    )
    ''')
    
    # Добавляем колонку manager_id если её нет
    try:
        cursor.execute('ALTER TABLE avito_messages ADD COLUMN manager_id INTEGER')
    except:
        pass  # Колонка уже существует

    # Таблица объявлений
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS avito_listings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        listing_id TEXT UNIQUE,
        title TEXT,
        price REAL DEFAULT 0,
        url TEXT,
        image_url TEXT,
        location TEXT,
        description TEXT,
        category TEXT,
        status TEXT DEFAULT 'new', -- new/in_work/sold/archived
        param_id INTEGER,
        assigned_manager_id INTEGER,
        notes TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (assigned_manager_id) REFERENCES users (id)
    )
    ''')
    
    # Таблица кэша объявлений для чатов (чтобы не делать запросы к Avito API каждый раз)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS chat_listing_cache (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id INTEGER UNIQUE NOT NULL,
        item_id TEXT NOT NULL,
        product_url TEXT,
        title TEXT,
        price REAL,
        price_info TEXT, -- JSON с дополнительной информацией о цене
        description TEXT,
        status TEXT, -- active/archived/sold и т.д.
        category TEXT,
        category_name TEXT,
        location TEXT,
        address TEXT,
        images TEXT, -- JSON массив URL изображений
        main_image_url TEXT,
        listing_data TEXT, -- Полные данные объявления в JSON
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (chat_id) REFERENCES avito_chats (id) ON DELETE CASCADE,
        UNIQUE(chat_id, item_id)
    )
    ''')
    
    # Создаем индекс для быстрого поиска по chat_id
    try:
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_chat_listing_cache_chat_id ON chat_listing_cache(chat_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_chat_listing_cache_item_id ON chat_listing_cache(item_id)')
    except Exception:
        pass

    # Таблица сохраненных параметров поиска
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS search_params (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        name TEXT,
        query TEXT,
        category_id INTEGER,
        location_id INTEGER,
        price_min REAL,
        price_max REAL,
        limit_results INTEGER DEFAULT 50,
        is_active BOOLEAN DEFAULT 1,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users (id)
    )
    ''')

    # Таблица доставок
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS deliveries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id INTEGER,
        manager_id INTEGER,
        delivery_status TEXT DEFAULT 'free', -- free/in_work/on_delivery/closed/refused
        address TEXT,
        tracking_number TEXT,
        notes TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (chat_id) REFERENCES avito_chats (id),
        FOREIGN KEY (manager_id) REFERENCES users (id)
    )
    ''')

    # Таблица KPI настроек
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS kpi_settings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        parameter_name TEXT UNIQUE NOT NULL, -- response_time, conversion, etc
        weight DECIMAL(3,2) DEFAULT 1.0,
        min_value DECIMAL(5,2) DEFAULT 0,
        penalty_amount DECIMAL(10,2) DEFAULT 0,
        bonus_amount DECIMAL(10,2) DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # Таблица тем и настроек
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS user_settings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER UNIQUE,
        theme TEXT DEFAULT 'dark', -- dark/light
        colors TEXT DEFAULT '{}',
        sound_alerts BOOLEAN DEFAULT 1,
        push_notifications BOOLEAN DEFAULT 1,
        tab_visibility TEXT DEFAULT NULL, -- JSON с настройками видимости вкладок
        FOREIGN KEY (user_id) REFERENCES users (id)
    )
    ''')
    
    # Добавляем колонку tab_visibility если её нет
    try:
        cursor.execute('ALTER TABLE user_settings ADD COLUMN tab_visibility TEXT DEFAULT NULL')
    except Exception:
        pass  # Колонка уже существует

    # Таблица клиентов
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS clients (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        phone TEXT UNIQUE NOT NULL,
        email TEXT,
        notes TEXT,
        total_orders INTEGER DEFAULT 0,
        total_spent DECIMAL(10,2) DEFAULT 0,
        is_blacklisted BOOLEAN DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # Таблица тегов клиентов
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS client_tags (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_id INTEGER,
        tag_name TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (client_id) REFERENCES clients (id),
        UNIQUE(client_id, tag_name)
    )
    ''')

    # Таблица заказов клиентов
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS client_orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_id INTEGER,
        chat_id INTEGER,
        order_number TEXT UNIQUE,
        product_name TEXT,
        amount DECIMAL(10,2),
        status TEXT DEFAULT 'pending', -- pending/completed/cancelled
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (client_id) REFERENCES clients (id),
        FOREIGN KEY (chat_id) REFERENCES avito_chats (id)
    )
    ''')

    # Таблица черного списка
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS blacklist (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_id INTEGER,
        phone TEXT,
        reason TEXT,
        added_by INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (client_id) REFERENCES clients (id),
        FOREIGN KEY (added_by) REFERENCES users (id)
    )
    ''')

    # Таблица шаблонов ответов
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS message_templates (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        content TEXT NOT NULL,
        category TEXT DEFAULT 'general', -- greeting/response/closing/etc
        created_by INTEGER,
        is_active BOOLEAN DEFAULT 1,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (created_by) REFERENCES users (id)
    )
    ''')

    # Таблица быстрых ответов
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS quick_replies (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        shortcut TEXT UNIQUE NOT NULL, -- например, /привет
        message TEXT NOT NULL,
        created_by INTEGER,
        is_active BOOLEAN DEFAULT 1,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (created_by) REFERENCES users (id)
    )
    ''')

    # Таблица настроек системы
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS system_settings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        setting_key TEXT UNIQUE NOT NULL,
        setting_value TEXT NOT NULL,
        setting_type TEXT DEFAULT 'string', -- string/number/boolean/json
        description TEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # Таблица автоматизации
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS automation_rules (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        trigger_type TEXT NOT NULL, -- new_chat/time_based/keyword
        trigger_condition TEXT, -- JSON с условиями
        action_type TEXT NOT NULL, -- auto_reply/assign/priority
        action_data TEXT, -- JSON с данными действия
        is_active BOOLEAN DEFAULT 1,
        created_by INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (created_by) REFERENCES users (id)
    )
    ''')

    # Таблица истории KPI
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS kpi_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        period_start DATE,
        period_end DATE,
        response_time_avg DECIMAL(5,2),
        conversion_rate DECIMAL(5,2),
        customer_satisfaction DECIMAL(5,2),
        messages_per_chat DECIMAL(5,2),
        total_score DECIMAL(5,2),
        bonus_amount DECIMAL(10,2) DEFAULT 0,
        penalty_amount DECIMAL(10,2) DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users (id)
    )
    ''')

    # Таблица аналитики
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS analytics_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_type TEXT NOT NULL, -- message_sent/chat_created/order_created
        user_id INTEGER,
        chat_id INTEGER,
        shop_id INTEGER,
        metadata TEXT, -- JSON с дополнительными данными
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users (id),
        FOREIGN KEY (chat_id) REFERENCES avito_chats (id),
        FOREIGN KEY (shop_id) REFERENCES avito_shops (id)
    )
    ''')

    # Таблица прав доступа ролей
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS role_permissions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        role TEXT NOT NULL, -- admin/manager
        permission_key TEXT NOT NULL, -- manage_shops/view_analytics/etc
        is_allowed BOOLEAN DEFAULT 1,
        UNIQUE(role, permission_key)
    )
    ''')

    # Таблица смен (shifts)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS shifts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        manager_id INTEGER NOT NULL,
        shift_date DATE NOT NULL,
        shift_start_time TIMESTAMP,
        shift_end_time TIMESTAMP,
        is_late BOOLEAN DEFAULT 0,
        late_minutes INTEGER DEFAULT 0,
        status TEXT DEFAULT 'active', -- active/completed/cancelled
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (manager_id) REFERENCES users (id),
        UNIQUE(manager_id, shift_date)
    )
    ''')

    # Таблица штрафов (penalties)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS penalties (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        manager_id INTEGER NOT NULL,
        shift_id INTEGER,
        penalty_type TEXT NOT NULL, -- late_shift/poor_performance/etc
        penalty_amount DECIMAL(10,2) NOT NULL,
        reason TEXT,
        is_paid BOOLEAN DEFAULT 0,
        created_by INTEGER, -- admin who created penalty
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        paid_at TIMESTAMP,
        FOREIGN KEY (manager_id) REFERENCES users (id),
        FOREIGN KEY (shift_id) REFERENCES shifts (id),
        FOREIGN KEY (created_by) REFERENCES users (id)
    )
    ''')

    # Таблица логов действий (activity_logs)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS activity_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        action_type TEXT NOT NULL, -- login/logout/send_message/open_chat/complete_chat/etc
        action_description TEXT,
        target_type TEXT, -- chat/user/shop/etc
        target_id INTEGER,
        metadata TEXT, -- JSON с дополнительными данными
        ip_address TEXT,
        user_agent TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users (id)
    )
    ''')

    # Таблица графика работы (work_schedules)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS work_schedules (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        day_of_week INTEGER NOT NULL, -- 0=Понедельник, 1=Вторник, ..., 6=Воскресенье
        start_time TIME NOT NULL, -- Время начала работы (например, 09:00)
        end_time TIME NOT NULL, -- Время окончания работы (например, 18:00)
        is_working_day BOOLEAN DEFAULT 1, -- Рабочий день или выходной
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users (id),
        UNIQUE(user_id, day_of_week)
    )
    ''')

    # Таблица назначения менеджеров на дни недели (для общего графика)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS day_manager_assignments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        day_of_week INTEGER NOT NULL, -- 0=Понедельник, 1=Вторник, ..., 6=Воскресенье
        manager_id INTEGER NOT NULL,
        start_time TIME, -- Время начала работы (например, 09:00)
        end_time TIME, -- Время окончания работы (например, 18:00)
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (manager_id) REFERENCES users (id),
        UNIQUE(day_of_week, manager_id)
    )
    ''')

    # Добавляем тестовые данные
    cursor.execute("SELECT COUNT(*) FROM users")
    if cursor.fetchone()[0] == 0:
        # Хешируем пароли
        def hash_password(password):
            return hashlib.sha256(password.encode()).hexdigest()

        # Добавляем супер-администратора
        cursor.execute('''
        INSERT INTO users (username, email, password, role, salary, settings, password_changed)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            'Супер Администратор',
            'admin@osagaming.com',
            hash_password('admin123'),
            'super_admin',
            0,
            json.dumps({'theme': 'dark', 'notifications': True}),
            1  # Супер-админ уже изменил пароль
        ))

        # Добавляем менеджера
        cursor.execute('''
        INSERT INTO users (username, email, password, role, salary, kpi_score, settings)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            'Адлан Джабраилов',
            'dannnnnbb@gmail.com',
            hash_password('manager123'),
            'manager',
            50000,
            85.5,
            json.dumps({'theme': 'dark', 'sound_alerts': True})
        ))

        # Добавляем тестовые магазины
        cursor.execute('''
        INSERT INTO avito_shops (name, shop_url, api_key)
        VALUES (?, ?, ?)
        ''', ('Магазин электроники', 'https://www.avito.ru/user123', 'test_api_key_1'))

        cursor.execute('''
        INSERT INTO avito_shops (name, shop_url, api_key) 
        VALUES (?, ?, ?)
        ''', ('Магазин одежды', 'https://www.avito.ru/user456', 'test_api_key_2'))

        # Назначаем менеджера на магазины
        cursor.execute('''
        INSERT INTO manager_assignments (manager_id, shop_id)
        VALUES (?, ?)
        ''', (2, 1))

        cursor.execute('''
        INSERT INTO manager_assignments (manager_id, shop_id)
        VALUES (?, ?)
        ''', (2, 2))

        # Добавляем KPI настройки
        kpi_parameters = [
            ('response_time', 0.3, 10, 500, 1000),
            ('conversion_rate', 0.4, 20, 300, 800),
            ('customer_satisfaction', 0.2, 80, 200, 500),
            ('messages_per_chat', 0.1, 5, 100, 300)
        ]

        for param in kpi_parameters:
            cursor.execute('''
            INSERT INTO kpi_settings (parameter_name, weight, min_value, penalty_amount, bonus_amount)
            VALUES (?, ?, ?, ?, ?)
            ''', param)

        # Добавляем настройки пользователей
        cursor.execute('INSERT INTO user_settings (user_id) VALUES (?)', (1,))
        cursor.execute('INSERT INTO user_settings (user_id) VALUES (?)', (2,))

        # Добавляем дефолтные настройки системы
        # json уже импортирован глобально
        default_settings = [
            ('timer_new_chat_hours', '1', 'number', 'Время для "нового" чата (часы)'),
            ('timer_urgent_minutes', '20', 'number', 'Время для "срочного" чата (минуты)'),
            ('timer_critical_minutes', '60', 'number', 'Критическое время ответа (минуты)'),
            ('notification_blink_interval', '1000', 'number', 'Интервал мигания уведомлений (мс)'),
            ('color_urgent', '#ef4444', 'string', 'Цвет для срочных чатов'),
            ('color_new', '#f59e0b', 'string', 'Цвет для новых чатов'),
            ('color_active', '#10b981', 'string', 'Цвет для активных чатов'),
            ('color_delivery', '#8b5cf6', 'string', 'Цвет для доставок'),
            ('sound_enabled', 'true', 'boolean', 'Включить звуковые уведомления'),
            ('push_enabled', 'true', 'boolean', 'Включить push-уведомления'),
            ('email_enabled', 'false', 'boolean', 'Включить email оповещения'),
            ('telegram_enabled', 'false', 'boolean', 'Включить Telegram бот'),
            # Настройки видимости вкладок для админа (по умолчанию все включены)
            ('tab_visibility_admin', json.dumps({
                'dashboard': True,
                'chats': True,
                'buyout': True,
                'deliveries': True,
                'quick_replies': True,
                'shops': True,
                'analytics': True,
                'settings': True
            }), 'json', 'Видимость вкладок для админа'),
            # Настройки видимости вкладок для менеджера (по умолчанию все включены, кроме админских)
            ('tab_visibility_manager', json.dumps({
                'dashboard': True,
                'chats': True,
                'buyout': True,
                'deliveries': True,
                'quick_replies': True,
                'shops': False,
                'analytics': False,
                'settings': False
            }), 'json', 'Видимость вкладок для менеджера'),
        ]

        for setting in default_settings:
            cursor.execute('''
                INSERT OR IGNORE INTO system_settings (setting_key, setting_value, setting_type, description)
                VALUES (?, ?, ?, ?)
            ''', setting)

        # Добавляем права доступа для ролей
        admin_permissions = [
            ('admin', 'manage_users', 1),
            ('admin', 'manage_shops', 1),
            ('admin', 'manage_settings', 1),
            ('admin', 'view_analytics', 1),
            ('admin', 'manage_kpi', 1),
            ('admin', 'manage_automation', 1),
            ('admin', 'view_all_chats', 1),
            ('admin', 'manage_clients', 1),
        ]

        super_admin_permissions = [
            ('super_admin', 'manage_users', 1),
            ('super_admin', 'manage_shops', 1),
            ('super_admin', 'manage_settings', 1),
            ('super_admin', 'view_analytics', 1),
            ('super_admin', 'manage_kpi', 1),
            ('super_admin', 'manage_automation', 1),
            ('super_admin', 'view_all_chats', 1),
            ('super_admin', 'manage_clients', 1),
            ('super_admin', 'view_logs', 1),
            ('super_admin', 'manage_admins', 1),
            ('super_admin', 'system_control', 1),
        ]

        manager_permissions = [
            ('manager', 'view_own_chats', 1),
            ('manager', 'send_messages', 1),
            ('manager', 'view_own_analytics', 1),
            ('manager', 'use_templates', 1),
            ('manager', 'manage_deliveries', 1),
        ]

        for perm in admin_permissions + super_admin_permissions + manager_permissions:
            cursor.execute('''
                INSERT OR IGNORE INTO role_permissions (role, permission_key, is_allowed)
                VALUES (?, ?, ?)
            ''', perm)

        # Добавляем дефолтные шаблоны ответов
        default_templates = [
            ('Приветствие', 'Здравствуйте! Спасибо за интерес к нашему товару. Чем могу помочь?', 'greeting', 1),
            ('Уточнение наличия', 'Да, товар в наличии. Могу ответить на ваши вопросы.', 'response', 1),
            ('Прощание', 'Спасибо за обращение! Если возникнут вопросы, обращайтесь.', 'closing', 1),
        ]

        for template in default_templates:
            cursor.execute('''
                INSERT INTO message_templates (name, content, category, created_by)
                VALUES (?, ?, ?, ?)
            ''', template)

        # Добавляем дефолтные быстрые ответы
        default_quick_replies = [
            ('/привет', 'Здравствуйте! Чем могу помочь?', 1),
            ('/наличие', 'Да, товар в наличии. Могу ответить на ваши вопросы.', 1),
            ('/цена', 'Цена актуальна. Могу предоставить дополнительную информацию.', 1),
        ]

        for reply in default_quick_replies:
            cursor.execute('''
                INSERT INTO quick_replies (shortcut, message, created_by)
                VALUES (?, ?, ?)
            ''', reply)

        # Добавляем тестовые чаты для демонстрации
        test_chats = [
            # СРОЧНЫЕ чаты (>20 минут)
            (1, 'chat_001', 'Иван Петров', '+79161234567', 'https://www.avito.ru/iphone',
             'Здравствуйте! Интересует iPhone 13. Есть в наличии?', 'urgent', 'active', 1, 25, 2),
            (1, 'chat_002', 'Мария Сидорова', '+79167654321', 'https://www.avito.ru/macbook',
             'Срочно нужен MacBook Pro! Цена актуальна?', 'urgent', 'active', 2, 35, 2),

            # НОВЫЕ чаты (<1 часа)
            (2, 'chat_003', 'Алексей Козлов', '+79169998877', 'https://www.avito.ru/jacket',
             'Добрый день! Какой размер посоветуете?', 'new', 'active', 0, 5, 2),
            (1, 'chat_004', 'Елена Васнецова', '+79165554433', 'https://www.avito.ru/airpods',
             'Здравствуйте! AirPods Pro есть в наличии?', 'new', 'active', 0, 15, 2),

            # АКТИВНЫЕ чаты
            (2, 'chat_005', 'Дмитрий Орлов', '+79162223344', 'https://www.avito.ru/shoes', 'Спасибо! Жду доставку',
             'active', 'active', 0, 120, 2),
            (1, 'chat_006', 'Ольга Новикова', '+79163332211', 'https://www.avito.ru/watch', 'Уточните гарантию',
             'active', 'active', 1, 180, 2),

            # ЧАТЫ В ДОСТАВКЕ
            (2, 'chat_007', 'Сергей Волков', '+79164445566', 'https://www.avito.ru/dress', 'Заказ получен, спасибо!',
             'delivery', 'completed', 0, 300, 2),
            (1, 'chat_008', 'Анна Морозова', '+79167778899', 'https://www.avito.ru/camera', 'Когда будет доставка?',
             'delivery', 'processing', 0, 250, 2),
        ]

        for chat in test_chats:
            cursor.execute('''
            INSERT INTO avito_chats (shop_id, chat_id, client_name, client_phone, product_url, last_message, priority, status, unread_count, response_timer, assigned_manager_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', chat)

        # Добавляем тестовые сообщения
        test_messages = [
            (1, 'Здравствуйте! Интересует iPhone 13. Есть в наличии?', 'incoming', 'Иван Петров', 0),
            (2, 'Срочно нужен MacBook Pro! Цена актуальна?', 'incoming', 'Мария Сидорова', 0),
            (3, 'Добрый день! Какой размер посоветуете?', 'incoming', 'Алексей Козлов', 0),
            (4, 'Здравствуйте! AirPods Pro есть в наличии?', 'incoming', 'Елена Васнецова', 0),
            (5, 'Спасибо! Жду доставку', 'incoming', 'Дмитрий Орлов', 1),
            (6, 'Уточните гарантию', 'incoming', 'Ольга Новикова', 0),
            (7, 'Заказ получен, спасибо!', 'incoming', 'Сергей Волков', 1),
            (8, 'Когда будет доставка?', 'incoming', 'Анна Морозова', 0),
        ]

        for msg in test_messages:
            cursor.execute('''
            INSERT INTO avito_messages (chat_id, message_text, message_type, sender_name, is_read)
            VALUES (?, ?, ?, ?, ?)
            ''', msg)

        print("[OK] Тестовые чаты и сообщения добавлены")

    # Создаем индексы для оптимизации производительности
    indexes = [
        # Индексы для таблицы доставок
        "CREATE INDEX IF NOT EXISTS idx_deliveries_manager_id ON deliveries(manager_id)",
        "CREATE INDEX IF NOT EXISTS idx_deliveries_chat_id ON deliveries(chat_id)",
        "CREATE INDEX IF NOT EXISTS idx_deliveries_status ON deliveries(delivery_status)",
        "CREATE INDEX IF NOT EXISTS idx_deliveries_updated_at ON deliveries(updated_at DESC)",
        
        # Индексы для таблицы чатов
        "CREATE INDEX IF NOT EXISTS idx_chats_shop_id ON avito_chats(shop_id)",
        "CREATE INDEX IF NOT EXISTS idx_chats_manager_id ON avito_chats(assigned_manager_id)",
        "CREATE INDEX IF NOT EXISTS idx_chats_status ON avito_chats(status)",
        "CREATE INDEX IF NOT EXISTS idx_chats_priority ON avito_chats(priority)",
        "CREATE INDEX IF NOT EXISTS idx_chats_updated_at ON avito_chats(updated_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_chats_client_phone ON avito_chats(client_phone)",
        "CREATE INDEX IF NOT EXISTS idx_chats_shop_status ON avito_chats(shop_id, status)",
        
        # Индексы для таблицы сообщений
        "CREATE INDEX IF NOT EXISTS idx_messages_chat_id ON avito_messages(chat_id)",
        "CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON avito_messages(timestamp DESC)",
        "CREATE INDEX IF NOT EXISTS idx_messages_manager_id ON avito_messages(manager_id)",
        "CREATE INDEX IF NOT EXISTS idx_messages_type ON avito_messages(message_type)",
        "CREATE INDEX IF NOT EXISTS idx_messages_chat_id_timestamp ON avito_messages(chat_id, timestamp DESC)",
        
        # Индексы для таблицы назначений менеджеров
        "CREATE INDEX IF NOT EXISTS idx_manager_assignments_manager_id ON manager_assignments(manager_id)",
        "CREATE INDEX IF NOT EXISTS idx_manager_assignments_shop_id ON manager_assignments(shop_id)",
        
        # Индексы для графика работы
        "CREATE INDEX IF NOT EXISTS idx_work_schedules_user_id ON work_schedules(user_id)",
        "CREATE INDEX IF NOT EXISTS idx_work_schedules_day ON work_schedules(day_of_week)",
        "CREATE INDEX IF NOT EXISTS idx_day_managers_day ON day_manager_assignments(day_of_week)",
        "CREATE INDEX IF NOT EXISTS idx_day_managers_manager ON day_manager_assignments(manager_id)",
        
        # Индексы для аналитики
        "CREATE INDEX IF NOT EXISTS idx_analytics_user_id ON analytics_logs(user_id)",
        "CREATE INDEX IF NOT EXISTS idx_analytics_created_at ON analytics_logs(created_at DESC)",
        
        # Индексы для активности
        "CREATE INDEX IF NOT EXISTS idx_activity_user_id ON activity_logs(user_id)",
        "CREATE INDEX IF NOT EXISTS idx_activity_created_at ON activity_logs(created_at DESC)",

        # Индексы для магазинов (OAuth)
        "CREATE INDEX IF NOT EXISTS idx_shops_user_id ON avito_shops(user_id)",
        "CREATE INDEX IF NOT EXISTS idx_shops_status ON avito_shops(is_active, token_status)"
    ]
    
    for index_sql in indexes:
        try:
            cursor.execute(index_sql)
        except Exception as e:
            print(f"[WARNING] Не удалось создать индекс: {e}")
    
    conn.commit()
    conn.close()
    print("[OK] CRM база данных инициализирована с индексами")


# Глобальное соединение с базой данных (всегда открыто)
_global_db_connection = None

def get_db_connection():
    """
    Получение глобального соединения с базой данных
    
    Использует одно глобальное соединение, которое всегда открыто.
    Это предотвращает ошибки "database is locked" при быстрых запросах.
    
    Настройки:
        - row_factory = sqlite3.Row: Позволяет обращаться к колонкам по имени
        - WAL режим: Включается для лучшей параллельной работы
        - timeout: 30 секунд для операций с БД
    
    Важно:
        - Соединение создается один раз и переиспользуется
        - НЕ закрывайте соединение вручную (conn.close())
        - Соединение автоматически переподключается при ошибках
    
    Returns:
        sqlite3.Connection: Глобальное соединение с базой данных
    """
    global _global_db_connection
    
    # Если соединение существует, проверяем его валидность
    if _global_db_connection is not None:
        # Проверяем, что соединение не закрыто (простая проверка без запроса к БД)
        # Это избегает проблем с disk I/O при проверке
        try:
            # Простая проверка - пытаемся получить total_changes
            # Это не требует чтения с диска, только проверяет состояние соединения
            _ = _global_db_connection.total_changes
            return _global_db_connection
        except (sqlite3.ProgrammingError, sqlite3.OperationalError, AttributeError) as e:
            # Соединение закрыто или повреждено, создаем новое
            error_msg = str(e).lower()
            # Если это disk I/O error, закрываем соединение и создаем новое
            if "disk i/o error" in error_msg or "i/o error" in error_msg:
                try:
                    _global_db_connection.close()
                except:
                    pass
                # Сбрасываем глобальное соединение
                _global_db_connection = None
                # Небольшая задержка перед повторной попыткой
                import time
                time.sleep(0.1)
            else:
                # Для других ошибок тоже сбрасываем соединение
            _global_db_connection = None
    
    # Убеждаемся, что директория существует
    db_dir = os.path.dirname(_DB_PATH)
    if not os.path.exists(db_dir):
        try:
            os.makedirs(db_dir, exist_ok=True)
            # Устанавливаем права доступа (если возможно)
            try:
                os.chmod(db_dir, 0o755)
            except:
                pass  # Игнорируем ошибки прав доступа
        except Exception as e:
            raise RuntimeError(
                f"Cannot create database directory {db_dir}: {e}\n"
                f"Current working directory: {os.getcwd()}\n"
                f"Database path: {_DB_PATH}\n"
                f"Please check permissions and ensure the directory can be created."
            ) from e
    
    # Проверяем права на запись в директорию
    if not os.access(db_dir, os.W_OK):
        raise RuntimeError(
            f"No write permission for database directory: {db_dir}\n"
            f"Current working directory: {os.getcwd()}\n"
            f"Database path: {_DB_PATH}\n"
            f"Please check file permissions (chmod 755 {db_dir})"
        )
    
    # Подключаемся к базе данных с обработкой ошибок
    try:
        conn = sqlite3.connect(_DB_PATH, timeout=30.0, check_same_thread=False)
    except sqlite3.OperationalError as e:
        error_msg = str(e).lower()
        if "unable to open database file" in error_msg:
            # Дополнительная диагностика для сервера
            db_exists = os.path.exists(_DB_PATH)
            db_readable = os.access(_DB_PATH, os.R_OK) if db_exists else False
            db_writable = os.access(_DB_PATH, os.W_OK) if db_exists else False
            dir_writable = os.access(db_dir, os.W_OK)
            
            error_details = (
                f"Cannot open database file: {_DB_PATH}\n"
                f"Directory: {db_dir}\n"
                f"Directory exists: {os.path.exists(db_dir)}\n"
                f"Directory writable: {dir_writable}\n"
                f"File exists: {db_exists}\n"
                f"File readable: {db_readable}\n"
                f"File writable: {db_writable}\n"
                f"Current working directory: {os.getcwd()}\n"
                f"__file__ location: {__file__}\n"
                f"Original error: {str(e)}\n\n"
                f"SOLUTIONS:\n"
                f"1. Check directory permissions: chmod 755 {db_dir}\n"
                f"2. Check file permissions: chmod 644 {_DB_PATH} (if exists)\n"
                f"3. Ensure directory exists: mkdir -p {db_dir}\n"
                f"4. Check disk space: df -h {db_dir}"
            )
            raise RuntimeError(error_details) from e
        elif "disk i/o error" in error_msg or "i/o error" in error_msg:
            # Disk I/O error - может быть временной проблемой
            # Пробуем переподключиться после небольшой задержки
            import time
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Disk I/O error detected, attempting to reconnect: {e}")
            time.sleep(0.1)  # Небольшая задержка перед повторной попыткой
            try:
                conn = sqlite3.connect(_DB_PATH, timeout=30.0, check_same_thread=False)
            except sqlite3.OperationalError as retry_error:
                error_details = (
                    f"Disk I/O error when connecting to database: {_DB_PATH}\n"
                    f"Original error: {str(e)}\n"
                    f"Retry error: {str(retry_error)}\n\n"
                    f"POSSIBLE CAUSES:\n"
                    f"1. Database file is locked by another process\n"
                    f"2. Disk is full or has I/O issues\n"
                    f"3. Database file is corrupted\n"
                    f"4. File system permissions issue\n\n"
                    f"SOLUTIONS:\n"
                    f"1. Check if another process is using the database\n"
                    f"2. Check disk space: df -h {db_dir}\n"
                    f"3. Check database file integrity\n"
                    f"4. Restart the application\n"
                    f"5. Check file permissions: ls -la {_DB_PATH}"
                )
                raise RuntimeError(error_details) from retry_error
        else:
            raise
    
    # Устанавливаем row_factory для доступа к колонкам по имени
    conn.row_factory = sqlite3.Row
    
    # Включаем WAL режим для лучшей параллельной работы
    try:
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA synchronous=NORMAL')
        conn.execute('PRAGMA busy_timeout=30000')  # 30 секунд timeout
    except:
        pass  # Игнорируем ошибки при установке PRAGMA
    
    # Сохраняем глобальное соединение
    _global_db_connection = conn
    
    return conn


def execute_with_retry(query_func, max_retries=3, retry_delay=0.1):
    """
    Выполнить функцию с запросом к БД с повторными попытками при disk I/O ошибках
    
    Args:
        query_func: Функция, которая выполняет запрос к БД
        max_retries: Максимальное количество попыток
        retry_delay: Задержка между попытками (в секундах)
    
    Returns:
        Результат выполнения query_func
    
    Raises:
        sqlite3.OperationalError: Если все попытки исчерпаны
    """
    import time
    import logging
    logger = logging.getLogger(__name__)
    
    for attempt in range(max_retries):
        try:
            return query_func()
        except sqlite3.OperationalError as e:
            error_msg = str(e).lower()
            if ("disk i/o error" in error_msg or "i/o error" in error_msg) and attempt < max_retries - 1:
                # Переподключаемся к БД
                global _global_db_connection
                if _global_db_connection is not None:
                    try:
                        _global_db_connection.close()
                    except:
                        pass
                    _global_db_connection = None
                
                logger.warning(f"Disk I/O error on attempt {attempt + 1}/{max_retries}, retrying...")
                time.sleep(retry_delay * (attempt + 1))  # Увеличиваем задержку с каждой попыткой
                continue
            else:
                # Другая ошибка или все попытки исчерпаны
                raise
    
    # Не должно сюда попасть, но на всякий случай
    raise sqlite3.OperationalError("Failed to execute query after all retries")


# Функция для безопасного подключения к базе данных с восстановлением
def safe_init_database():
    """
    Безопасная инициализация базы данных с восстановлением при ошибках
    """
    import shutil
    from datetime import datetime

    db_file = _DB_PATH
    db_dir = os.path.dirname(_DB_PATH)
    backup_dir = os.path.dirname(_DB_PATH)
    backup_file = os.path.join(backup_dir, f"osagaming_crm_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db")

    try:
        # Убеждаемся, что директория существует
        if not os.path.exists(db_dir):
            try:
                os.makedirs(db_dir, exist_ok=True)
                # Устанавливаем права доступа (если возможно)
                try:
                    os.chmod(db_dir, 0o755)
                except:
                    pass  # Игнорируем ошибки прав доступа
                print(f"[INFO] Создана директория для базы данных: {db_dir}")
            except Exception as e:
                error_msg = (
                    f"[ERROR] Не удалось создать директорию {db_dir}: {e}\n"
                    f"Current working directory: {os.getcwd()}\n"
                    f"Database path: {_DB_PATH}\n"
                    f"Please run: mkdir -p {db_dir} && chmod 755 {db_dir}"
                )
                print(error_msg)
                raise
        
        # Проверяем права на запись в директорию
        if not os.access(db_dir, os.W_OK):
            error_msg = (
                f"[ERROR] Нет прав на запись в директорию: {db_dir}\n"
                f"Current working directory: {os.getcwd()}\n"
                f"Please run: chmod 755 {db_dir}"
            )
            print(error_msg)
            raise PermissionError(f"No write permission for directory: {db_dir}")
        
        # Проверяем существование файла
        if os.path.exists(db_file):
            # Проверяем, можем ли мы открыть базу данных
            try:
                test_conn = sqlite3.connect(db_file, timeout=5.0)
                test_conn.execute('SELECT 1').fetchone()
                test_conn.close()
                print(f"[OK] База данных {db_file} доступна")
                # Всегда вызываем init_database() для применения миграций
                print(f"[INFO] Применение миграций...")
                init_database()
                return
            except sqlite3.OperationalError as e:
                error_msg = str(e).lower()
                if "unable to open database file" in error_msg:
                    print(f"[WARNING] Не удалось открыть базу данных: {e}")
                    print(f"[INFO] Проверка прав доступа...")
                    print(f"   Директория существует: {os.path.exists(db_dir)}")
                    print(f"   Директория доступна для записи: {os.access(db_dir, os.W_OK)}")
                    print(f"   Файл существует: {os.path.exists(db_file)}")
                    if os.path.exists(db_file):
                        print(f"   Файл доступен для чтения: {os.access(db_file, os.R_OK)}")
                        print(f"   Файл доступен для записи: {os.access(db_file, os.W_OK)}")
                    # Пробуем создать резервную копию и пересоздать
                    print(f"[INFO] Попытка восстановления...")
                elif "database is locked" in error_msg:
                    print(f"[WARNING] База данных заблокирована другим процессом")
                    return  # Не пересоздаем, просто возвращаемся
                else:
                    print(f"[WARNING] База данных повреждена: {e}")
                    print(f"[INFO] Создание резервной копии: {backup_file}")
                    try:
                        shutil.copy2(db_file, backup_file)
                    except:
                        print("[WARNING] Не удалось создать резервную копию")
            except sqlite3.DatabaseError as e:
                print(f"[WARNING] База данных повреждена: {e}")
                print(f"[INFO] Создание резервной копии: {backup_file}")
                try:
                    shutil.copy2(db_file, backup_file)
                except:
                    print("[WARNING] Не удалось создать резервную копию")

        # Создаем новую базу данных
        print(f"[INFO] Создание новой базы данных: {db_file}")
        init_database()

    except Exception as e:
        print(f"[ERROR] Критическая ошибка инициализации БД: {e}")
        import traceback
        traceback.print_exc()
        raise


# Инициализируем базу данных безопасно
# НЕ вызываем при импорте, чтобы избежать проблем на сервере
# Будет вызвано явно в app.py
# safe_init_database()