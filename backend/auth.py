"""
OSAGAMING CRM - Модуль аутентификации и авторизации
==================================================

Этот файл содержит функции для:
- Хеширования и проверки паролей
- Аутентификации пользователей
- Получения информации о пользователях
- Работы с настройками пользователей

Безопасность:
- Пароли хранятся в виде SHA256 хешей (не в открытом виде)
- Проверка активности аккаунта перед аутентификацией
- Защита от SQL инъекций через параметризованные запросы

Автор: OSAGAMING Development Team
Версия: 2.0
"""

import hashlib
import secrets
import string
from database import get_db_connection


def hash_password(password):
    """
    Хеширование пароля с использованием SHA256
    
    Преобразует пароль в хеш для безопасного хранения в базе данных.
    Пароли никогда не хранятся в открытом виде.
    
    Алгоритм: SHA256 (одностороннее хеширование)
    
    Args:
        password (str): Пароль в открытом виде
    
    Returns:
        str: Хеш пароля в шестнадцатеричном формате (64 символа)
    
    Пример:
        hash_password("mypassword123")
        -> "ef92b778bafe771e89245b89ecbc08a44a4e166c06659911881f383d4473e94"
    
    Примечание:
        SHA256 - это односторонняя функция, нельзя восстановить пароль из хеша.
        Для проверки используется сравнение хешей.
    """
    # Кодируем пароль в байты, хешируем SHA256, преобразуем в hex строку
    return hashlib.sha256(password.encode()).hexdigest()


def generate_temp_password(length=12):
    """
    Генерация одноразового пароля для новых менеджеров

    Создает безопасный одноразовый пароль из букв, цифр и символов.

    Args:
        length (int): Длина пароля (по умолчанию 12)

    Returns:
        str: Одноразовый пароль
    """
    alphabet = string.ascii_letters + string.digits + "!@#$%^&*"
    return ''.join(secrets.choice(alphabet) for i in range(length))


def update_user_password(user_id, new_password):
    """
    Обновление пароля пользователя

    Args:
        user_id (int): ID пользователя
        new_password (str): Новый пароль

    Returns:
        bool: True если успешно
    """
    conn = get_db_connection()
    try:
        conn.execute('''
            UPDATE users
            SET password = ?, temp_password = NULL, password_changed = 1
            WHERE id = ?
        ''', (hash_password(new_password), user_id))
        conn.commit()
        return True
    except Exception as e:
        print(f"Ошибка обновления пароля: {e}")
        return False
    # Соединение глобальное, не закрываем


def verify_password(password, hashed):
    """
    Проверка пароля путем сравнения хешей
    
    Сравнивает хеш введенного пароля с хешем, хранящимся в базе данных.
    
    Args:
        password (str): Пароль для проверки (в открытом виде)
        hashed (str): Хранящийся хеш пароля из базы данных
    
    Returns:
        bool: True если пароли совпадают, False в противном случае
    
    Пример:
        verify_password("mypassword", "ef92b778...") -> True
        verify_password("wrongpass", "ef92b778...") -> False
    """
    # Хешируем введенный пароль и сравниваем с хранящимся хешем
    return hash_password(password) == hashed


def authenticate_user(email, password):
    """
    Аутентификация пользователя по email и паролю
    
    Поддерживает одноразовые пароли для новых менеджеров.
    При первом входе менеджер должен изменить пароль.
    
    Процесс:
        1. Поиск пользователя по email и проверяем активность
        2. Проверка правильности пароля (основной или одноразовый)
        3. Возврат данных пользователя с флагом необходимости смены пароля
    
    Args:
        email (str): Email адрес пользователя
        password (str): Пароль пользователя
    
    Returns:
        dict: Словарь с данными пользователя при успешной аутентификации
        None: Если пользователь не найден, неактивен или пароль неверен
    
    Словарь содержит:
        - Все поля пользователя из БД
        - 'temp_password_used': True если использован одноразовый пароль
    """
    conn = get_db_connection()
    
    # Ищем пользователя по email и проверяем активность
    user = conn.execute(
        'SELECT id, username, email, password, role, is_active, salary, kpi_score, temp_password, password_changed, created_at, created_by, settings, first_name, last_name FROM users WHERE email = ? AND is_active = 1',
        (email,)
    ).fetchone()

    if not user:
        # Соединение глобальное, не закрываем
        return None

    user_dict = dict(user)
    temp_password_used = False

    # Проверяем основной пароль
    if verify_password(password, user_dict['password']):
        pass  # Основной пароль верен
    # Проверяем одноразовый пароль (если он есть и менеджер еще не изменил пароль)
    elif (user_dict.get('temp_password') and
          user_dict['temp_password'] == password and
          not user_dict.get('password_changed', False)):
        temp_password_used = True
        user_dict['temp_password_used'] = True
    else:
        # Соединение глобальное, не закрываем
        return None

    # Соединение глобальное, не закрываем
    return user_dict


def get_user_by_id(user_id):
    """
    Получение информации о пользователе по его ID
    
    Используется для получения данных пользователя из сессии или
    для отображения информации о других пользователях.
    
    Args:
        user_id (int): ID пользователя в базе данных
    
    Returns:
        dict: Словарь с данными пользователя:
            {
                'id': int,
                'username': str,
                'email': str,
                'role': str,
                'is_active': bool,
                'kpi_score': float,
                'password_changed': bool
            }
        None: Если пользователь не найден
    
    Пример:
        user = get_user_by_id(1)
        if user:
            print(f"Пользователь: {user['username']}, Роль: {user['role']}")
    
    Примечание:
        Не возвращает пароль и другие чувствительные данные для безопасности
    """
    conn = get_db_connection()
    
    # Выбираем только необходимые поля (без пароля)
    user = conn.execute(
        'SELECT id, username, email, role, is_active, kpi_score, password_changed FROM users WHERE id = ?',
        (user_id,)
    ).fetchone()
    # Соединение глобальное, не закрываем

    # Преобразуем в словарь или возвращаем None
    return dict(user) if user else None


def get_user_settings(user_id):
    """
    Получение настроек пользователя
    
    Возвращает персональные настройки пользователя (тема, уведомления и т.д.)
    
    Args:
        user_id (int): ID пользователя
    
    Returns:
        dict: Словарь с настройками пользователя:
            {
                'id': int,
                'user_id': int,
                'theme': str,  # 'dark' или 'light'
                'colors': str,  # JSON строка с цветами
                'sound_alerts': bool,
                'push_notifications': bool
            }
        None: Если настройки не найдены
    
    Пример:
        settings = get_user_settings(1)
        if settings:
            print(f"Тема: {settings['theme']}")
    """
    conn = get_db_connection()
    
    # Получаем все настройки пользователя
    settings = conn.execute(
        'SELECT id, user_id, theme, colors, sound_alerts, push_notifications, tab_visibility FROM user_settings WHERE user_id = ?',
        (user_id,)
    ).fetchone()
    # Соединение глобальное, не закрываем

    # Преобразуем в словарь или возвращаем None
    return dict(settings) if settings else None