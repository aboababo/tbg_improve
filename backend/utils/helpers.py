"""
Вспомогательные функции
"""
import json
import logging
from flask import request
from database import get_db_connection

logger = logging.getLogger(__name__)


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
        logger.error(f'Error logging activity: {str(e)}')
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
        dict: Словарь со статистикой системы
    """
    conn = get_db_connection()
    
    # Считаем общее количество чатов
    total_chats = conn.execute('SELECT COUNT(*) as count FROM avito_chats').fetchone()['count']

    # Считаем активные чаты (не завершенные, требующие внимания)
    active_chats = conn.execute('SELECT COUNT(*) as count FROM avito_chats WHERE status = "active"').fetchone()['count']

    # Считаем срочные чаты (требующие немедленного ответа)
    urgent_chats = conn.execute('SELECT COUNT(*) as count FROM avito_chats WHERE priority = "urgent"').fetchone()['count']

    # Считаем пользователей
    total_users = conn.execute('SELECT COUNT(*) as count FROM users WHERE is_active = 1').fetchone()['count']

    # Считаем менеджеров
    managers_count = conn.execute('SELECT COUNT(*) as count FROM users WHERE role = "manager" AND is_active = 1').fetchone()['count']

    # Считаем магазины
    shops_count = conn.execute('SELECT COUNT(*) as count FROM avito_shops WHERE is_active = 1').fetchone()['count']

    # Пытаемся получить статистику из Avito API для всех магазинов
    avito_stats = {
        'total_messages': 0,
        'new_messages': 0,
        'responses': 0
    }
    
    try:
        from avito_api import AvitoAPI
        shops = conn.execute('''
            SELECT id, name, client_id, client_secret, user_id
            FROM avito_shops
            WHERE is_active = 1 AND client_id IS NOT NULL AND client_secret IS NOT NULL AND user_id IS NOT NULL
            LIMIT 5
        ''').fetchall()
        
        for shop in shops:
            try:
                api = AvitoAPI(client_id=shop['client_id'], client_secret=shop['client_secret'])
                stats = api.get_account_stats(user_id=shop['user_id'])
                if stats and isinstance(stats, dict):
                    avito_stats['total_messages'] += stats.get('total_messages', 0)
                    avito_stats['new_messages'] += stats.get('new_messages', 0)
                    avito_stats['responses'] += stats.get('responses', 0)
            except Exception as stats_err:
                logger.debug(f'Не удалось получить статистику аккаунта для магазина {shop["id"]}: {stats_err}')
    except Exception as e:
        logger.warning(f'Ошибка получения статистики Avito: {e}')

    return {
        'total_chats': total_chats,
        'active_chats': active_chats,
        'urgent_chats': urgent_chats,
        'total_users': total_users,
        'managers_count': managers_count,
        'shops_count': shops_count,
        'avito_stats': avito_stats
    }
