"""
OsaGaming CRM - Асинхронные задачи
==================================

Асинхронная обработка задач с использованием RQ (Redis Queue):
- Синхронизация чатов
- Загрузка сообщений
- Отправка уведомлений
- Обработка webhook'ов

Автор: OsaGaming Development Team
Версия: 1.0
"""

import logging
import os
from datetime import datetime
from typing import Optional, Dict, List

logger = logging.getLogger(__name__)

# Проверяем доступность RQ
try:
    from rq import Queue, get_current_job
    from rq.job import Job
    import redis
    RQ_AVAILABLE = True
    
    # Инициализация Redis и очереди
    try:
        redis_host = os.environ.get('REDIS_HOST', 'localhost')
        redis_port = int(os.environ.get('REDIS_PORT', 6379))
        redis_db = int(os.environ.get('REDIS_DB', 0))
        redis_password = os.environ.get('REDIS_PASSWORD')
        
        redis_conn = redis.StrictRedis(
            host=redis_host,
            port=redis_port,
            db=redis_db,
            password=redis_password,
            decode_responses=True
        )
        redis_conn.ping()
        
        # Создаем очереди
        default_queue = Queue('default', connection=redis_conn)
        sync_queue = Queue('sync', connection=redis_conn)
        notifications_queue = Queue('notifications', connection=redis_conn)
        
        logger.info("RQ успешно инициализирован")
    except Exception as e:
        logger.warning(f"Не удалось подключиться к Redis для RQ: {e}. Асинхронные задачи будут отключены.")
        RQ_AVAILABLE = False
        default_queue = None
        sync_queue = None
        notifications_queue = None
except ImportError:
    logger.warning("RQ не установлен. Установите: pip install rq")
    RQ_AVAILABLE = False
    default_queue = None
    sync_queue = None
    notifications_queue = None


def sync_all_chats_task():
    """
    Асинхронная задача для синхронизации всех чатов
    """
    try:
        from database import get_db_connection
        from avito_api import AvitoAPI
        from services.sync_service import SyncService
        
        logger.info("Начало синхронизации чатов (асинхронно)")
        
        conn = get_db_connection()
        try:
            # Получаем все активные магазины
            shops = conn.execute('''
                SELECT id, client_id, client_secret, user_id, name
                FROM avito_shops
                WHERE is_active = 1
            ''').fetchall()
            
            if not shops:
                logger.info("Нет активных магазинов для синхронизации")
                return {'status': 'success', 'shops_synced': 0}
            
            synced_count = 0
            errors = []
            
            for shop in shops:
                try:
                    # Создаем API клиент для магазина
                    api = AvitoAPI(shop['client_id'], shop['client_secret'])
                    
                    # Создаем сервис синхронизации
                    sync_service = SyncService(conn, api)
                    
                    # Синхронизируем чаты
                    result = sync_service.sync_chats_for_shop(
                        shop_id=shop['id'],
                        user_id=str(shop['user_id'])
                    )
                    
                    if result.get('success'):
                        synced_count += 1
                        logger.info(f"Магазин {shop['name']} синхронизирован: {result.get('chats_synced', 0)} чатов")
                    else:
                        errors.append(f"{shop['name']}: {result.get('error', 'Unknown error')}")
                        
                except Exception as e:
                    error_msg = f"Ошибка синхронизации магазина {shop.get('name', shop['id'])}: {str(e)}"
                    logger.error(error_msg, exc_info=True)
                    errors.append(error_msg)
            
            result = {
                'status': 'success',
                'shops_synced': synced_count,
                'total_shops': len(shops),
                'errors': errors,
                'timestamp': datetime.now().isoformat()
            }
            
            logger.info(f"Синхронизация завершена: {synced_count}/{len(shops)} магазинов")
            return result
            
        finally:
            conn.close()
            
    except Exception as e:
        logger.error(f"Критическая ошибка в sync_all_chats_task: {e}", exc_info=True)
        return {
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }


def sync_chat_messages_task(chat_id: int, user_id: str, avito_chat_id: str):
    """
    Асинхронная задача для синхронизации сообщений конкретного чата
    
    Args:
        chat_id: ID чата в БД
        user_id: user_id Avito
        avito_chat_id: chat_id в Avito
    """
    try:
        from database import get_db_connection
        from avito_api import AvitoAPI
        from services.messenger_service import MessengerService
        
        logger.info(f"Начало синхронизации сообщений для чата {chat_id}")
        
        conn = get_db_connection()
        try:
            # Получаем информацию о магазине
            shop = conn.execute('''
                SELECT s.client_id, s.client_secret
                FROM avito_chats c
                JOIN avito_shops s ON c.shop_id = s.id
                WHERE c.id = ?
            ''', (chat_id,)).fetchone()
            
            if not shop:
                logger.error(f"Магазин не найден для чата {chat_id}")
                return {'status': 'error', 'error': 'Shop not found'}
            
            # Создаем API клиент
            api = AvitoAPI(shop['client_id'], shop['client_secret'])
            
            # Создаем сервис
            messenger_service = MessengerService(conn, api)
            
            # Синхронизируем сообщения
            new_count = messenger_service.sync_chat_messages(
                chat_id=chat_id,
                user_id=user_id,
                avito_chat_id=avito_chat_id
            )
            
            result = {
                'status': 'success',
                'chat_id': chat_id,
                'new_messages': new_count,
                'timestamp': datetime.now().isoformat()
            }
            
            logger.info(f"Синхронизация сообщений завершена для чата {chat_id}: {new_count} новых сообщений")
            return result
            
        finally:
            conn.close()
            
    except Exception as e:
        logger.error(f"Ошибка в sync_chat_messages_task: {e}", exc_info=True)
        return {
            'status': 'error',
            'chat_id': chat_id,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }


def send_notification_task(user_id: int, message: str, notification_type: str = 'info'):
    """
    Асинхронная задача для отправки уведомления пользователю
    
    Args:
        user_id: ID пользователя
        message: Текст уведомления
        notification_type: Тип уведомления (info, success, warning, error)
    """
    try:
        from database import get_db_connection
        
        logger.info(f"Отправка уведомления пользователю {user_id}: {message}")
        
        conn = get_db_connection()
        try:
            # Сохраняем уведомление в БД (если есть таблица notifications)
            # Пока просто логируем
            logger.info(f"Уведомление отправлено: user_id={user_id}, type={notification_type}, message={message}")
            
            # Здесь можно добавить:
            # - Сохранение в БД
            # - Отправку через WebSocket
            # - Email уведомления
            # - Push уведомления
            
            return {
                'status': 'success',
                'user_id': user_id,
                'message': message,
                'type': notification_type,
                'timestamp': datetime.now().isoformat()
            }
            
        finally:
            conn.close()
            
    except Exception as e:
        logger.error(f"Ошибка в send_notification_task: {e}", exc_info=True)
        return {
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }


def process_webhook_task(webhook_data: Dict):
    """
    Асинхронная задача для обработки webhook от Avito
    
    Args:
        webhook_data: Данные webhook
    """
    try:
        from database import get_db_connection
        from avito_api import AvitoAPI
        from services.messenger_service import MessengerService
        
        logger.info(f"Обработка webhook: {webhook_data.get('type', 'unknown')}")
        
        webhook_type = webhook_data.get('type')
        
        if webhook_type == 'message':
            # Обработка нового сообщения
            chat_id = webhook_data.get('chat_id')
            user_id = webhook_data.get('user_id')
            
            if chat_id and user_id:
                # Ставим задачу на синхронизацию сообщений
                enqueue_sync_chat_messages(chat_id, user_id, chat_id)
        
        elif webhook_type == 'chat':
            # Обработка изменений чата
            logger.info(f"Обработка изменений чата: {webhook_data}")
            # Здесь можно добавить логику обновления чата
        
        return {
            'status': 'success',
            'webhook_type': webhook_type,
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Ошибка в process_webhook_task: {e}", exc_info=True)
        return {
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }


def enqueue_sync_all_chats():
    """
    Поставить задачу синхронизации всех чатов в очередь
    
    Returns:
        Job объект или None если RQ недоступен
    """
    if not RQ_AVAILABLE:
        logger.warning("RQ недоступен, выполняется синхронная синхронизация")
        return sync_all_chats_task()
    
    try:
        job = sync_queue.enqueue(sync_all_chats_task, job_timeout='5m')
        logger.info(f"Задача синхронизации чатов поставлена в очередь: {job.id}")
        return job
    except Exception as e:
        logger.error(f"Ошибка постановки задачи в очередь: {e}")
        # Fallback на синхронное выполнение
        return sync_all_chats_task()


def enqueue_sync_chat_messages(chat_id: int, user_id: str, avito_chat_id: str):
    """
    Поставить задачу синхронизации сообщений в очередь
    
    Args:
        chat_id: ID чата в БД
        user_id: user_id Avito
        avito_chat_id: chat_id в Avito
    
    Returns:
        Job объект или None если RQ недоступен
    """
    if not RQ_AVAILABLE:
        logger.warning("RQ недоступен, выполняется синхронная синхронизация")
        return sync_chat_messages_task(chat_id, user_id, avito_chat_id)
    
    try:
        job = sync_queue.enqueue(
            sync_chat_messages_task,
            chat_id,
            user_id,
            avito_chat_id,
            job_timeout='2m'
        )
        logger.info(f"Задача синхронизации сообщений поставлена в очередь: {job.id}")
        return job
    except Exception as e:
        logger.error(f"Ошибка постановки задачи в очередь: {e}")
        # Fallback на синхронное выполнение
        return sync_chat_messages_task(chat_id, user_id, avito_chat_id)


def enqueue_notification(user_id: int, message: str, notification_type: str = 'info'):
    """
    Поставить задачу отправки уведомления в очередь
    
    Args:
        user_id: ID пользователя
        message: Текст уведомления
        notification_type: Тип уведомления
    """
    if not RQ_AVAILABLE:
        return send_notification_task(user_id, message, notification_type)
    
    try:
        job = notifications_queue.enqueue(
            send_notification_task,
            user_id,
            message,
            notification_type,
            job_timeout='30s'
        )
        return job
    except Exception as e:
        logger.error(f"Ошибка постановки задачи уведомления в очередь: {e}")
        return send_notification_task(user_id, message, notification_type)


def enqueue_webhook(webhook_data: Dict):
    """
    Поставить задачу обработки webhook в очередь
    
    Args:
        webhook_data: Данные webhook
    """
    if not RQ_AVAILABLE:
        return process_webhook_task(webhook_data)
    
    try:
        job = default_queue.enqueue(
            process_webhook_task,
            webhook_data,
            job_timeout='1m'
        )
        return job
    except Exception as e:
        logger.error(f"Ошибка постановки задачи webhook в очередь: {e}")
        return process_webhook_task(webhook_data)

