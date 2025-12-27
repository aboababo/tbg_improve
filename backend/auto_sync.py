#!/usr/bin/env python3
"""
Автоматическая синхронизация чатов и сообщений с Avito API
Запуск: python3 backend/auto_sync.py
Использует SyncService для правильного сохранения listing_data из context.value
"""
import sqlite3
import sys
import os
import time
import logging
from datetime import datetime

# Добавляем путь к backend
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from avito_api import AvitoAPI
from services.sync_service import SyncService

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('auto_sync.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('auto_sync')

def get_db_connection():
    # Используем функцию из database.py для консистентности
    try:
        from database import get_db_connection as db_get_conn
        return db_get_conn()
    except ImportError:
        # Fallback если database.py недоступен
        db_path = os.path.join(os.path.dirname(__file__), 'osagaming_crm.db')
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        return conn

def to_str(value, default=''):
    if value is None:
        return default
    return str(value)

def extract_text(msg_data):
    """Извлечь текст из разных структур"""
    if not msg_data:
        return ''
    if isinstance(msg_data, str):
        return msg_data
    if not isinstance(msg_data, dict):
        return str(msg_data)
    
    for key in ['text', 'content', 'message']:
        if msg_data.get(key):
            val = msg_data[key]
            if isinstance(val, dict):
                return extract_text(val)
            return to_str(val)
    return ''

def sync_shop_chats(shop, conn):
    """Синхронизация чатов одного магазина через SyncService"""
    logger.info(f"Синхронизация магазина: {shop['name']} (ID: {shop['id']})")
    
    try:
        api = AvitoAPI(
            client_id=shop['client_id'],
            client_secret=shop['client_secret']
        )
        
        # Используем SyncService для правильного сохранения listing_data из context.value
        sync_service = SyncService(conn, api)
        
        # Синхронизируем чаты для магазина
        result = sync_service.sync_chats_for_shop(
            shop_id=shop['id'],
            user_id=str(shop['user_id'])
        )
        
        if result.get('success'):
            logger.info(f"  ✓ Создано: {result.get('chats_created', 0)}, Обновлено: {result.get('chats_updated', 0)}, Сообщений: {result.get('messages_created', 0)}")
            return True
        else:
            error_msg = result.get('error', 'Unknown error')
            if '403' in error_msg or 'permission denied' in error_msg.lower():
                logger.warning(f"  ⚠ Нет доступа к Messenger API")
            else:
                logger.error(f"  ✗ Ошибка: {error_msg}")
            return False
        
    except Exception as e:
        error_str = str(e)
        if '403' in error_str or 'permission denied' in error_str.lower():
            logger.warning(f"  ⚠ Нет доступа к Messenger API")
        else:
            logger.error(f"  ✗ Ошибка: {e}", exc_info=True)
        return False

def run_sync():
    """Один цикл синхронизации"""
    logger.info("="*60)
    logger.info(f"НАЧАЛО СИНХРОНИЗАЦИИ: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*60)
    
    conn = get_db_connection()
    
    # Добавляем колонку customer_id если её нет
    try:
        conn.execute('ALTER TABLE avito_chats ADD COLUMN customer_id TEXT')
        conn.commit()
    except:
        pass  # Колонка уже существует
    
    # Получаем все активные магазины
    shops = conn.execute('''
        SELECT id, name, client_id, client_secret, user_id
        FROM avito_shops
        WHERE is_active = 1
        AND client_id IS NOT NULL
        AND user_id IS NOT NULL
    ''').fetchall()
    
    logger.info(f"Магазинов для синхронизации: {len(shops)}\n")
    
    success = 0
    failed = 0
    
    for shop in shops:
        if sync_shop_chats(shop, conn):
            success += 1
        else:
            failed += 1
    
    conn.close()
    
    logger.info("\n" + "="*60)
    logger.info(f"СИНХРОНИЗАЦИЯ ЗАВЕРШЕНА")
    logger.info(f"Успешно: {success}, Ошибок: {failed}")
    logger.info("="*60 + "\n")

def main():
    """Основной цикл"""
    # Интервал синхронизации (в секундах)
    SYNC_INTERVAL = int(os.environ.get('SYNC_INTERVAL', 60))  # По умолчанию 60 секунд
    
    logger.info(f"Запуск автосинхронизации (интервал: {SYNC_INTERVAL} сек)")
    
    while True:
        try:
            run_sync()
        except Exception as e:
            logger.error(f"Критическая ошибка: {e}")
            import traceback
            traceback.print_exc()
        
        logger.info(f"Следующая синхронизация через {SYNC_INTERVAL} секунд...")
        time.sleep(SYNC_INTERVAL)

if __name__ == '__main__':
    # Проверяем аргументы
    if len(sys.argv) > 1 and sys.argv[1] == '--once':
        # Однократный запуск
        run_sync()
    else:
        # Непрерывный режим
        main()

