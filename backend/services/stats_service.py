"""
Stats Service - статистика и аналитика
"""
import logging
from typing import Dict
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class StatsService:
    """Сервис статистики"""
    
    def __init__(self, db_connection):
        self.conn = db_connection
    
    def get_dashboard_stats(self) -> Dict:
        """Получить статистику для дашборда"""
        
        # Чаты
        chats_total = self.conn.execute('SELECT COUNT(*) as count FROM avito_chats').fetchone()['count']
        chats_active = self.conn.execute("SELECT COUNT(*) as count FROM avito_chats WHERE status = 'active'").fetchone()['count']
        chats_pool = self.conn.execute('SELECT COUNT(*) as count FROM avito_chats WHERE assigned_manager_id IS NULL').fetchone()['count']
        
        # Сообщения
        messages_total = self.conn.execute('SELECT COUNT(*) as count FROM avito_messages').fetchone()['count']
        messages_today = self.conn.execute('''
            SELECT COUNT(*) as count FROM avito_messages 
            WHERE DATE(timestamp) = DATE('now')
        ''').fetchone()['count']
        
        # Объявления
        listings_total = self.conn.execute('SELECT COUNT(*) as count FROM avito_listings').fetchone()['count']
        listings_new = self.conn.execute("SELECT COUNT(*) as count FROM avito_listings WHERE status = 'new'").fetchone()['count']
        listings_contacted = self.conn.execute("SELECT COUNT(*) as count FROM avito_listings WHERE status = 'contacted'").fetchone()['count']
        
        # Доставки
        deliveries_total = self.conn.execute('SELECT COUNT(*) as count FROM deliveries').fetchone()['count']
        deliveries_active = self.conn.execute("SELECT COUNT(*) as count FROM deliveries WHERE delivery_status IN ('pending', 'shipped')").fetchone()['count']
        
        return {
            'chats': {
                'total': chats_total,
                'active': chats_active,
                'pool': chats_pool
            },
            'messages': {
                'total': messages_total,
                'today': messages_today
            },
            'listings': {
                'total': listings_total,
                'new': listings_new,
                'contacted': listings_contacted
            },
            'deliveries': {
                'total': deliveries_total,
                'active': deliveries_active
            }
        }
    
    def get_manager_stats(self, manager_id: int) -> Dict:
        """Статистика менеджера"""
        
        # Чаты менеджера
        chats_count = self.conn.execute('''
            SELECT COUNT(*) as count FROM avito_chats 
            WHERE assigned_manager_id = ?
        ''', (manager_id,)).fetchone()['count']
        
        # Сообщения менеджера
        messages_count = self.conn.execute('''
            SELECT COUNT(*) as count FROM avito_messages 
            WHERE manager_id = ?
        ''', (manager_id,)).fetchone()['count']
        
        # Доставки менеджера
        deliveries_count = self.conn.execute('''
            SELECT COUNT(*) as count FROM deliveries 
            WHERE manager_id = ?
        ''', (manager_id,)).fetchone()['count']
        
        return {
            'chats': chats_count,
            'messages': messages_count,
            'deliveries': deliveries_count
        }

