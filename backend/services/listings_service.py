"""
Listings Service - работа с объявлениями Avito
"""
import logging
from typing import List, Dict, Optional, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)


class ListingsService:
    """Сервис для работы с объявлениями"""
    
    def __init__(self, db_connection):
        self.conn = db_connection
    
    def search_public_listings(self, query: str = None, category_id: int = None,
                               location_id: int = None, price_min: float = None,
                               price_max: float = None, limit: int = 50) -> Dict:
        """
        Поиск объявлений через публичный парсер
        
        Args:
            query: Поисковый запрос
            category_id: ID категории
            location_id: ID локации
            price_min: Минимальная цена
            price_max: Максимальная цена
            limit: Максимальное количество результатов
        
        Returns:
            Dict: Результаты поиска
        """
        logger.info(f"[LISTINGS SERVICE] Начало поиска объявлений через публичный парсер")
        logger.debug(f"[LISTINGS SERVICE] Параметры: query='{query}', category_id={category_id}, "
                    f"location_id={location_id}, price_min={price_min}, price_max={price_max}, limit={limit}")
        
        from avito_public_parser import AvitoPublicParser
        
        try:
            logger.debug(f"[LISTINGS SERVICE] Создание экземпляра AvitoPublicParser...")
            parser = AvitoPublicParser()
            logger.debug(f"[LISTINGS SERVICE] AvitoPublicParser создан успешно")
            
            import time
            start_time = time.time()
            
            logger.info(f"[LISTINGS SERVICE] Вызов parser.search_listings()...")
            results = parser.search_listings(
                query=query,
                category_id=category_id,
                location_id=location_id,
                price_min=price_min,
                price_max=price_max,
                limit=limit
            )
            
            elapsed_time = time.time() - start_time
            listings = results.get('listings', [])
            listings_count = len(listings)
            total = results.get('total', 0)
            
            logger.info(f"[LISTINGS SERVICE] Поиск завершен за {elapsed_time:.2f} сек. "
                       f"Найдено объявлений: {listings_count}, total: {total}")
            
            if listings_count > 0:
                # Логируем примеры найденных объявлений
                sample_listings = listings[:3]  # Первые 3 для примера
                logger.debug(f"[LISTINGS SERVICE] Примеры найденных объявлений:")
                for idx, listing in enumerate(sample_listings, 1):
                    listing_id = listing.get('listing_id', 'N/A')
                    title = listing.get('title', 'N/A')[:60]
                    price = listing.get('price', 'N/A')
                    url = listing.get('url', 'N/A')[:80]
                    logger.debug(f"[LISTINGS SERVICE]   {idx}. ID={listing_id}, title='{title}', "
                                f"price={price}, url={url}")
            
            if 'error' in results:
                logger.warning(f"[LISTINGS SERVICE] В результатах есть ошибка: {results.get('error')}")
            
            logger.info(f"[LISTINGS SERVICE] Поиск завершен успешно")
            return results
            
        except Exception as e:
            logger.error(f"[LISTINGS SERVICE] КРИТИЧЕСКАЯ ОШИБКА при поиске объявлений: {str(e)}", exc_info=True)
            return {'listings': [], 'total': 0, 'error': str(e)}
    
    def save_listing(self, listing_data: Dict, param_id: Optional[int] = None) -> int:
        """
        Сохранить объявление в БД
        
        Args:
            listing_data: Данные объявления
            param_id: ID параметров поиска (опционально)
        
        Returns:
            int: ID сохранённого объявления или 0
        """
        listing_id = listing_data.get('listing_id', 'unknown')
        title = listing_data.get('title', '')[:50]
        
        logger.debug(f"[LISTINGS SERVICE] Сохранение объявления: listing_id={listing_id}, "
                    f"title='{title}', param_id={param_id}")
        
        try:
            # Проверяем, нет ли уже такого объявления
            logger.debug(f"[LISTINGS SERVICE] Проверка существования объявления listing_id={listing_id}...")
            existing = self.conn.execute('''
                SELECT id FROM avito_listings 
                WHERE listing_id = ?
            ''', (listing_id,)).fetchone()
            
            if existing:
                existing_id = existing['id']
                logger.debug(f"[LISTINGS SERVICE] Объявление уже существует в БД с ID: {existing_id}")
                return existing_id
            
            logger.debug(f"[LISTINGS SERVICE] Объявление не найдено, создание новой записи...")
            
            # Создаём новое
            cursor = self.conn.execute('''
                INSERT INTO avito_listings (
                    listing_id, title, price, url, image_url, location,
                    description, category, status, param_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'new', ?)
            ''', (
                listing_id,
                listing_data.get('title', ''),
                listing_data.get('price', 0),
                listing_data.get('url', ''),
                listing_data.get('image_url', ''),
                listing_data.get('location', ''),
                listing_data.get('description', ''),
                listing_data.get('category', ''),
                param_id
            ))
            
            new_id = cursor.lastrowid
            logger.debug(f"[LISTINGS SERVICE] Объявление создано в БД с ID: {new_id}")
            
            self.conn.commit()
            logger.debug(f"[LISTINGS SERVICE] Изменения зафиксированы в БД")
            
            return new_id
            
        except Exception as e:
            logger.error(f"[LISTINGS SERVICE] ОШИБКА сохранения объявления listing_id={listing_id}: {str(e)}", exc_info=True)
            return 0
    
    def save_search_params(self, params: Dict, user_id: int) -> int:
        """
        Сохранить параметры поиска
        
        Args:
            params: Параметры поиска
            user_id: ID пользователя
        
        Returns:
            int: ID сохранённых параметров
        """
        logger.debug(f"[LISTINGS SERVICE] Сохранение параметров поиска для user_id={user_id}")
        logger.debug(f"[LISTINGS SERVICE] Параметры: {params}")
        
        try:
            name = params.get('name', 'Поиск ' + datetime.now().strftime('%Y-%m-%d %H:%M'))
            logger.debug(f"[LISTINGS SERVICE] Имя параметров поиска: '{name}'")
            
            cursor = self.conn.execute('''
                INSERT INTO search_params (
                    user_id, name, query, category_id, location_id,
                    price_min, price_max, limit_results, is_active
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
            ''', (
                user_id,
                name,
                params.get('query'),
                params.get('category_id'),
                params.get('location_id'),
                params.get('price_min'),
                params.get('price_max'),
                params.get('limit', 50)
            ))
            
            param_id = cursor.lastrowid
            logger.debug(f"[LISTINGS SERVICE] Параметры поиска сохранены с ID: {param_id}")
            
            self.conn.commit()
            logger.debug(f"[LISTINGS SERVICE] Изменения зафиксированы в БД")
            
            return param_id
            
        except Exception as e:
            logger.error(f"[LISTINGS SERVICE] ОШИБКА сохранения параметров поиска: {str(e)}", exc_info=True)
            return 0
    
    def get_saved_listings(self, status: str = None, assigned_manager_id: int = None,
                          limit: int = 100, offset: int = 0) -> Tuple[List[Dict], int]:
        """
        Получить сохранённые объявления
        
        Args:
            status: Статус фильтрации
            assigned_manager_id: ID менеджера
            limit: Количество результатов
            offset: Смещение
        
        Returns:
            Tuple[List[Dict], int]: (список объявлений, общее количество)
        """
        logger.debug(f"[LISTINGS SERVICE] Получение сохраненных объявлений: status='{status}', "
                    f"assigned_manager_id={assigned_manager_id}, limit={limit}, offset={offset}")
        
        # Проверяем наличие колонок first_name и last_name
        has_name_columns = False
        try:
            cursor = self.conn.execute("PRAGMA table_info(users)")
            columns_info = cursor.fetchall()
            user_columns = [row[1] if len(row) > 1 else str(row[0]) for row in columns_info]
            has_name_columns = 'first_name' in user_columns and 'last_name' in user_columns
        except Exception:
            has_name_columns = False
        
        if has_name_columns:
            query = '''
                SELECT l.*, 
                       COALESCE(
                           NULLIF(TRIM(u.first_name || ' ' || COALESCE(u.last_name, '')), ''),
                           u.username,
                           ''
                       ) as assigned_manager_name
                FROM avito_listings l
                LEFT JOIN users u ON l.assigned_manager_id = u.id
                WHERE 1=1
            '''
        else:
            query = '''
                SELECT l.*, 
                       COALESCE(u.username, '') as assigned_manager_name
                FROM avito_listings l
                LEFT JOIN users u ON l.assigned_manager_id = u.id
                WHERE 1=1
            '''
        params = []
        
        if status:
            query += ' AND l.status = ?'
            params.append(status)
            logger.debug(f"[LISTINGS SERVICE] Добавлен фильтр по статусу: {status}")
        
        if assigned_manager_id:
            query += ' AND l.assigned_manager_id = ?'
            params.append(assigned_manager_id)
            logger.debug(f"[LISTINGS SERVICE] Добавлен фильтр по менеджеру: {assigned_manager_id}")
        
        query += ' ORDER BY l.created_at DESC LIMIT ? OFFSET ?'
        params.extend([limit, offset])
        
        logger.debug(f"[LISTINGS SERVICE] Выполнение SQL запроса: {query[:200]}...")
        logger.debug(f"[LISTINGS SERVICE] Параметры запроса: {params}")
        
        listings = self.conn.execute(query, tuple(params)).fetchall()
        listings_count = len(listings)
        logger.debug(f"[LISTINGS SERVICE] Получено строк из БД: {listings_count}")
        
        # Подсчет общего количества
        count_query = 'SELECT COUNT(*) as count FROM avito_listings WHERE 1=1' + 
            (' AND status = ?' if status else '') +
            (' AND assigned_manager_id = ?' if assigned_manager_id else '')
        count_params = tuple([p for p in params if p not in [limit, offset]])
        
        logger.debug(f"[LISTINGS SERVICE] Выполнение запроса подсчета: {count_query}")
        logger.debug(f"[LISTINGS SERVICE] Параметры подсчета: {count_params}")
        
        total = self.conn.execute(count_query, count_params).fetchone()['count']
        
        logger.info(f"[LISTINGS SERVICE] Запрос выполнен: получено {listings_count} объявлений, "
                   f"всего в БД: {total}")
        
        return [dict(listing) for listing in listings], total
    
    def update_listing_status(self, listing_id: int, status: str, notes: str = None) -> bool:
        """
        Обновить статус объявления
        
        Args:
            listing_id: ID объявления
            status: Новый статус
            notes: Заметки (опционально)
        
        Returns:
            bool: True если успешно
        """
        try:
            if notes:
                self.conn.execute('''
                    UPDATE avito_listings
                    SET status = ?, notes = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', (status, notes, listing_id))
            else:
                self.conn.execute('''
                    UPDATE avito_listings
                    SET status = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', (status, listing_id))
            
            self.conn.commit()
            return True
            
        except Exception as e:
            logger.error(f"Ошибка обновления статуса объявления {listing_id}: {e}")
            return False

