"""
Сервис для работы с объявлениями в чатах
=========================================

Отвечает за получение информации об объявлении из данных чата (context.value)
"""

import re
import json
import logging
import sqlite3
from typing import Dict, Optional, Tuple, Any
from database import get_db_connection
from avito_api import AvitoAPI

logger = logging.getLogger(__name__)


class ChatListingService:
    """Сервис для работы с объявлениями в чатах"""
    
    @staticmethod
    def extract_item_id_from_url(product_url: str) -> Optional[str]:
        """Извлечь item_id из URL объявления"""
        if not product_url:
            return None
        
        product_url = product_url.strip()
        
        # Формат 1: /items/123456
        match = re.search(r'/items/(\d+)', product_url)
        if match and len(match.group(1)) >= 5:
            return match.group(1)
        
        # Формат 2: ?item_id=123456
        match = re.search(r'[?&]item[_-]?id=(\d+)', product_url, re.IGNORECASE)
        if match and len(match.group(1)) >= 5:
            return match.group(1)
        
        # Формат 3: ..._123456 (ID в конце URL)
        match = re.search(r'[_-](\d{5,})(?:\?|$|/)', product_url)
        if match and 5 <= len(match.group(1)) <= 15:
            return match.group(1)
        
        # Формат 4: ID в конце пути
        match = re.search(r'/(\d{5,})(?:\?|$)', product_url)
        if match and '/items/' not in product_url and 5 <= len(match.group(1)) <= 15:
            return match.group(1)
        
        # Формат 5: Самый длинный числовой паттерн
        matches = re.findall(r'(\d{7,})', product_url)
        if matches:
            item_id = max(matches, key=len)
            if len(item_id) <= 15:
                return item_id
        
        return None
    
    @staticmethod
    def get_chat_listing_info(chat_id: int, try_extract_from_api: bool = True) -> Tuple[Optional[Dict], Optional[str], Optional[str]]:
        """Получить информацию о чате и объявлении из БД"""
        conn = get_db_connection()
        try:
            chat = conn.execute('''
                SELECT ac.*, s.client_id, s.client_secret, s.user_id, s.shop_url
                FROM avito_chats ac
                JOIN avito_shops s ON ac.shop_id = s.id
                WHERE ac.id = ?
            ''', (chat_id,)).fetchone()
            
            if not chat:
                return None, None, None
            
            chat_dict = dict(chat)
            product_url = chat_dict.get('product_url')
            
            if product_url and isinstance(product_url, str):
                product_url = product_url.strip() or None
            
            item_id = None
            if product_url:
                item_id = ChatListingService.extract_item_id_from_url(product_url)
            
            # Проверяем наличие сохраненных данных объявления из синхронизации
            listing_data_str = chat_dict.get('listing_data')
            if listing_data_str:
                try:
                    listing_data = json.loads(listing_data_str)
                    if isinstance(listing_data, dict) and (listing_data.get('title') or listing_data.get('url')):
                        # Сохраняем данные в chat_dict для использования
                        chat_dict['_saved_listing_data'] = listing_data
                        logger.info(f"[CHAT LISTING SERVICE] ✅ Найдены сохраненные данные объявления из синхронизации: title={bool(listing_data.get('title'))}, price_string={bool(listing_data.get('price_string'))}, images={bool(listing_data.get('images'))}, url={bool(listing_data.get('url'))}")
                    else:
                        logger.warning(f"[CHAT LISTING SERVICE] listing_data есть, но неполные: title={bool(listing_data.get('title'))}, url={bool(listing_data.get('url'))}")
                except Exception as e:
                    logger.warning(f"[CHAT LISTING SERVICE] Ошибка парсинга listing_data: {e}")
            else:
                logger.info(f"[CHAT LISTING SERVICE] listing_data отсутствует в БД, будем загружать из API")
            
            # Если product_url отсутствует, пытаемся извлечь из API
            if not product_url and try_extract_from_api:
                client_id = chat_dict.get('client_id')
                client_secret = chat_dict.get('client_secret')
                avito_user_id = chat_dict.get('user_id')
                avito_chat_id = chat_dict.get('chat_id')
                
                if client_id and client_secret and avito_user_id and avito_chat_id:
                    try:
                        product_url = ChatListingService._extract_product_url_from_api(
                            client_id, client_secret, avito_user_id, avito_chat_id, chat_dict.get('shop_url')
                        )
                        if product_url:
                            conn.execute('''
                                UPDATE avito_chats 
                                SET product_url = ?, updated_at = CURRENT_TIMESTAMP
                                WHERE id = ?
                            ''', (product_url, chat_id))
                            conn.commit()
                            item_id = ChatListingService.extract_item_id_from_url(product_url)
                    except Exception:
                        pass
            
            return chat_dict, product_url, item_id
        except Exception as e:
            logger.error(f"[CHAT LISTING SERVICE] Ошибка получения информации о чате {chat_id}: {e}", exc_info=True)
            return None, None, None
    
    @staticmethod
    def _extract_listing_from_chat_context(client_id: str, client_secret: str,
                                          avito_user_id: str, avito_chat_id: str) -> Optional[Dict]:
        """Извлечь данные об объявлении из context чата"""
        try:
            logger.info(f"[CHAT LISTING SERVICE] Запрос к API get_chat_by_id для чата {avito_chat_id}...")
            api = AvitoAPI(client_id=client_id, client_secret=client_secret)
            chat_info = api.get_chat_by_id(user_id=str(avito_user_id), chat_id=str(avito_chat_id))
            
            if not isinstance(chat_info, dict):
                logger.warning(f"[CHAT LISTING SERVICE] chat_info не является dict: {type(chat_info)}")
                return None
            
            context = chat_info.get('context', {})
            if not isinstance(context, dict):
                logger.warning(f"[CHAT LISTING SERVICE] context не является dict: {type(context)}")
                return None
            
            item_data = context.get('value') or context.get('item') or {}
            if not isinstance(item_data, dict) or not item_data:
                logger.warning(f"[CHAT LISTING SERVICE] item_data пустой или не dict. context keys: {list(context.keys())}")
                return None
            
            logger.info(f"[CHAT LISTING SERVICE] Извлекаем данные из item_data. Keys: {list(item_data.keys())[:10]}")
            
            listing_data = {
                'title': item_data.get('title'),
                'url': item_data.get('url'),
                'user_id': item_data.get('user_id'),
            }
            
            # Цена
            price_string = item_data.get('price_string')
            if price_string:
                match = re.search(r'(\d[\d\s]*)', price_string.replace(' ', ''))
                if match:
                    try:
                        listing_data['price'] = float(match.group(1).replace(' ', ''))
                    except (ValueError, AttributeError):
                        listing_data['price'] = None
                else:
                    listing_data['price'] = None
            else:
                listing_data['price'] = None
            
            # Изображения
            images_data = item_data.get('images', {})
            images_list = []
            if isinstance(images_data, dict):
                main_image = images_data.get('main', {})
                if isinstance(main_image, dict):
                    size_urls = [(int(m.group(1)) * int(m.group(2)), url) if (m := re.search(r'(\d+)x(\d+)', k)) 
                                else (0, url) for k, url in main_image.items() if url]
                    size_urls.sort(key=lambda x: x[0], reverse=True)
                    images_list = [url for _, url in size_urls] or [url for url in main_image.values() if url]
            listing_data['images'] = images_list
            
            # Локация
            location_data = item_data.get('location', {})
            if isinstance(location_data, dict):
                location_title = location_data.get('title')
                listing_data['location'] = location_title
                listing_data['address'] = location_title
            else:
                listing_data['location'] = None
                listing_data['address'] = None
            
            has_title = bool(listing_data.get('title'))
            has_url = bool(listing_data.get('url'))
            
            if has_title or has_url:
                logger.info(f"[CHAT LISTING SERVICE] ✅ Данные извлечены: title={has_title}, url={has_url}, price={bool(listing_data.get('price'))}, images={len(images_list)}")
                return listing_data
            else:
                logger.warning(f"[CHAT LISTING SERVICE] Данные неполные: нет title и url")
                return None
        except Exception as e:
            logger.error(f"[CHAT LISTING SERVICE] Ошибка при извлечении данных из чата: {e}", exc_info=True)
            return None
    
    @staticmethod
    def _extract_product_url_from_api(client_id: str, client_secret: str, 
                                      avito_user_id: str, avito_chat_id: str, 
                                      shop_url: Optional[str] = None) -> Optional[str]:
        """Извлечь product_url из Avito API для чата"""
        try:
            api = AvitoAPI(client_id=client_id, client_secret=client_secret)
            
            try:
                chat_details = api.get_chat_by_id(user_id=avito_user_id, chat_id=avito_chat_id)
                if isinstance(chat_details, dict):
                    value = (chat_details.get('context', {}).get('value', {}) if 
                            isinstance(chat_details.get('context'), dict) else {})
                    if isinstance(value, dict):
                        url = value.get('url')
                        if url:
                            return f"https://www.avito.ru{url}" if url.startswith('/') else (
                                f"https://www.avito.ru/{url}" if not url.startswith('http') else url)
            except Exception as e:
                if '404' in str(e):
                    try:
                        chats_list = api.get_chats(user_id=avito_user_id, limit=100, offset=0)
                        chats_array = (chats_list.get('chats') if isinstance(chats_list, dict) else 
                                     chats_list if isinstance(chats_list, list) else [])
                        
                        for chat_item in chats_array:
                            if isinstance(chat_item, dict):
                                item_id = chat_item.get('id') or chat_item.get('chat_id')
                                if item_id and str(item_id) == str(avito_chat_id):
                                    context_item = ((chat_item.get('context', {}).get('value') or 
                                                   chat_item.get('context', {}).get('item') or {}) 
                                                  if isinstance(chat_item.get('context'), dict) else {})
                                    if isinstance(context_item, dict):
                                        url = context_item.get('url')
                                        if url:
                                            return f"https://www.avito.ru{url}" if url.startswith('/') else (
                                                f"https://www.avito.ru/{url}" if not url.startswith('http') else url)
                                    break
                    except Exception:
                        pass
        except Exception:
            pass
        return None
    
    @staticmethod
    def validate_oauth_keys(chat_dict: Dict) -> Tuple[bool, Optional[str], Optional[str], Optional[str]]:
        """Проверить наличие OAuth ключей для чата"""
        client_id = chat_dict.get('client_id')
        client_secret = chat_dict.get('client_secret')
        avito_user_id = chat_dict.get('user_id')
        
        if not client_id or not client_secret or not avito_user_id:
            return False, None, None, None
        
        return True, client_id, client_secret, str(avito_user_id)
    
    @staticmethod
    def get_listing_from_avito(client_id: str, client_secret: str, 
                               avito_user_id: str, item_id: str, 
                               avito_chat_id: str, product_url: Optional[str] = None) -> Dict[str, Any]:
        """Получить информацию об объявлении из данных чата"""
        if not avito_chat_id:
            logger.warning(f"[CHAT LISTING] avito_chat_id отсутствует")
            return {}
        
        try:
            listing_info = ChatListingService._extract_listing_from_chat_context(
                client_id, client_secret, str(avito_user_id), str(avito_chat_id)
            )
            
            if listing_info:
                logger.info(f"[CHAT LISTING] Данные извлечены из API: title={bool(listing_info.get('title'))}, price={bool(listing_info.get('price'))}, images={len(listing_info.get('images', []))}")
            else:
                logger.warning(f"[CHAT LISTING] API не вернул данные об объявлении")
            
            return listing_info or {}
        except Exception as e:
            logger.error(f"[CHAT LISTING] Ошибка при извлечении данных из API: {e}", exc_info=True)
            return {}
    
    @staticmethod
    def get_chat_listing(chat_id: int, user_id: Optional[int] = None, 
                        user_role: Optional[str] = None) -> Dict[str, Any]:
        """Получить информацию об объявлении, связанном с чатом"""
            # Получаем информацию о чате
        try:
            chat_dict, product_url, item_id = ChatListingService.get_chat_listing_info(chat_id)
        except Exception as e:
            logger.error(f"[CHAT LISTING] Ошибка получения информации о чате: {e}", exc_info=True)
            return {'success': False, 'error': f'Error getting chat info: {str(e)}', 'product_url': None, 'item_id': None}
        
        if not chat_dict:
            return {'success': False, 'error': 'Chat not found', 'product_url': None, 'item_id': None}
        
        # Шаг 2: Проверяем кэш (product_url не обязателен, данные могут быть в listing_data)
        conn = get_db_connection()
        try:
            conn.execute('SELECT 1 FROM chat_listing_cache LIMIT 1')
        except (sqlite3.OperationalError, sqlite3.DatabaseError):
                conn.execute('''
                CREATE TABLE IF NOT EXISTS chat_listing_cache (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER UNIQUE NOT NULL,
                    item_id TEXT NOT NULL,
                    product_url TEXT,
                    title TEXT,
                    price REAL,
                    price_info TEXT,
                    description TEXT,
                    status TEXT,
                    category TEXT,
                    category_name TEXT,
                    location TEXT,
                    address TEXT,
                    images TEXT,
                    main_image_url TEXT,
                    listing_data TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (chat_id) REFERENCES avito_chats (id) ON DELETE CASCADE,
                    UNIQUE(chat_id, item_id)
                )
                ''')
                conn.execute('CREATE INDEX IF NOT EXISTS idx_chat_listing_cache_chat_id ON chat_listing_cache(chat_id)')
                conn.commit()
        
        cached_listing = None
        try:
            cached_listing = conn.execute('''
                SELECT id, chat_id, item_id, product_url, title, price, price_info, description, status, category, category_name, location, address, images, main_image_url, listing_data, created_at, updated_at
                FROM chat_listing_cache WHERE chat_id = ?
            ''', (chat_id,)).fetchone()
        except Exception:
            pass
        
        # Если есть в кэше - проверяем полноту данных
        if cached_listing:
            try:
                listing_data_str = cached_listing.get('listing_data')
                if listing_data_str:
                    listing_data = json.loads(listing_data_str)
                    has_title = bool(listing_data.get('title') or listing_data.get('name'))
                    has_price = listing_data.get('price') is not None or listing_data.get('price_info') is not None
                    images = listing_data.get('images') or listing_data.get('photos') or listing_data.get('pictures') or []
                    has_images = len(images) > 0 if isinstance(images, list) else bool(images)
                    
                    if has_title and has_price and has_images:
                        cache_url = listing_data.get('url') or product_url or ''
                        cache_item_id = listing_data.get('id') or item_id
                        return {'success': True, 'listing': listing_data, 'product_url': cache_url, 
                               'item_id': cache_item_id, 'from_cache': True}
                
                conn.execute('DELETE FROM chat_listing_cache WHERE chat_id = ?', (chat_id,))
                conn.commit()
            except Exception:
                pass
        
        # Шаг 4: Загружаем данные из чата
        listing_info = None
        saved_listing_data = chat_dict.get('_saved_listing_data')
        
        # Сначала проверяем сохраненные данные из синхронизации (context.value)
        if saved_listing_data and isinstance(saved_listing_data, dict):
            logger.info(f"[CHAT LISTING] ✅ Найдены сохраненные данные из синхронизации (context.value)")
            logger.info(f"[CHAT LISTING] Данные: title={bool(saved_listing_data.get('title'))}, price_string={bool(saved_listing_data.get('price_string'))}, images={bool(saved_listing_data.get('images'))}, url={bool(saved_listing_data.get('url'))}")
            
            # Используем данные напрямую из context.value, преобразуя только price_string в price
            listing_info = saved_listing_data.copy()
            
            # Преобразуем price_string в price
            price_string = listing_info.get('price_string')
            if price_string:
                match = re.search(r'(\d[\d\s]*)', price_string.replace(' ', ''))
                if match:
                    try:
                        listing_info['price'] = float(match.group(1).replace(' ', ''))
                    except (ValueError, AttributeError):
                        listing_info['price'] = None
                else:
                    listing_info['price'] = None
                
            # Преобразуем images.main в список URL
            images_data = listing_info.get('images', {})
            if isinstance(images_data, dict):
                main_image = images_data.get('main', {})
                if isinstance(main_image, dict):
                    # Собираем все URL изображений, сортируя по размеру
                    size_urls = []
                    for size_key, img_url in main_image.items():
                        if img_url:
                            match = re.search(r'(\d+)x(\d+)', size_key)
                            if match:
                                size_urls.append((int(match.group(1)) * int(match.group(2)), img_url))
                            else:
                                size_urls.append((0, img_url))
                    size_urls.sort(key=lambda x: x[0], reverse=True)
                    listing_info['images'] = [url for _, url in size_urls] if size_urls else []
            elif not isinstance(images_data, list):
                listing_info['images'] = []
            else:
                listing_info['images'] = []
            
            # Преобразуем location в строку
            location_data = listing_info.get('location', {})
            if isinstance(location_data, dict):
                listing_info['address'] = location_data.get('title', '')
            elif not listing_info.get('address'):
                listing_info['address'] = ''
            
            # Проверяем, что есть хотя бы title или url
            if not listing_info.get('title') and not listing_info.get('url'):
                logger.warning(f"[CHAT LISTING] Сохраненные данные неполные (нет title и url), загружаем из API")
                listing_info = None
            else:
                logger.info(f"[CHAT LISTING] ✅ Данные готовы: title={bool(listing_info.get('title'))}, price={bool(listing_info.get('price'))}, images={len(listing_info.get('images', []))}")
        
        # Если нет сохраненных данных или они неполные, загружаем из API
        if not listing_info:
            logger.info(f"[CHAT LISTING] Загружаем данные из API чата...")
            try:
                is_valid, client_id, client_secret, avito_user_id = ChatListingService.validate_oauth_keys(chat_dict)
                avito_chat_id = chat_dict.get('chat_id')
                
                if is_valid and avito_chat_id:
                    logger.info(f"[CHAT LISTING] OAuth ключи валидны, запрашиваем данные из API...")
                    listing_info = ChatListingService.get_listing_from_avito(
                        client_id, client_secret, avito_user_id, item_id or '', avito_chat_id, product_url
                    )
                    if listing_info:
                        logger.info(f"[CHAT LISTING] Данные получены из API: title={bool(listing_info.get('title'))}, price={bool(listing_info.get('price'))}, images={len(listing_info.get('images', []))}")
                    else:
                        logger.warning(f"[CHAT LISTING] API вернул пустые данные")
                else:
                    if not is_valid:
                        logger.warning(f"[CHAT LISTING] OAuth ключи не валидны")
                    if not avito_chat_id:
                        logger.warning(f"[CHAT LISTING] avito_chat_id отсутствует")
            except Exception as api_error:
                logger.error(f"[CHAT LISTING] Ошибка при загрузке из API: {api_error}", exc_info=True)
                # Не прерываем выполнение, продолжаем с тем, что есть
        
        # Если нет данных, но есть сохраненные данные из синхронизации, используем их
        if not listing_info or (isinstance(listing_info, dict) and not listing_info.get('title') and not listing_info.get('url')):
            # Используем сохраненные данные из синхронизации (context.value)
            if saved_listing_data and isinstance(saved_listing_data, dict):
                logger.info(f"[CHAT LISTING] Используем сохраненные данные из синхронизации (context.value)")
                listing_info = saved_listing_data.copy()
                
                # Преобразуем price_string в price если нужно
                if 'price' not in listing_info or listing_info.get('price') is None:
                    price_string = listing_info.get('price_string')
                    if price_string:
                        match = re.search(r'(\d[\d\s]*)', price_string.replace(' ', ''))
                        if match:
                            try:
                                listing_info['price'] = float(match.group(1).replace(' ', ''))
                            except (ValueError, AttributeError):
                                listing_info['price'] = None
                
                # Преобразуем images.main в список URL если нужно
                images_data = listing_info.get('images', {})
                if isinstance(images_data, dict) and not isinstance(images_data, list):
                    main_image = images_data.get('main', {})
                    if isinstance(main_image, dict):
                        size_urls = []
                        for size_key, img_url in main_image.items():
                            if img_url:
                                match = re.search(r'(\d+)x(\d+)', size_key)
                                if match:
                                    size_urls.append((int(match.group(1)) * int(match.group(2)), img_url))
                                else:
                                    size_urls.append((0, img_url))
                        size_urls.sort(key=lambda x: x[0], reverse=True)
                        listing_info['images'] = [url for _, url in size_urls] if size_urls else []
                    else:
                        listing_info['images'] = []
                elif not isinstance(images_data, list):
                    listing_info['images'] = []
                
                logger.info(f"[CHAT LISTING] ✅ Данные из сохраненных: title={bool(listing_info.get('title'))}, price={bool(listing_info.get('price'))}, images={len(listing_info.get('images', []))}")
            else:
                # Если нет сохраненных данных, возвращаем ошибку
                logger.warning(f"[CHAT LISTING] Не удалось получить данные объявления. saved_listing_data={bool(saved_listing_data)}, listing_info={bool(listing_info)}")
                return {'success': False, 'error': 'Failed to extract listing data from chat', 
                       'product_url': product_url or '', 'item_id': item_id}
        
        # Нормализуем данные
        try:
            listing_info = ChatListingService.normalize_listing_data(listing_info)
        except Exception as normalize_error:
            logger.error(f"[CHAT LISTING] Ошибка нормализации данных: {normalize_error}", exc_info=True)
            # Используем исходные данные, если нормализация не удалась
            if not isinstance(listing_info, dict):
                listing_info = {}
            
        # Проверяем полноту данных перед сохранением
        has_title = bool(listing_info.get('title'))
        has_price = listing_info.get('price') is not None and listing_info.get('price') != 0
        images = listing_info.get('images', [])
        has_images = len(images) > 0 if isinstance(images, list) else bool(images)
        
        # Извлекаем URL из данных если product_url отсутствует
        final_url = product_url or listing_info.get('url') or ''
        
        # Всегда возвращаем данные, даже если они неполные
        # Сохраняем в кэш только если данные полные
        if has_title and has_price and has_images:
            try:
                import hashlib
                cache_item_id = item_id or (hashlib.md5(final_url.encode()).hexdigest()[:12] if final_url else str(chat_id))
                ChatListingService.save_listing_to_cache(conn, chat_id, cache_item_id, final_url, listing_info)
            except Exception:
                pass
        else:
            try:
                conn.execute('DELETE FROM chat_listing_cache WHERE chat_id = ?', (chat_id,))
                conn.commit()
            except Exception:
                pass
        
        return {'success': True, 'listing': listing_info, 'product_url': final_url, 
               'item_id': item_id, 'from_cache': False}
    
    @staticmethod
    def normalize_listing_data(listing_info: Dict) -> Dict:
        """Нормализовать данные объявления для единообразного формата"""
        normalized = {
            'title': listing_info.get('title') or listing_info.get('name') or listing_info.get('heading') or '',
            'description': listing_info.get('description') or listing_info.get('text') or 
                         listing_info.get('content') or listing_info.get('about') or '',
            'status': listing_info.get('status') or listing_info.get('state') or '',
            'id': listing_info.get('id') or listing_info.get('item_id') or listing_info.get('listing_id') or '',
            'url': listing_info.get('url') or listing_info.get('link') or '',
        }
            
        # Price
        price = listing_info.get('price')
        if price is None:
            price_info = listing_info.get('price_info', {})
            if isinstance(price_info, dict):
                price = price_info.get('value') or price_info.get('price')
        if price is None:
            price = listing_info.get('priceValue') or listing_info.get('price_value')
        normalized['price'] = price
        if listing_info.get('price_info'):
            normalized['price_info'] = listing_info.get('price_info')
        
        # Images
        images = listing_info.get('images') or listing_info.get('photos') or listing_info.get('pictures') or []
        image_urls = []
        for img in images:
            if isinstance(img, dict):
                img_url = (img.get('url') or img.get('urls', {}).get('large') or 
                         img.get('urls', {}).get('medium') or img.get('urls', {}).get('small') or
                         img.get('full') or img.get('original') or img.get('src') or img.get('href'))
                if img_url and isinstance(img_url, str):
                    image_urls.append(img_url)
            elif isinstance(img, str):
                image_urls.append(img)
        normalized['images'] = image_urls
        
        # Category
        category = listing_info.get('category', {})
        if isinstance(category, dict):
            normalized['category'] = category
            normalized['category_name'] = category.get('name') or category.get('title') or category.get('label') or ''
        elif category:
            normalized['category'] = {'name': str(category)}
            normalized['category_name'] = str(category)
        else:
            normalized['category'] = {}
            normalized['category_name'] = ''
        
        # Location и Address
        location = listing_info.get('location', {})
        if isinstance(location, dict):
            normalized['location'] = location
            address = (location.get('name') or location.get('address') or 
                      location.get('fullName') or location.get('full_name') or '')
            if not address:
                parts = [str(location[k]) for k in ('region', 'city', 'district') if location.get(k)]
                address = ', '.join(parts) if parts else ''
            normalized['address'] = address
        elif location:
            normalized['location'] = {'name': str(location)}
            normalized['address'] = str(location)
        else:
            normalized['location'] = {}
            normalized['address'] = listing_info.get('address') or ''
        
        # Сохраняем все остальные поля
        normalized.update({k: v for k, v in listing_info.items() if k not in normalized})
        
        return normalized
    
    @staticmethod
    def save_listing_to_cache(conn, chat_id: int, item_id: str, product_url: str, listing_info: Dict) -> None:
        """Сохранить данные объявления в кэш"""
        try:
            title = listing_info.get('title', '')
            price = listing_info.get('price')
            description = listing_info.get('description', '')
            status = listing_info.get('status', '')
            category = listing_info.get('category', {})
            category_name = listing_info.get('category_name', '')
            location = listing_info.get('location', {})
            address = listing_info.get('address', '')
            images = listing_info.get('images', [])
            main_image_url = images[0] if images else None
            
            conn.execute('''
                INSERT OR REPLACE INTO chat_listing_cache 
                (chat_id, item_id, product_url, title, price, price_info, description, status, 
                 category, category_name, location, address, images, main_image_url, listing_data, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (
                chat_id,
                item_id,
                product_url,
                title,
                price,
                json.dumps(listing_info.get('price_info', {})) if listing_info.get('price_info') else None,
                description,
                status,
                json.dumps(category) if isinstance(category, dict) else str(category) if category else None,
                category_name,
                json.dumps(location) if isinstance(location, dict) else str(location) if location else None,
                address,
                json.dumps(images) if images else None,
                main_image_url,
                json.dumps(listing_info)
            ))
            conn.commit()
        except Exception as e:
            logger.error(f"[CHAT LISTING] Ошибка сохранения в кэш: {str(e)}", exc_info=True)
            conn.rollback()
