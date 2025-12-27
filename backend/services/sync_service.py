"""
Sync Service - синхронизация данных с Avito API
"""
import logging
from typing import Dict, List
from datetime import datetime

logger = logging.getLogger(__name__)


class SyncService:
    """Сервис автоматической синхронизации с Avito"""
    
    def __init__(self, db_connection):
        self.conn = db_connection
    
    @staticmethod
    def to_str(value, default=''):
        """Безопасное преобразование в строку"""
        if value is None:
            return default
        return str(value)
    
    def sync_all_shops(self) -> Dict:
        """
        Синхронизировать все магазины
        
        Returns:
            Dict: Результаты синхронизации
        """
        from avito_api import AvitoAPI
        
        shops = self.conn.execute('''
            SELECT id, name, client_id, client_secret, user_id
            FROM avito_shops
            WHERE is_active = 1
            AND client_id IS NOT NULL
            AND user_id IS NOT NULL
        ''').fetchall()
        
        results = {
            'shops_total': len(shops),
            'shops_success': 0,
            'shops_failed': 0,
            'chats_created': 0,
            'chats_updated': 0,
            'messages_created': 0,
            'shops_details': []
        }
        
        for shop in shops:
            shop_result = self.sync_shop(shop)
            results['shops_details'].append(shop_result)
            
            if shop_result['success']:
                results['shops_success'] += 1
                results['chats_created'] += shop_result.get('chats_created', 0)
                results['chats_updated'] += shop_result.get('chats_updated', 0)
                results['messages_created'] += shop_result.get('messages_created', 0)
            else:
                results['shops_failed'] += 1
        
        return results
    
    def sync_shop(self, shop: Dict) -> Dict:
        """
        Синхронизировать один магазин
        
        Args:
            shop: Данные магазина из БД
        
        Returns:
            Dict: Результат синхронизации
        """
        from avito_api import AvitoAPI
        from services.messenger_service import MessengerService
        
        result = {
            'shop_id': shop['id'],
            'shop_name': shop['name'],
            'success': False,
            'chats_created': 0,
            'chats_updated': 0,
            'messages_created': 0,
            'error': None
        }
        
        try:
            api = AvitoAPI(
                client_id=shop['client_id'],
                client_secret=shop['client_secret']
            )
            
            # Получаем чаты
            chats_response = api.get_chats(user_id=shop['user_id'], limit=100, offset=0, timeout=10)
            
            if isinstance(chats_response, dict):
                chats_list = chats_response.get('chats', chats_response.get('items', []))
            else:
                chats_list = chats_response if isinstance(chats_response, list) else []
            
            logger.info(f"Магазин {shop['name']}: получено {len(chats_list)} чатов")
            
            for api_chat in chats_list:
                chat_result = self.sync_chat(shop, api_chat, api)
                result['chats_created'] += chat_result['created']
                result['chats_updated'] += chat_result['updated']
                result['messages_created'] += chat_result['messages']
            
            self.conn.commit()
            result['success'] = True
            
        except Exception as e:
            error_str = str(e)
            if '403' in error_str or 'permission denied' in error_str.lower():
                result['error'] = 'Permission denied (нужен тариф Pro/Максимальный)'
                logger.warning(f"Магазин {shop['name']}: нет доступа к Messenger API")
            else:
                result['error'] = str(e)
                logger.error(f"Ошибка синхронизации магазина {shop['name']}: {e}", exc_info=True)
        
        return result
    
    def sync_chat(self, shop: Dict, api_chat: Dict, api) -> Dict:
        """
        Синхронизировать один чат
        
        Args:
            shop: Данные магазина
            api_chat: Данные чата из API
            api: Экземпляр AvitoAPI
        
        Returns:
            Dict: {'created': int, 'updated': int, 'messages': int}
        """
        from services.messenger_service import MessengerService
        
        api_chat_id = self.to_str(api_chat.get('id'))
        if not api_chat_id:
            return {'created': 0, 'updated': 0, 'messages': 0}
        
        # Извлекаем last_message
        last_message_data = api_chat.get('last_message', {})
        if isinstance(last_message_data, dict):
            last_message = self.to_str(last_message_data.get('text', last_message_data.get('content', '')))
        else:
            last_message = self.to_str(last_message_data)
        
        if not last_message or last_message == 'None':
            last_message = ''
        
        # Имя клиента и customer_id
        users = api_chat.get('users', [])
        client_name = 'Клиент'
        customer_id = None
        
        for user in users:
            if isinstance(user, dict):
                user_id = self.to_str(user.get('id', ''))
                if user_id != self.to_str(shop['user_id']):
                    client_name = self.to_str(user.get('name', user.get('username', 'Клиент')), 'Клиент')
                    customer_id = user_id
                    break
        
        unread_count = api_chat.get('unread_count', 0)
        if not isinstance(unread_count, int):
            unread_count = 0
        
        # Извлекаем product_url из данных чата
        # Согласно документации Avito API: https://developers.avito.ru/api-catalog
        # Информация об объявлении может быть в полях: context.item, item, listing, ad
        product_url = None
        
        logger.info(f"[SYNC CHAT] Извлечение product_url для чата {api_chat_id}")
        logger.debug(f"[SYNC CHAT] Структура api_chat: keys={list(api_chat.keys())}")
        
        # ВАЖНО: Avito API v3 возвращает context.value, а не context.item!
        # Структура: {"context": {"type": "item", "value": {"id": 123, "url": "..."}}}
        context = api_chat.get('context', {})
        logger.debug(f"[SYNC CHAT] context type: {type(context)}, value: {context}")
        
        if isinstance(context, dict):
            # Приоритет: context.value (API v3), затем context.item (старая версия)
            item_data = (context.get('value') or 
                        context.get('item') or 
                        context.get('listing') or 
                        context.get('ad', {}))
            logger.debug(f"[SYNC CHAT] context.value/item type: {type(item_data)}, value: {item_data}")
        else:
            # Fallback на прямые поля в api_chat
            item_data = api_chat.get('item', api_chat.get('listing', api_chat.get('ad', {})))
            logger.debug(f"[SYNC CHAT] api_chat.item type: {type(item_data)}, value: {item_data}")
        
        # Сохраняем полные данные объявления из context.value
        listing_data_json = None
        if isinstance(item_data, dict) and item_data:
            # Сохраняем ВСЕ данные из context.value (title, price_string, images, url, location и т.д.)
            import json
            listing_data_json = json.dumps(item_data, ensure_ascii=False)
            logger.info(f"[SYNC CHAT] Сохранены данные объявления из context.value: title={bool(item_data.get('title'))}, price_string={bool(item_data.get('price_string'))}, images={bool(item_data.get('images'))}, url={bool(item_data.get('url'))}")
            
            # Пробуем разные варианты ключей для URL
            # Согласно документации, item может содержать: id, url, или другие поля
            item_id = item_data.get('id')
            logger.debug(f"[SYNC CHAT] item_data keys: {list(item_data.keys())}, item_id: {item_id}")
            
            product_url = (item_data.get('url') or 
                          item_data.get('link') or 
                          item_data.get('href') or
                          item_data.get('value') or
                          item_data.get('uri'))
            
            logger.debug(f"[SYNC CHAT] product_url из item_data: {product_url}")
            
            # Сохраняем полные данные объявления (title, price_string, images, location)
            import json
            listing_data_json = json.dumps(item_data, ensure_ascii=False)
            logger.info(f"[SYNC CHAT] Сохранены данные объявления из context.value: title={bool(item_data.get('title'))}, price={bool(item_data.get('price_string'))}, images={bool(item_data.get('images'))}")
            
            # Если URL не найден, но есть ID, формируем URL из ID
            if not product_url and item_id:
                item_id_str = str(item_id)
                # Формируем URL на основе ID объявления
                shop_url_part = shop.get('shop_url', '').split('/')[-1] if shop.get('shop_url') else ''
                if shop_url_part:
                    product_url = f"https://www.avito.ru/{shop_url_part}/items/{item_id_str}"
                else:
                    product_url = f"https://www.avito.ru/items/{item_id_str}"
                logger.info(f"[SYNC CHAT] product_url сформирован из item_id: {product_url}")
            
            # Если URL относительный, делаем его абсолютным
            if product_url and isinstance(product_url, str):
                if product_url.startswith('/'):
                    product_url = f"https://www.avito.ru{product_url}"
                    logger.debug(f"[SYNC CHAT] product_url преобразован из относительного: {product_url}")
                elif not product_url.startswith('http'):
                    # Если это ID объявления, формируем URL
                    shop_url_part = shop.get('shop_url', '').split('/')[-1] if shop.get('shop_url') else ''
                    if shop_url_part:
                        product_url = f"https://www.avito.ru/{shop_url_part}/items/{product_url}"
                    else:
                        product_url = f"https://www.avito.ru/items/{product_url}"
                    logger.debug(f"[SYNC CHAT] product_url сформирован из строки: {product_url}")
        elif isinstance(item_data, str):
            logger.debug(f"[SYNC CHAT] item_data - строка: {item_data}")
            if item_data.startswith('http'):
                product_url = item_data
            elif item_data.isdigit():
                shop_url_part = shop.get('shop_url', '').split('/')[-1] if shop.get('shop_url') else ''
                if shop_url_part:
                    product_url = f"https://www.avito.ru/{shop_url_part}/items/{item_data}"
                else:
                    product_url = f"https://www.avito.ru/items/{item_data}"
                logger.info(f"[SYNC CHAT] product_url сформирован из строки-ID: {product_url}")
        
        # Также проверяем прямые поля в api_chat (для обратной совместимости)
        if not product_url:
            product_url = (api_chat.get('item_url') or 
                          api_chat.get('listing_url') or 
                          api_chat.get('ad_url') or
                          api_chat.get('product_url'))
            logger.debug(f"[SYNC CHAT] product_url из прямых полей api_chat: {product_url}")
        
        # Если product_url или listing_data отсутствуют, пытаемся получить через get_chat_by_id
        if (not product_url or not listing_data_json) and api:
            try:
                logger.info(f"[SYNC CHAT] Запрашиваем get_chat_by_id для {api_chat_id} (product_url={bool(product_url)}, listing_data={bool(listing_data_json)})")
                chat_details = api.get_chat_by_id(
                    user_id=shop['user_id'],
                    chat_id=api_chat_id
                )
                if isinstance(chat_details, dict):
                    logger.debug(f"[SYNC CHAT] chat_details keys: {list(chat_details.keys())}")
                    # ВАЖНО: Avito API v3 возвращает context.value, а не context.item!
                    # Структура: {"context": {"type": "item", "value": {"id": 123, "url": "...", "title": "...", "price_string": "...", "images": {...}}}}
                    detail_context = chat_details.get('context', {})
                    if isinstance(detail_context, dict):
                        # Приоритет: context.value (API v3), затем context.item (старая версия)
                        detail_item = (detail_context.get('value') or 
                                      detail_context.get('item') or 
                                      detail_context.get('listing') or 
                                      detail_context.get('ad', {}))
                        if isinstance(detail_item, dict) and detail_item:
                            # Сохраняем полные данные из get_chat_by_id, если их еще нет
                            if not listing_data_json:
                                import json
                                listing_data_json = json.dumps(detail_item, ensure_ascii=False)
                                logger.info(f"[SYNC CHAT] ✅ Сохранены данные из get_chat_by_id context.value: title={bool(detail_item.get('title'))}, price_string={bool(detail_item.get('price_string'))}, images={bool(detail_item.get('images'))}")
                            
                            detail_item_id = detail_item.get('id')
                            detail_url = (detail_item.get('url') or 
                                         detail_item.get('link') or 
                                         detail_item.get('href') or
                                         detail_item.get('value') or
                                         detail_item.get('uri'))
                            if detail_url and not product_url:
                                product_url = detail_url
                                if product_url.startswith('/'):
                                    product_url = f"https://www.avito.ru{product_url}"
                                elif not product_url.startswith('http'):
                                    product_url = f"https://www.avito.ru{product_url}"
                                logger.info(f"[SYNC CHAT] ✅ product_url найден через get_chat_by_id context.value (url): {product_url}")
                            elif detail_item_id and not product_url:
                                item_id_str = str(detail_item_id)
                                shop_url_part = shop.get('shop_url', '').split('/')[-1] if shop.get('shop_url') else ''
                                if shop_url_part:
                                    product_url = f"https://www.avito.ru/{shop_url_part}/items/{item_id_str}"
                                else:
                                    product_url = f"https://www.avito.ru/items/{item_id_str}"
                                logger.info(f"[SYNC CHAT] ✅ product_url найден через get_chat_by_id context.value (id): {product_url}")
                    
                    # Если не нашли в context, проверяем прямые поля
                    if not product_url:
                        product_url = (chat_details.get('item_url') or 
                                     chat_details.get('listing_url') or 
                                     chat_details.get('ad_url') or
                                     chat_details.get('product_url'))
                        if product_url:
                            logger.info(f"[SYNC CHAT] ✅ product_url найден через get_chat_by_id (прямые поля): {product_url}")
            except Exception as api_error:
                logger.warning(f"[SYNC CHAT] Ошибка при попытке получить данные через get_chat_by_id: {api_error}")
        
        if product_url:
            logger.info(f"[SYNC CHAT] ✅ product_url найден: {product_url}")
        else:
            logger.warning(f"[SYNC CHAT] ⚠️ product_url НЕ найден для чата {api_chat_id}. Все доступные ключи api_chat: {list(api_chat.keys())}")
        
        # Проверяем существование
        existing = self.conn.execute('''
            SELECT id FROM avito_chats 
            WHERE shop_id = ? AND chat_id = ?
        ''', (shop['id'], api_chat_id)).fetchone()
        
        created = 0
        updated = 0
        messages = 0
        
        if existing:
            # Обновляем
            logger.info(f"[SYNC CHAT] Обновление чата {existing['id']} с product_url={product_url}")
            # Проверяем наличие поля listing_data в таблице
            try:
                self.conn.execute('''
                    UPDATE avito_chats
                    SET last_message = ?, updated_at = CURRENT_TIMESTAMP,
                        client_name = ?, unread_count = ?, customer_id = ?, product_url = ?, listing_data = ?
                    WHERE id = ?
                ''', (last_message, client_name, unread_count, customer_id, product_url if product_url else None, listing_data_json, existing['id']))
            except Exception:
                # Если поля нет, добавляем его
                try:
                    self.conn.execute('ALTER TABLE avito_chats ADD COLUMN listing_data TEXT')
                    self.conn.commit()
                    self.conn.execute('''
                        UPDATE avito_chats
                        SET last_message = ?, updated_at = CURRENT_TIMESTAMP,
                            client_name = ?, unread_count = ?, customer_id = ?, product_url = ?, listing_data = ?
                        WHERE id = ?
                    ''', (last_message, client_name, unread_count, customer_id, product_url if product_url else None, listing_data_json, existing['id']))
                except Exception:
                    # Если не удалось добавить поле, обновляем без него
                    self.conn.execute('''
                        UPDATE avito_chats
                        SET last_message = ?, updated_at = CURRENT_TIMESTAMP,
                            client_name = ?, unread_count = ?, customer_id = ?, product_url = ?
                        WHERE id = ?
                    ''', (last_message, client_name, unread_count, customer_id, product_url if product_url else None, existing['id']))
            self.conn.commit()  # Явно коммитим изменения
            updated = 1
            chat_db_id = existing['id']
            
            # Проверяем, что product_url действительно сохранился
            verify_chat = self.conn.execute('''
                SELECT product_url FROM avito_chats WHERE id = ?
            ''', (chat_db_id,)).fetchone()
            if verify_chat:
                logger.info(f"[SYNC CHAT] Проверка сохранения: product_url в БД = {verify_chat.get('product_url')}")
        else:
            # Создаём
            logger.info(f"[SYNC CHAT] Создание нового чата с product_url={product_url}")
            # Проверяем наличие поля listing_data в таблице
            try:
                cursor = self.conn.execute('''
                    INSERT INTO avito_chats (shop_id, chat_id, client_name, last_message, status, priority, unread_count, customer_id, product_url, listing_data)
                    VALUES (?, ?, ?, ?, 'active', 'new', ?, ?, ?, ?)
                ''', (shop['id'], api_chat_id, client_name, last_message, unread_count, customer_id, product_url if product_url else None, listing_data_json))
            except Exception:
                # Если поля нет, добавляем его
                try:
                    self.conn.execute('ALTER TABLE avito_chats ADD COLUMN listing_data TEXT')
                    self.conn.commit()
                    cursor = self.conn.execute('''
                        INSERT INTO avito_chats (shop_id, chat_id, client_name, last_message, status, priority, unread_count, customer_id, product_url, listing_data)
                        VALUES (?, ?, ?, ?, 'active', 'new', ?, ?, ?, ?)
                    ''', (shop['id'], api_chat_id, client_name, last_message, unread_count, customer_id, product_url if product_url else None, listing_data_json))
                except Exception:
                    # Если не удалось добавить поле, создаем без него
                    cursor = self.conn.execute('''
                        INSERT INTO avito_chats (shop_id, chat_id, client_name, last_message, status, priority, unread_count, customer_id, product_url)
                        VALUES (?, ?, ?, ?, 'active', 'new', ?, ?, ?)
                    ''', (shop['id'], api_chat_id, client_name, last_message, unread_count, customer_id, product_url if product_url else None))
            self.conn.commit()  # Явно коммитим изменения
            created = 1
            chat_db_id = cursor.lastrowid
            
            # Проверяем, что product_url действительно сохранился
            verify_chat = self.conn.execute('''
                SELECT product_url FROM avito_chats WHERE id = ?
            ''', (chat_db_id,)).fetchone()
            if verify_chat:
                logger.info(f"[SYNC CHAT] Проверка сохранения: product_url в БД = {verify_chat.get('product_url')}")
        
        # Синхронизируем сообщения
        try:
            messenger_service = MessengerService(self.conn, api)
            messages = messenger_service.sync_chat_messages(
                chat_id=chat_db_id,
                user_id=shop['user_id'],
                avito_chat_id=api_chat_id
            )
        except:
            messages = 0
        
        return {'created': created, 'updated': updated, 'messages': messages}

