"""
Chats API - endpoints для работы с чатами
"""
from flask import Blueprint, request, jsonify, session, current_app
from functools import wraps
import logging
from logging.handlers import RotatingFileHandler
import os

# Настройка логирования для этого модуля
# Используем тот же logger, что и в app.py для консистентности
logger = logging.getLogger('app')


def handle_errors(f):
    """Декоратор для обработки ошибок"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as error:
            logger.error(f"Ошибка в {f.__name__}: {error}", exc_info=True)
            return jsonify({'error': str(error), 'code': 'INTERNAL_ERROR'}), 500
    return decorated_function

chats_bp = Blueprint('chats_api', __name__, url_prefix='/api/chats')


def require_auth(f):
    """Декоратор проверки аутентификации"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            return jsonify({'error': 'Not authenticated'}), 401
        return f(*args, **kwargs)
    return decorated_function


def _ensure_manager_can_access_chat(chat_row) -> bool:
    """
    Проверяет доступ к чату.
    Все аутентифицированные пользователи (менеджеры, админы, супер-админы) 
    имеют доступ ко всем чатам.
    """
    # Если пользователь аутентифицирован (проверка выполняется в @require_auth),
    # то он имеет доступ ко всем чатам
    # Просто проверяем, что чат существует
    return chat_row is not None


@chats_bp.route('/', methods=['GET'])
@require_auth
@handle_errors
def get_chats():
    """Получить список чатов"""
    logger.info(f"[CHATS_API] Запрос получен через chats_bp blueprint. Session: user_id={session.get('user_id')}")
    from database import get_db_connection
    from services.messenger_service import MessengerService
    from avito_api import AvitoAPI
    import sqlite3
    
    limit = max(1, min(request.args.get('limit', default=100, type=int), 500))
    offset = max(0, request.args.get('offset', default=0, type=int))

    # Пробуем получить соединение с обработкой ошибок диска
    max_retries = 3
    retry_count = 0
    conn = None
    
    while retry_count < max_retries:
        try:
            conn = get_db_connection()
            break
        except (sqlite3.OperationalError, RuntimeError) as conn_error:
            error_msg = str(conn_error).lower()
            if "disk i/o error" in error_msg or "i/o error" in error_msg:
                retry_count += 1
                if retry_count >= max_retries:
                    logger.error(f"[GET_CHATS] Disk I/O error after {max_retries} retries: {conn_error}")
                    return jsonify({
                        'error': 'Internal server error',
                        'message': 'disk I/O error',
                        'code': 'DISK_IO_ERROR'
                    }), 500
                import time
                time.sleep(0.1 * retry_count)  # Увеличиваем задержку с каждой попыткой
                continue
            else:
                # Другая ошибка, пробрасываем дальше
                raise
    
    if conn is None:
        logger.error("[GET_CHATS] Failed to get database connection")
        return jsonify({
            'error': 'Internal server error',
            'message': 'Failed to connect to database',
            'code': 'DB_CONNECTION_ERROR'
        }), 500
    
    try:
        # Параметры фильтрации
        shop_id = request.args.get('shop_id', type=int)
        pool_only = request.args.get('pool', 'false').lower() == 'true'
        
        # Убраны фильтры по менеджерам - все видят все чаты
        manager_id = None
        
        # Создаём сервис (без API, так как только читаем из БД)
        service = MessengerService(conn, None)
        
        # Пробуем выполнить запрос с повторными попытками при disk I/O ошибках
        max_query_retries = 3
        query_retry_count = 0
        chats = None
        total = 0
        
        while query_retry_count < max_query_retries:
            try:
                chats, total = service.get_chats_list(
                    shop_id=shop_id,
                    manager_id=manager_id,  # Всегда None - убраны фильтры
                    pool_only=pool_only,
                    limit=limit,
                    offset=offset,
                    with_total=True
                )
                break  # Успешно выполнили запрос
            except sqlite3.OperationalError as query_error:
                error_msg = str(query_error).lower()
                if ("disk i/o error" in error_msg or "i/o error" in error_msg) and query_retry_count < max_query_retries - 1:
                    query_retry_count += 1
                    logger.warning(f"[GET_CHATS] Disk I/O error during query (attempt {query_retry_count}/{max_query_retries}), retrying...")
                    
                    # Сбрасываем глобальное соединение через get_db_connection
                    # (он сам обработает переподключение)
                    import time
                    time.sleep(0.1 * query_retry_count)
                    
                    # Получаем новое соединение (get_db_connection сам переподключится)
                    try:
                        # Принудительно сбрасываем соединение, вызвав get_db_connection
                        # который обнаружит проблему и переподключится
                        conn = get_db_connection()
                        service = MessengerService(conn, None)
                    except Exception as reconnect_error:
                        logger.error(f"[GET_CHATS] Failed to reconnect: {reconnect_error}")
                        if query_retry_count >= max_query_retries:
                            raise
                        continue
                else:
                    # Другая ошибка или все попытки исчерпаны
                    raise
        
        if chats is None:
            raise RuntimeError("Failed to get chats after all retries")
        
        # Соединение глобальное, не закрываем
        
        response = jsonify({
            'items': chats,
            'total': total,
            'limit': limit,
            'offset': offset,
            'has_more': offset + limit < total
        })
        response.headers['X-Total-Count'] = total
        response.headers['X-Limit'] = limit
        response.headers['X-Offset'] = offset
        return response
    except sqlite3.OperationalError as db_error:
        error_msg = str(db_error).lower()
        if "disk i/o error" in error_msg or "i/o error" in error_msg:
            logger.error(f"[GET_CHATS] Disk I/O error during query: {db_error}")
            return jsonify({
                'error': 'Internal server error',
                'message': 'disk I/O error',
                'code': 'DISK_IO_ERROR'
            }), 500
        else:
            logger.error(f"[GET_CHATS] Database error: {db_error}", exc_info=True)
            return jsonify({
                'error': 'Internal server error',
                'message': str(db_error),
                'code': 'DB_ERROR'
            }), 500


@chats_bp.route('/<int:chat_id>/messages', methods=['GET'])
@require_auth
def get_messages(chat_id):
    """Получить сообщения чата"""
    from database import get_db_connection
    from services.messenger_service import MessengerService
    from avito_api import AvitoAPI
    
    try:
        limit = max(1, min(int(request.args.get('limit', 100)), 500))
        offset = max(0, int(request.args.get('offset', 0)))
        sync = request.args.get('sync', 'false').lower() == 'true'
        
        conn = get_db_connection()
        
        # Получаем данные чата
        try:
            chat_row = conn.execute('''
                SELECT ac.*, s.client_id, s.client_secret, s.user_id
                FROM avito_chats ac
                JOIN avito_shops s ON ac.shop_id = s.id
                WHERE ac.id = ?
            ''', (chat_id,)).fetchone()
        except Exception as e:
            logger.error(f"[API/MESSAGES] Ошибка получения данных чата {chat_id}: {e}", exc_info=True)
            return jsonify({'error': f'Database error: {str(e)}', 'code': 'DB_ERROR'}), 500
        
        if not chat_row:
            # Соединение глобальное, не закрываем
            return jsonify({'error': 'Chat not found'}), 404

        # Преобразуем в dict для безопасного доступа
        chat = dict(chat_row) if not isinstance(chat_row, dict) else chat_row

        if not _ensure_manager_can_access_chat(chat):
            # Соединение глобальное, не закрываем
            return jsonify({'error': 'Access denied'}), 403
        
        # Создаём API если нужна синхронизация
        logger.info(f"[API/MESSAGES] Запрос сообщений для чата {chat_id}, sync={sync}, client_id={bool(chat.get('client_id'))}, client_secret={bool(chat.get('client_secret'))}, user_id={chat.get('user_id')}")
        
        service = None
        if sync and chat.get('client_id') and chat.get('client_secret') and chat.get('user_id'):
            try:
                logger.info(f"[API/MESSAGES] Начинаем синхронизацию сообщений для чата {chat_id}, user_id={chat['user_id']}, avito_chat_id={chat.get('chat_id')}")
                api = AvitoAPI(
                    client_id=chat['client_id'],
                    client_secret=chat['client_secret']
                )
                service = MessengerService(conn, api)
                
                # Синхронизируем
                new_messages_count = service.sync_chat_messages(
                    chat_id=chat_id,
                    user_id=chat['user_id'],
                    avito_chat_id=chat.get('chat_id') or ''
                )
                logger.info(f"[API/MESSAGES] Синхронизация завершена: {new_messages_count} новых сообщений для чата {chat_id}")
            except Exception as sync_error:
                logger.error(f"[API/MESSAGES] Ошибка синхронизации сообщений для чата {chat_id}: {sync_error}", exc_info=True)
                # Продолжаем без синхронизации
                service = MessengerService(conn, None)
        else:
            if sync:
                logger.warning(f"[API/MESSAGES] Синхронизация запрошена, но не выполнена: client_id={bool(chat.get('client_id'))}, client_secret={bool(chat.get('client_secret'))}, user_id={bool(chat.get('user_id'))}")
            service = MessengerService(conn, None)
        
        # Получаем сообщения
        try:
            messages, total = service.get_chat_messages(chat_id, limit, offset)
        except Exception as msg_error:
            logger.error(f"[API/MESSAGES] Ошибка получения сообщений для чата {chat_id}: {msg_error}", exc_info=True)
            import traceback
            logger.error(f"[API/MESSAGES] Traceback получения сообщений: {traceback.format_exc()}")
            return jsonify({'error': f'Error getting messages: {str(msg_error)}', 'code': 'MESSAGES_ERROR'}), 500
        
        logger.info(f"[API/MESSAGES] Возвращаем {len(messages)} сообщений из {total} всего для чата {chat_id}")
        
        # Соединение глобальное, не закрываем
        
        return jsonify({
            'messages': messages,
            'total': total,
            'limit': limit,
            'offset': offset,
            'has_more': offset + limit < total
        })
    except Exception as e:
        logger.error(f"[API/MESSAGES] Критическая ошибка при получении сообщений для чата {chat_id}: {e}", exc_info=True)
        import traceback
        logger.error(f"[API/MESSAGES] Полный traceback: {traceback.format_exc()}")
        return jsonify({'error': str(e), 'code': 'INTERNAL_ERROR'}), 500


@chats_bp.route('/<int:chat_id>/send', methods=['POST'])
@require_auth
def send_message(chat_id):
    """
    Отправить сообщение в чат
    
    Поддерживает отправку:
    - Только текста
    - Только вложений (attachments)
    - Текста с вложениями
    
    Формат запроса:
    {
        "message": "Текст сообщения" (опционально, если есть attachments),
        "attachments": [{"id": "attachment_id"}, ...] (опционально)
    }
    """
    from database import get_db_connection
    from services.messenger_service import MessengerService
    from avito_api import AvitoAPI
    
    data = request.get_json()
    if not data:
        return jsonify({'error': 'Request body is required'}), 400
    
    message_text = data.get('message', '').strip() if data.get('message') else None
    attachments = data.get('attachments', [])
    
    # Валидация: должно быть либо сообщение, либо вложения
    if not message_text and not attachments:
        return jsonify({'error': 'Either message text or attachments are required'}), 400
    
    if message_text and len(message_text) > 5000:
        return jsonify({'error': 'Message is too long (max 5000 characters)'}), 400
    
    if attachments and not isinstance(attachments, list):
        return jsonify({'error': 'Attachments must be a list'}), 400
    
    # Валидация формата attachments
    if attachments:
        for i, attachment in enumerate(attachments):
            if not isinstance(attachment, dict):
                return jsonify({'error': f'Attachment {i} must be an object'}), 400
            if 'id' not in attachment:
                return jsonify({'error': f'Attachment {i} must have an "id" field'}), 400
    
    conn = get_db_connection()
    
    # Получаем данные чата
    chat = conn.execute('''
        SELECT ac.*, s.client_id, s.client_secret, s.user_id
        FROM avito_chats ac
        JOIN avito_shops s ON ac.shop_id = s.id
        WHERE ac.id = ?
    ''', (chat_id,)).fetchone()
    
    if not chat:
        # Соединение глобальное, не закрываем
        return jsonify({'error': 'Chat not found'}), 404

    # Преобразуем sqlite3.Row в словарь для безопасного доступа
    chat = dict(chat) if not isinstance(chat, dict) else chat

    if not _ensure_manager_can_access_chat(chat):
        # Соединение глобальное, не закрываем
        return jsonify({'error': 'Access denied'}), 403
    
    # Проверяем наличие учетных данных для Авито
    if not chat.get('client_id') or not chat.get('client_secret') or not chat.get('user_id'):
        # Соединение глобальное, не закрываем
        return jsonify({'error': 'Avito credentials are missing for this shop'}), 400

    # Создаём API и сервис
    api = AvitoAPI(
        client_id=chat['client_id'],
        client_secret=chat['client_secret']
    )
    service = MessengerService(conn, api)
    
    # Отправляем - устанавливаем manager_id для всех пользователей, которые отправляют сообщение
    manager_id = session.get('user_id')
    
    # Логируем начало отправки
    logger.info(f"[SEND MESSAGE] ========== НАЧАЛО ОТПРАВКИ СООБЩЕНИЯ ==========")
    logger.info(f"[SEND MESSAGE] Chat ID (БД): {chat_id}")
    logger.info(f"[SEND MESSAGE] Avito Chat ID: {chat.get('chat_id')}")
    logger.info(f"[SEND MESSAGE] User ID: {chat.get('user_id')}")
    logger.info(f"[SEND MESSAGE] Message text: {message_text[:100] if message_text else 'None'}...")
    logger.info(f"[SEND MESSAGE] Has attachments: {bool(attachments)}, count: {len(attachments) if attachments else 0}")
    
    # Если есть attachments, отправляем через API напрямую
    if attachments:
        try:
            logger.info(f"[SEND MESSAGE] Отправка через api.send_message с attachments")
            # Отправляем через API с attachments
            api_result = api.send_message(
                user_id=str(chat.get('user_id')),
                chat_id=str(chat.get('chat_id')),
                message=message_text,
                attachments=attachments
            )
            
            # Сохраняем в БД
            message_for_db = message_text or f"[{len(attachments)} вложений]"
            conn.execute('''
                INSERT INTO avito_messages (chat_id, message_text, message_type, sender_name, manager_id)
                VALUES (?, ?, 'outgoing', 'Магазин', ?)
            ''', (chat_id, message_for_db, manager_id))
            
            # Обновляем чат
            conn.execute('''
                UPDATE avito_chats
                SET last_message = ?, updated_at = CURRENT_TIMESTAMP, unread_count = 0
                WHERE id = ?
            ''', (message_for_db, chat_id))
            
            conn.commit()
            # Соединение глобальное, не закрываем
            
            logger.info(f"[SEND MESSAGE] Сообщение с вложениями отправлено для чата {chat_id}")
            return jsonify({'success': True, 'message_id': api_result.get('id')})
        except Exception as e:
            logger.error(f"[SEND MESSAGE] Ошибка отправки сообщения с вложениями: {e}", exc_info=True)
            # Соединение глобальное, не закрываем
            return jsonify({'error': str(e)}), 500
    else:
        # Обычная отправка текста через сервис
        logger.info(f"[SEND MESSAGE] Отправка через service.send_message (без attachments)")
        success, error_msg = service.send_message(chat_id, message_text, manager_id)
        # Соединение глобальное, не закрываем
        
        if success:
            logger.info(f"[SEND MESSAGE] ✅ Сообщение успешно отправлено через service.send_message для чата {chat_id}")
            return jsonify({'success': True})
        else:
            logger.error(f"[SEND MESSAGE] ❌ Ошибка отправки через service.send_message для чата {chat_id}: {error_msg}")
            return jsonify({'error': error_msg or 'Failed to send message'}), 500


@chats_bp.route('/<int:chat_id>/take', methods=['POST'])
@require_auth
def take_chat(chat_id):
    """Взять чат из пула"""
    from database import get_db_connection
    from services.messenger_service import MessengerService
    
    conn = get_db_connection()
    service = MessengerService(conn, None)
    
    success = service.take_from_pool(chat_id, session['user_id'])
    
    # Соединение глобальное, не закрываем
    
    return jsonify({'success': success})


@chats_bp.route('/<int:chat_id>/return', methods=['POST'])
@require_auth
def return_chat(chat_id):
    """Вернуть чат в пул"""
    from database import get_db_connection
    from services.messenger_service import MessengerService
    
    conn = get_db_connection()
    chat = conn.execute('SELECT assigned_manager_id FROM avito_chats WHERE id = ?', (chat_id,)).fetchone()
    if not chat:
        # Соединение глобальное, не закрываем
        return jsonify({'error': 'Chat not found'}), 404

    if session.get('user_role') == 'manager' and chat['assigned_manager_id'] != session.get('user_id'):
        # Соединение глобальное, не закрываем
        return jsonify({'error': 'Access denied'}), 403

    service = MessengerService(conn, None)
    success = service.return_to_pool(chat_id)
    
    # Соединение глобальное, не закрываем
    
    return jsonify({'success': success})


@chats_bp.route('/<int:chat_id>/block', methods=['POST'])
@require_auth
def block_chat(chat_id):
    """Заблокировать пользователя в чате"""
    from database import get_db_connection
    from services.messenger_service import MessengerService
    from avito_api import AvitoAPI
    
    data = request.get_json() or {}
    block = data.get('block', True)
    
    conn = get_db_connection()
    
    chat = conn.execute('''
        SELECT ac.*, s.client_id, s.client_secret, s.user_id
        FROM avito_chats ac
        JOIN avito_shops s ON ac.shop_id = s.id
        WHERE ac.id = ?
    ''', (chat_id,)).fetchone()
    
    if not chat:
        # Соединение глобальное, не закрываем
        return jsonify({'error': 'Chat not found'}), 404

    if not _ensure_manager_can_access_chat(chat):
        # Соединение глобальное, не закрываем
        return jsonify({'error': 'Access denied'}), 403
    
    if not chat['client_id'] or not chat['client_secret'] or not chat['user_id']:
        # Соединение глобальное, не закрываем
        return jsonify({'error': 'Avito credentials are missing for this shop'}), 400

    api = AvitoAPI(
        client_id=chat['client_id'],
        client_secret=chat['client_secret']
    )
    service = MessengerService(conn, api)
    
    success = service.block_user(
        chat_id=chat_id,
        user_id=chat['user_id'],
        avito_chat_id=chat['chat_id'],
        block=block
    )
    
    # Соединение глобальное, не закрываем
    
    return jsonify({'success': success})


@chats_bp.route('/sync', methods=['POST'])
@require_auth
def sync_chats():
    """Синхронизировать все чаты (асинхронно)"""
    try:
        from tasks import enqueue_sync_all_chats, RQ_AVAILABLE
        
        if RQ_AVAILABLE:
            job = enqueue_sync_all_chats()
            if hasattr(job, 'id'):
                return jsonify({
                    'success': True,
                    'job_id': job.id,
                    'status': 'queued',
                    'message': 'Задача синхронизации поставлена в очередь'
                }), 202
            else:
                # Fallback - синхронное выполнение
                return jsonify({
                    'success': True,
                    'status': 'completed',
                    'result': job
                }), 200
        else:
            # Синхронное выполнение если RQ недоступен
            from database import get_db_connection
            from services.sync_service import SyncService
            
            conn = get_db_connection()
            service = SyncService(conn)
            results = service.sync_all_shops()
            # Соединение глобальное, не закрываем
            
            return jsonify(results)
    except Exception as e:
        logger.error(f"Ошибка синхронизации чатов: {e}", exc_info=True)
        # Fallback на синхронную синхронизацию
        from database import get_db_connection
        from services.sync_service import SyncService
        
        conn = get_db_connection()
        service = SyncService(conn)
        results = service.sync_all_shops()
        # Соединение глобальное, не закрываем
        
        return jsonify(results)


@chats_bp.route('/extract-all-product-urls', methods=['POST'])
@require_auth
@handle_errors
def extract_all_product_urls():
    """
    Принудительно извлечь product_url для всех чатов, у которых его нет
    """
    from database import get_db_connection
    from avito_api import AvitoAPI
    import re
    
    conn = get_db_connection()
    try:
        # Получаем все чаты без product_url (увеличиваем лимит и добавляем пагинацию)
        request_data = request.get_json() or {}
        limit = request_data.get('limit', 500)  # По умолчанию 500 чатов
        offset = request_data.get('offset', 0)
        
        chats_without_url = conn.execute('''
            SELECT ac.id, ac.chat_id, ac.shop_id, s.client_id, s.client_secret, s.user_id, s.shop_url
            FROM avito_chats ac
            JOIN avito_shops s ON ac.shop_id = s.id
            WHERE (ac.product_url IS NULL OR ac.product_url = '')
            AND s.client_id IS NOT NULL AND s.client_secret IS NOT NULL AND s.user_id IS NOT NULL
            ORDER BY ac.id
            LIMIT ? OFFSET ?
        ''', (limit, offset)).fetchall()
        
        # Получаем общее количество чатов без product_url для информации
        total_without_url = conn.execute('''
            SELECT COUNT(*) as cnt
            FROM avito_chats ac
            JOIN avito_shops s ON ac.shop_id = s.id
            WHERE (ac.product_url IS NULL OR ac.product_url = '')
            AND s.client_id IS NOT NULL AND s.client_secret IS NOT NULL AND s.user_id IS NOT NULL
        ''').fetchone()
        total_count = total_without_url['cnt'] if total_without_url else 0
        
        logger.info(f"[EXTRACT ALL] Найдено {len(chats_without_url)} чатов без product_url (всего без URL: {total_count}, offset: {offset}, limit: {limit})")
        
        results = {
            'total': len(chats_without_url),
            'total_without_url': total_count,
            'offset': offset,
            'limit': limit,
            'extracted': 0,
            'errors': 0,
            'has_more': (offset + limit) < total_count
        }
        
        import time
        
        for idx, chat_row in enumerate(chats_without_url):
            chat = dict(chat_row)
            chat_id = chat['id']
            avito_chat_id = chat['chat_id']
            
            # Добавляем небольшую задержку между запросами, чтобы не превысить лимиты Avito API
            # (примерно 1 запрос в секунду)
            if idx > 0 and idx % 10 == 0:
                logger.info(f"[EXTRACT ALL] Прогресс: обработано {idx}/{len(chats_without_url)} чатов...")
                time.sleep(1)  # Задержка каждые 10 чатов
            
            try:
                # ВАЖНО: Avito API не возвращает context.item в списке чатов
                # Поэтому ОБЯЗАТЕЛЬНО вызываем get_chat_by_id для каждого чата
                product_url = None
                if chat['client_id'] and chat['client_secret'] and chat['user_id']:
                    try:
                        api = AvitoAPI(
                            client_id=chat['client_id'],
                            client_secret=chat['client_secret']
                        )
                        # Вызываем get_chat_by_id для получения детальной информации о чате
                        # Это единственный надежный способ получить product_url
                        chat_details = api.get_chat_by_id(
                            user_id=chat['user_id'],
                            chat_id=avito_chat_id
                        )
                        
                        if isinstance(chat_details, dict):
                            # ВАЖНО: Avito API возвращает context.value, а не context.item!
                            # Структура: {"context": {"type": "item", "value": {"id": 123, "url": "..."}}}
                            detail_context = chat_details.get('context', {})
                            if isinstance(detail_context, dict):
                                detail_item = detail_context.get('value') or detail_context.get('item') or detail_context.get('listing') or detail_context.get('ad', {})
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
                                    elif detail_item_id:
                                        item_id_str = str(detail_item_id)
                                        shop_url_part = chat.get('shop_url', '').split('/')[-1] if chat.get('shop_url') else ''
                                        if shop_url_part:
                                            product_url = f"https://www.avito.ru/{shop_url_part}/items/{item_id_str}"
                                        else:
                                            product_url = f"https://www.avito.ru/items/{item_id_str}"
                            
                            # Если не нашли в context, проверяем прямые поля
                            if not product_url:
                                product_url = (chat_details.get('item_url') or 
                                             chat_details.get('listing_url') or 
                                             chat_details.get('ad_url') or
                                             chat_details.get('product_url'))
                    except Exception as api_error:
                        logger.warning(f"[EXTRACT ALL] Ошибка API для чата {chat_id} (avito_chat_id={avito_chat_id}): {api_error}")
                
                # Сохраняем найденный product_url
                if product_url:
                    conn.execute('''
                        UPDATE avito_chats 
                        SET product_url = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                    ''', (product_url, chat_id))
                    results['extracted'] += 1
                    logger.info(f"[EXTRACT ALL] ✅ Для чата {chat_id} найден product_url: {product_url}")
                else:
                    logger.warning(f"[EXTRACT ALL] ⚠️ Для чата {chat_id} product_url не найден")
                    results['errors'] += 1
            except Exception as e:
                logger.error(f"[EXTRACT ALL] Ошибка для чата {chat_id}: {e}", exc_info=True)
                results['errors'] += 1
        
        conn.commit()
        # Соединение глобальное, не закрываем
        
        logger.info(f"[EXTRACT ALL] Завершено: обработано {results['total']}, найдено {results['extracted']}, ошибок {results['errors']}, осталось: {results.get('total_without_url', 0) - offset - len(chats_without_url)}")
        
        response_data = {
            'success': True,
            'results': results
        }
        
        # Если есть еще чаты для обработки, добавляем информацию для следующего запроса
        if results.get('has_more'):
            response_data['next_offset'] = offset + limit
            response_data['message'] = f'Обработано {len(chats_without_url)} из {total_count} чатов. Для продолжения отправьте запрос с offset={offset + limit}'
        
        return jsonify(response_data), 200
    except Exception as e:
        logger.error(f"[EXTRACT ALL] Критическая ошибка: {e}", exc_info=True)
        # Соединение глобальное, не закрываем
        return jsonify({'error': str(e)}), 500


@chats_bp.route('/extract-all-product-urls-internal', methods=['POST'])
@handle_errors
def extract_all_product_urls_internal():
    """
    Извлечь product_url для всех чатов БЕЗ авторизации (для внутреннего использования)
    Требует API ключ в заголовке X-API-Key для защиты (опционально)
    
    ВАЖНО: Обработка каждого чата требует вызова Avito API, что занимает время.
    Используйте небольшой limit (10-50) чтобы избежать таймаута nginx.
    """
    from database import get_db_connection
    from avito_api import AvitoAPI
    import re
    import os
    
    # Проверка API ключа (опционально, можно задать через переменную окружения)
    request_data = request.get_json() or {}
    api_key = request.headers.get('X-API-Key') or request_data.get('api_key')
    expected_api_key = os.environ.get('EXTRACT_API_KEY', '')
    
    # Если API ключ настроен, проверяем его
    if expected_api_key and api_key != expected_api_key:
        logger.warning(f"[EXTRACT ALL INTERNAL] Неверный API ключ")
        return jsonify({'error': 'Invalid API key'}), 401
    
    conn = get_db_connection()
    try:
        # Получаем все чаты без product_url (уменьшаем лимит по умолчанию, чтобы избежать таймаута)
        # Обработка каждого чата требует вызова API Avito, что занимает время
        request_data = request.get_json() or {}
        limit = request_data.get('limit', 50)  # Уменьшено с 500 до 50
        offset = request_data.get('offset', 0)
        
        chats_without_url = conn.execute('''
            SELECT ac.id, ac.chat_id, ac.shop_id, s.client_id, s.client_secret, s.user_id, s.shop_url
            FROM avito_chats ac
            JOIN avito_shops s ON ac.shop_id = s.id
            WHERE (ac.product_url IS NULL OR ac.product_url = '')
            AND s.client_id IS NOT NULL AND s.client_secret IS NOT NULL AND s.user_id IS NOT NULL
            ORDER BY ac.id
            LIMIT ? OFFSET ?
        ''', (limit, offset)).fetchall()
        
        # Получаем общее количество чатов без product_url для информации
        total_without_url = conn.execute('''
            SELECT COUNT(*) as cnt
            FROM avito_chats ac
            JOIN avito_shops s ON ac.shop_id = s.id
            WHERE (ac.product_url IS NULL OR ac.product_url = '')
            AND s.client_id IS NOT NULL AND s.client_secret IS NOT NULL AND s.user_id IS NOT NULL
        ''').fetchone()
        total_count = total_without_url['cnt'] if total_without_url else 0
        
        logger.info(f"[EXTRACT ALL INTERNAL] Найдено {len(chats_without_url)} чатов без product_url (всего без URL: {total_count}, offset: {offset}, limit: {limit})")
        
        results = {
            'total': len(chats_without_url),
            'total_without_url': total_count,
            'offset': offset,
            'limit': limit,
            'extracted': 0,
            'errors': 0,
            'has_more': (offset + limit) < total_count
        }
        
        import time
        
        for idx, chat_row in enumerate(chats_without_url):
            chat = dict(chat_row)
            chat_id = chat['id']
            avito_chat_id = chat['chat_id']
            
            # Добавляем небольшую задержку между запросами (но не слишком долгую, чтобы не превысить таймаут)
            if idx > 0 and idx % 5 == 0:
                logger.info(f"[EXTRACT ALL INTERNAL] Прогресс: обработано {idx}/{len(chats_without_url)} чатов...")
                time.sleep(0.5)  # Уменьшена задержка
            
            try:
                # ВАЖНО: Avito API не возвращает context.item в списке чатов
                # Поэтому ОБЯЗАТЕЛЬНО вызываем get_chat_by_id для каждого чата
                product_url = None
                if chat['client_id'] and chat['client_secret'] and chat['user_id']:
                    try:
                        api = AvitoAPI(
                            client_id=chat['client_id'],
                            client_secret=chat['client_secret']
                        )
                        chat_details = api.get_chat_by_id(
                            user_id=chat['user_id'],
                            chat_id=avito_chat_id
                        )
                        
                        # Детальное логирование для первых 3 чатов
                        if idx < 3:
                            logger.info(f"[EXTRACT ALL INTERNAL] Чат {chat_id}: ответ API тип={type(chat_details)}")
                            if isinstance(chat_details, dict):
                                logger.info(f"[EXTRACT ALL INTERNAL] Чат {chat_id}: ключи в ответе: {list(chat_details.keys())[:20]}")
                                if 'context' in chat_details:
                                    logger.info(f"[EXTRACT ALL INTERNAL] Чат {chat_id}: context тип={type(chat_details.get('context'))}")
                                    if isinstance(chat_details.get('context'), dict):
                                        logger.info(f"[EXTRACT ALL INTERNAL] Чат {chat_id}: context ключи: {list(chat_details.get('context', {}).keys())}")
                        
                        if isinstance(chat_details, dict):
                            detail_context = chat_details.get('context', {})
                            if isinstance(detail_context, dict):
                                # ВАЖНО: Avito API возвращает context.value, а не context.item!
                                # Структура: {"context": {"type": "item", "value": {"id": 123, "url": "..."}}}
                                detail_item = detail_context.get('value') or detail_context.get('item') or detail_context.get('listing') or detail_context.get('ad', {})
                                
                                # Логирование для первых чатов
                                if idx < 3:
                                    logger.info(f"[EXTRACT ALL INTERNAL] Чат {chat_id}: detail_item тип={type(detail_item)}, значение={str(detail_item)[:200] if detail_item else 'None'}")
                                
                                if isinstance(detail_item, dict):
                                    detail_item_id = detail_item.get('id')
                                    detail_url = (detail_item.get('url') or 
                                                 detail_item.get('link') or 
                                                 detail_item.get('href') or
                                                 detail_item.get('uri'))
                                    
                                    if idx < 3:
                                        logger.info(f"[EXTRACT ALL INTERNAL] Чат {chat_id}: detail_item_id={detail_item_id}, detail_url={detail_url}")
                                    
                                    if detail_url:
                                        product_url = detail_url
                                        # URL может быть относительным или абсолютным
                                        if product_url.startswith('/'):
                                            product_url = f"https://www.avito.ru{product_url}"
                                        elif not product_url.startswith('http'):
                                            product_url = f"https://www.avito.ru{product_url}"
                                    elif detail_item_id:
                                        item_id_str = str(detail_item_id)
                                        shop_url_part = chat.get('shop_url', '').split('/')[-1] if chat.get('shop_url') else ''
                                        if shop_url_part:
                                            product_url = f"https://www.avito.ru/{shop_url_part}/items/{item_id_str}"
                                        else:
                                            product_url = f"https://www.avito.ru/items/{item_id_str}"
                            
                            if not product_url:
                                product_url = (chat_details.get('item_url') or 
                                             chat_details.get('listing_url') or 
                                             chat_details.get('ad_url') or
                                             chat_details.get('product_url'))
                                
                                if idx < 3:
                                    logger.info(f"[EXTRACT ALL INTERNAL] Чат {chat_id}: проверка прямых полей, product_url={product_url}")
                    except Exception as api_error:
                        logger.warning(f"[EXTRACT ALL INTERNAL] Ошибка API для чата {chat_id} (avito_chat_id={avito_chat_id}): {api_error}")
                
                # Сохраняем найденный product_url
                if product_url:
                    conn.execute('''
                        UPDATE avito_chats 
                        SET product_url = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                    ''', (product_url, chat_id))
                    results['extracted'] += 1
                    logger.info(f"[EXTRACT ALL INTERNAL] ✅ Для чата {chat_id} найден product_url: {product_url}")
                else:
                    logger.warning(f"[EXTRACT ALL INTERNAL] ⚠️ Для чата {chat_id} product_url не найден")
                    results['errors'] += 1
            except Exception as e:
                logger.error(f"[EXTRACT ALL INTERNAL] Ошибка для чата {chat_id}: {e}", exc_info=True)
                results['errors'] += 1
        
        conn.commit()
        
        logger.info(f"[EXTRACT ALL INTERNAL] Завершено: обработано {results['total']}, найдено {results['extracted']}, ошибок {results['errors']}")
        
        response_data = {
            'success': True,
            'results': results
        }
        
        if results.get('has_more'):
            response_data['next_offset'] = offset + limit
            response_data['message'] = f'Обработано {len(chats_without_url)} из {total_count} чатов. Для продолжения отправьте запрос с offset={offset + limit}'
        
        return jsonify(response_data), 200
    except Exception as e:
        logger.error(f"[EXTRACT ALL INTERNAL] Критическая ошибка: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500


@chats_bp.route('/<int:chat_id>/extract-product-url', methods=['POST'])
@require_auth
@handle_errors
def extract_product_url_from_messages(chat_id):
    """
    Извлечь product_url для чата (сначала через API, затем из сообщений)
    """
    from database import get_db_connection
    from avito_api import AvitoAPI
    import re
    import json
    
    # ВАЖНО: Логируем в app.logger для гарантированного попадания в логи
    import logging
    app_logger = logging.getLogger('app')
    app_logger.info(f"[EXTRACT PRODUCT URL] ========== НАЧАЛО ИЗВЛЕЧЕНИЯ ДЛЯ ЧАТА {chat_id} ==========")
    logger.info(f"[EXTRACT PRODUCT URL] ========== НАЧАЛО ИЗВЛЕЧЕНИЯ ДЛЯ ЧАТА {chat_id} ==========")
    
    conn = get_db_connection()
    try:
        # Получаем информацию о чате
        chat = conn.execute('''
            SELECT ac.id, ac.chat_id, ac.shop_id, ac.product_url as existing_product_url, 
                   s.client_id, s.client_secret, s.user_id, s.shop_url
            FROM avito_chats ac
            JOIN avito_shops s ON ac.shop_id = s.id
            WHERE ac.id = ?
        ''', (chat_id,)).fetchone()
        
        if not chat:
            logger.warning(f"[EXTRACT PRODUCT URL] Чат {chat_id} не найден в базе данных")
            return jsonify({
                'success': False,
                'error': 'Chat not found',
                'message': f'Чат с ID {chat_id} не найден в базе данных'
            }), 404
        
        chat_dict = dict(chat)
        
        # Проверяем, есть ли уже product_url
        if chat_dict.get('existing_product_url'):
            logger.info(f"[EXTRACT PRODUCT URL] У чата {chat_id} уже есть product_url: {chat_dict['existing_product_url']}")
            return jsonify({
                'success': True,
                'product_url': chat_dict['existing_product_url'],
                'source': 'existing'
            }), 200
        
        
        product_url = None
        source = None
        
        # Сначала пробуем через API get_chat_by_id
        if chat_dict.get('client_id') and chat_dict.get('client_secret') and chat_dict.get('user_id'):
            app_logger.info(f"[EXTRACT PRODUCT URL] Пытаемся получить данные через Avito API для чата {chat_id}...")
            logger.info(f"[EXTRACT PRODUCT URL] Пытаемся получить данные через Avito API...")
            try:
                api = AvitoAPI(
                    client_id=chat_dict['client_id'],
                    client_secret=chat_dict['client_secret']
                )
                # ВАЖНО: Согласно документации Avito API, context может приходить в базовом ответе
                # Но для надежности пробуем и с параметрами include_messages/include_users
                # Сначала пробуем без параметров (быстрее)
                chat_details = None
                first_try_error = None
                try:
                    chat_details = api.get_chat_by_id(
                        user_id=chat_dict['user_id'],
                        chat_id=chat_dict['chat_id']
                    )
                except Exception as first_try_error:
                    # Если не получилось, пробуем с параметрами (может помочь получить context)
                    error_str = str(first_try_error) if first_try_error else ''
                    if '404' in error_str:
                        pass  # Пробуем с параметрами без логирования
                        try:
                            chat_details = api.get_chat_by_id(
                                user_id=chat_dict['user_id'],
                                chat_id=chat_dict['chat_id'],
                                include_messages=True,
                                include_users=True
                            )
                        except Exception as second_try_error:
                            # Не пробрасываем исключение, просто продолжаем - chat_details останется None
                            chat_details = None
                    else:
                        # Если ошибка не 404, просто продолжаем - chat_details останется None
                        chat_details = None
                
                app_logger.info(f"[EXTRACT PRODUCT URL] API вернул данные для чата {chat_id}. Тип: {type(chat_details)}")
                logger.info(f"[EXTRACT PRODUCT URL] API вернул данные. Тип: {type(chat_details)}")
                if isinstance(chat_details, dict):
                    all_keys = list(chat_details.keys())
                    app_logger.info(f"[EXTRACT PRODUCT URL] Ключи в ответе API для чата {chat_id}: {all_keys}")
                    logger.info(f"[EXTRACT PRODUCT URL] Ключи в ответе API: {all_keys}")
                    if 'context' in chat_details:
                        context_str = json.dumps(chat_details.get('context'), indent=2, ensure_ascii=False)[:1000]
                        app_logger.info(f"[EXTRACT PRODUCT URL] context найден для чата {chat_id}: {context_str}")
                        logger.info(f"[EXTRACT PRODUCT URL] context найден: {context_str[:500]}")
                    else:
                        app_logger.warning(f"[EXTRACT PRODUCT URL] В ответе API для чата {chat_id} НЕТ поля 'context'! Это означает, что чат не связан с конкретным объявлением.")
                        logger.warning(f"[EXTRACT PRODUCT URL] В ответе API НЕТ поля 'context'!")
                else:
                    app_logger.warning(f"[EXTRACT PRODUCT URL] API вернул не словарь для чата {chat_id}: {chat_details}")
                    logger.warning(f"[EXTRACT PRODUCT URL] API вернул не словарь: {chat_details}")
                
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
                        
                        if isinstance(detail_item, dict):
                            detail_item_id = detail_item.get('id')
                            detail_url = (detail_item.get('url') or 
                                         detail_item.get('link') or 
                                         detail_item.get('href') or
                                         detail_item.get('value') or
                                         detail_item.get('uri'))
                            
                            if detail_url:
                                product_url = detail_url
                                # URL может быть относительным или абсолютным
                                if product_url.startswith('/'):
                                    product_url = f"https://www.avito.ru{product_url}"
                                elif not product_url.startswith('http'):
                                    product_url = f"https://www.avito.ru{product_url}"
                                source = 'api_context_value'
                                app_logger.info(f"[EXTRACT PRODUCT URL] ✅ Найден через API context.value.url для чата {chat_id}: {product_url}")
                                logger.info(f"[EXTRACT PRODUCT URL] ✅ Найден через API context.value.url: {product_url}")
                            elif detail_item_id:
                                item_id_str = str(detail_item_id)
                                shop_url_part = chat_dict.get('shop_url', '').split('/')[-1] if chat_dict.get('shop_url') else ''
                                if shop_url_part:
                                    product_url = f"https://www.avito.ru/{shop_url_part}/items/{item_id_str}"
                                else:
                                    product_url = f"https://www.avito.ru/items/{item_id_str}"
                                source = 'api_context_value_id'
                                logger.info(f"[EXTRACT PRODUCT URL] ✅ Найден через API: {product_url}")
                    
                    # Если не нашли в context, проверяем прямые поля
                    if not product_url:
                        product_url = (chat_details.get('item_url') or 
                                     chat_details.get('listing_url') or 
                                     chat_details.get('ad_url') or
                                     chat_details.get('product_url'))
                        if product_url:
                            source = 'api_direct'
                            logger.info(f"[EXTRACT PRODUCT URL] Найден через API прямые поля: {product_url}")
                    
                    # Стратегия 3: Ищем в любых полях ответа API (глубокий поиск)
                    if not product_url:
                        def find_url_in_dict(d, depth=0, max_depth=3):
                            """Рекурсивный поиск URL в словаре"""
                            if depth > max_depth:
                                return None
                            if isinstance(d, dict):
                                for key, value in d.items():
                                    if isinstance(value, str) and ('avito.ru' in value.lower() or '/items/' in value.lower()):
                                        # Проверяем, похоже ли на URL объявления
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
                            logger.info(f"[EXTRACT PRODUCT URL] Найден через глубокий поиск в API: {product_url}")
            except Exception as api_error:
                # Ошибка уже обработана выше во внутренних блоках try-except
                # Устанавливаем chat_details в None, чтобы продолжить выполнение
                chat_details = None
            
            # УЛУЧШЕНИЕ СПОСОБА 2: Если get_chat_by_id не сработал (404 или None), пробуем get_chats
            # Это часть способа 2, так как используем тот же API, просто другой метод
            if not product_url and chat_dict.get('client_id') and chat_dict.get('client_secret') and chat_dict.get('user_id'):
                try:
                    # Ищем нужный чат в списке
                    target_chat = None
                    avito_chat_id = chat_dict.get('chat_id')
                    
                    # Используем пагинацию: проверяем первые 3 страницы (300 чатов)
                    max_pages = 3
                    limit_per_page = 100
                    
                    for page in range(max_pages):
                        offset = page * limit_per_page
                        
                        # Получаем список чатов через get_chats
                        chats_list = api.get_chats(
                            user_id=chat_dict['user_id'],
                            limit=limit_per_page,
                            offset=offset
                        )
                        
                        if isinstance(chats_list, dict):
                            # Проверяем разные форматы ответа
                            chats_array = (chats_list.get('chats') or 
                                         chats_list.get('items') or 
                                         chats_list.get('data', {}).get('chats') or
                                         [])
                        elif isinstance(chats_list, list):
                            chats_array = chats_list
                        else:
                            chats_array = []
                        
                        # Ищем чат по avito_chat_id
                        for idx, chat_item in enumerate(chats_array):
                            if isinstance(chat_item, dict):
                                # Проверяем разные возможные поля для ID чата
                                item_id = (chat_item.get('id') or 
                                          chat_item.get('chat_id') or
                                          chat_item.get('value', {}).get('id'))
                                
                                # Сравниваем ID (с учетом разных форматов)
                                if item_id and (str(item_id) == str(avito_chat_id) or 
                                               item_id == avito_chat_id):
                                    target_chat = chat_item
                                    logger.info(f"[EXTRACT PRODUCT URL] ✅ Найден чат в списке get_chats (страница {page + 1})")
                                    break
                            
                            if target_chat:
                                break
                        
                        if target_chat:
                            break
                        
                        # Если на этой странице меньше чатов, чем лимит, значит это последняя страница
                        if len(chats_array) < limit_per_page:
                            break
                    
                    if target_chat:
                        # Извлекаем context из найденного чата
                        target_context = target_chat.get('context', {})
                        if isinstance(target_context, dict):
                            # ВАЖНО: Avito API v3 возвращает context.value, а не context.item!
                            context_item = (target_context.get('value') or 
                                           target_context.get('item') or 
                                           target_context.get('listing') or 
                                           target_context.get('ad', {}))
                            
                            if isinstance(context_item, dict):
                                context_item_id = context_item.get('id')
                                context_url = (context_item.get('url') or 
                                             context_item.get('link') or 
                                             context_item.get('href') or
                                             context_item.get('value') or
                                             context_item.get('uri'))
                                
                                if context_url:
                                    product_url = context_url
                                    if product_url.startswith('/'):
                                        product_url = f"https://www.avito.ru{product_url}"
                                    elif not product_url.startswith('http'):
                                        product_url = f"https://www.avito.ru{product_url}"
                                    source = 'api_get_chats_context_value'
                                    logger.info(f"[EXTRACT PRODUCT URL] ✅ Найден через get_chats: {product_url}")
                                elif context_item_id:
                                    item_id_str = str(context_item_id)
                                    shop_url_part = chat_dict.get('shop_url', '').split('/')[-1] if chat_dict.get('shop_url') else ''
                                    if shop_url_part:
                                        product_url = f"https://www.avito.ru/{shop_url_part}/items/{item_id_str}"
                                    else:
                                        product_url = f"https://www.avito.ru/items/{item_id_str}"
                                    source = 'api_get_chats_context_value_id'
                                    logger.info(f"[EXTRACT PRODUCT URL] ✅ Найден через get_chats: {product_url}")
                    else:
                        # Логируем примеры ID из последней проверенной страницы для диагностики
                        if chats_array and len(chats_array) > 0:
                            sample_ids = []
                            for idx, sample_chat in enumerate(chats_array[:3]):  # Первые 3 чата
                                if isinstance(sample_chat, dict):
                                    sample_id = (sample_chat.get('id') or 
                                                sample_chat.get('chat_id') or
                                                sample_chat.get('value', {}).get('id'))
                                    sample_ids.append(str(sample_id))
                            logger.warning(f"[EXTRACT PRODUCT URL] ⚠️ Чат {avito_chat_id} не найден. Примеры ID: {', '.join(sample_ids)}")
                except Exception as get_chats_error:
                    logger.debug(f"[EXTRACT PRODUCT URL] Ошибка get_chats: {get_chats_error}")
        else:
            logger.warning(f"[EXTRACT PRODUCT URL] Нет credentials для API запроса (client_id, client_secret или user_id отсутствуют)")
        
        # Сохраняем найденный product_url
        if product_url:
            conn.execute('''
                UPDATE avito_chats 
                SET product_url = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (product_url, chat_id))
            conn.commit()
            
            logger.info(f"[EXTRACT PRODUCT URL] Для чата {chat_id} найден product_url: {product_url} (источник: {source})")
            # Соединение глобальное, не закрываем
            return jsonify({
                'success': True,
                'product_url': product_url,
                'source': source or 'unknown'
            }), 200
        
        # Соединение глобальное, не закрываем
        logger.warning(f"[EXTRACT PRODUCT URL] ⚠️ Не удалось найти product_url для чата {chat_id}")
        
        # Возвращаем 200 с success: false, а не 404, так как это не ошибка маршрута
        return jsonify({
            'success': False,
            'message': 'Product URL not found',
            'error': 'Product URL not found in chat messages or API',
            'debug_info': {
                'has_api_credentials': bool(chat_dict.get('client_id') and chat_dict.get('client_secret') and chat_dict.get('user_id')),
                'chat_id_in_db': chat_dict.get('chat_id'),
                'shop_id': chat_dict.get('shop_id')
            }
        }), 200
    except Exception as e:
        logger.error(f"[EXTRACT PRODUCT URL] Ошибка для чата {chat_id}: {e}", exc_info=True)
        # Соединение глобальное, не закрываем
        return jsonify({
            'success': False,
            'error': str(e),
            'message': f'Ошибка при извлечении product_url: {str(e)}'
        }), 500

