"""
Listings API - endpoints для работы с объявлениями
"""
from flask import Blueprint, request, jsonify, session
from functools import wraps
import logging

logger = logging.getLogger(__name__)

listings_bp = Blueprint('listings_api', __name__, url_prefix='/api/listings')


def require_auth(f):
    """Декоратор проверки аутентификации"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            return jsonify({'error': 'Not authenticated'}), 401
        return f(*args, **kwargs)
    return decorated_function


@listings_bp.route('/search', methods=['POST'])
@require_auth
def search_listings():
    """Поиск объявлений"""
    from database import get_db_connection
    from services.listings_service import ListingsService
    
    user_id = session.get('user_id')
    user_role = session.get('user_role', 'unknown')
    
    logger.info(f"[LISTINGS API] Начало поиска объявлений. User ID: {user_id}, Role: {user_role}")
    
    data = request.get_json() or {}
    
    query = data.get('query')
    category_id = data.get('category_id', type=int)
    location_id = data.get('location_id', type=int)
    price_min = data.get('price_min', type=float)
    price_max = data.get('price_max', type=float)
    limit = min(int(data.get('limit', 50)), 100)
    save_results = data.get('save_results', False)
    
    logger.info(f"[LISTINGS API] Параметры поиска: query='{query}', category_id={category_id}, "
                f"location_id={location_id}, price_min={price_min}, price_max={price_max}, "
                f"limit={limit}, save_results={save_results}")
    
    try:
        conn = get_db_connection()
        logger.debug(f"[LISTINGS API] Подключение к БД установлено")
        
        service = ListingsService(conn)
        logger.debug(f"[LISTINGS API] ListingsService создан")
        
        # Поиск
        logger.info(f"[LISTINGS API] Вызов service.search_public_listings()...")
        import time
        start_time = time.time()
        
        results = service.search_public_listings(
            query=query,
            category_id=category_id,
            location_id=location_id,
            price_min=price_min,
            price_max=price_max,
            limit=limit
        )
        
        elapsed_time = time.time() - start_time
        listings_count = len(results.get('listings', []))
        total_count = results.get('total', 0)
        
        logger.info(f"[LISTINGS API] Поиск завершен за {elapsed_time:.2f} сек. "
                    f"Найдено объявлений: {listings_count}, total: {total_count}")
        
        if 'error' in results:
            logger.warning(f"[LISTINGS API] Ошибка в результатах поиска: {results.get('error')}")
        
        # Сохраняем результаты если нужно
        saved_count = 0
        param_id = None
        
        if save_results:
            logger.info(f"[LISTINGS API] Сохранение результатов включено. Начало сохранения...")
            
            if results.get('listings'):
                logger.debug(f"[LISTINGS API] Сохранение параметров поиска...")
                # Сохраняем параметры поиска
                param_id = service.save_search_params(data, user_id)
                logger.info(f"[LISTINGS API] Параметры поиска сохранены с ID: {param_id}")
                
                # Сохраняем объявления
                logger.debug(f"[LISTINGS API] Сохранение {listings_count} объявлений...")
                save_start_time = time.time()
                
                for idx, listing in enumerate(results['listings'], 1):
                    listing_id = listing.get('listing_id', 'unknown')
                    listing_title = listing.get('title', '')[:50]  # Первые 50 символов
                    logger.debug(f"[LISTINGS API] Сохранение объявления {idx}/{listings_count}: "
                                f"ID={listing_id}, title='{listing_title}'")
                    
                    saved_id = service.save_listing(listing, param_id)
                    if saved_id:
                        saved_count += 1
                        logger.debug(f"[LISTINGS API] Объявление {idx} сохранено с БД ID: {saved_id}")
                    else:
                        logger.warning(f"[LISTINGS API] Не удалось сохранить объявление {idx} (ID={listing_id})")
                
                save_elapsed = time.time() - save_start_time
                logger.info(f"[LISTINGS API] Сохранение завершено за {save_elapsed:.2f} сек. "
                           f"Сохранено: {saved_count}/{listings_count} объявлений")
            else:
                logger.warning(f"[LISTINGS API] Нет объявлений для сохранения")
        else:
            logger.debug(f"[LISTINGS API] Сохранение результатов отключено")
        
        conn.close()
        logger.debug(f"[LISTINGS API] Подключение к БД закрыто")
        
        response_data = {
            'listings': results.get('listings', []),
            'total': total_count,
            'saved': saved_count,
            'param_id': param_id
        }
        
        logger.info(f"[LISTINGS API] Поиск завершен успешно. Возврат ответа: "
                   f"listings={listings_count}, total={total_count}, saved={saved_count}")
        
        return jsonify(response_data)
        
    except Exception as e:
        logger.error(f"[LISTINGS API] КРИТИЧЕСКАЯ ОШИБКА при поиске объявлений: {str(e)}", exc_info=True)
        return jsonify({
            'error': str(e),
            'listings': [],
            'total': 0,
            'saved': 0
        }), 500


@listings_bp.route('/', methods=['GET'])
@require_auth
def get_listings():
    """Получить сохранённые объявления"""
    from database import get_db_connection
    from services.listings_service import ListingsService
    
    user_id = session.get('user_id')
    user_role = session.get('user_role', 'unknown')
    
    logger.info(f"[LISTINGS API] Получение сохраненных объявлений. User ID: {user_id}, Role: {user_role}")
    
    status = request.args.get('status')
    limit = min(int(request.args.get('limit', 100)), 500)
    offset = int(request.args.get('offset', 0))
    
    logger.info(f"[LISTINGS API] Параметры запроса: status='{status}', limit={limit}, offset={offset}")
    
    # Для менеджеров - только их объявления
    if session.get('user_role') == 'manager':
        manager_id = session['user_id']
        logger.debug(f"[LISTINGS API] Фильтр по менеджеру: manager_id={manager_id}")
    else:
        manager_id = None
        logger.debug(f"[LISTINGS API] Фильтр по менеджеру: нет (роль: {user_role})")
    
    try:
        conn = get_db_connection()
        logger.debug(f"[LISTINGS API] Подключение к БД установлено")
        
        service = ListingsService(conn)
        logger.debug(f"[LISTINGS API] ListingsService создан")
        
        import time
        start_time = time.time()
        
        listings, total = service.get_saved_listings(
            status=status,
            assigned_manager_id=manager_id,
            limit=limit,
            offset=offset
        )
        
        elapsed_time = time.time() - start_time
        listings_count = len(listings)
        
        logger.info(f"[LISTINGS API] Запрос выполнен за {elapsed_time:.2f} сек. "
                   f"Получено объявлений: {listings_count}, всего в БД: {total}")
        
        conn.close()
        logger.debug(f"[LISTINGS API] Подключение к БД закрыто")
        
        response_data = {
            'listings': listings,
            'total': total,
            'limit': limit,
            'offset': offset,
            'has_more': offset + limit < total
        }
        
        logger.info(f"[LISTINGS API] Запрос завершен успешно. has_more={response_data['has_more']}")
        
        return jsonify(response_data)
        
    except Exception as e:
        logger.error(f"[LISTINGS API] ОШИБКА при получении объявлений: {str(e)}", exc_info=True)
        return jsonify({
            'error': str(e),
            'listings': [],
            'total': 0
        }), 500


@listings_bp.route('/<int:listing_id>', methods=['PUT'])
@require_auth
def update_listing(listing_id):
    """Обновить объявление"""
    from database import get_db_connection
    from services.listings_service import ListingsService
    
    data = request.get_json() or {}
    
    status = data.get('status')
    notes = data.get('notes')
    
    if not status:
        return jsonify({'error': 'Status is required'}), 400
    
    conn = get_db_connection()
    service = ListingsService(conn)

    listing = conn.execute('SELECT assigned_manager_id FROM avito_listings WHERE id = ?', (listing_id,)).fetchone()
    if not listing:
        conn.close()
        return jsonify({'error': 'Listing not found'}), 404
    if session.get('user_role') == 'manager' and listing['assigned_manager_id'] not in (None, session.get('user_id')):
        conn.close()
        return jsonify({'error': 'Access denied'}), 403
    
    success = service.update_listing_status(listing_id, status, notes)
    
    conn.close()
    
    return jsonify({'success': success})


@listings_bp.route('/<int:listing_id>', methods=['DELETE'])
@require_auth
def delete_listing(listing_id):
    """Удалить объявление"""
    from database import get_db_connection
    
    conn = get_db_connection()
    
    try:
        listing = conn.execute('SELECT assigned_manager_id FROM avito_listings WHERE id = ?', (listing_id,)).fetchone()
        if not listing:
            conn.close()
            return jsonify({'error': 'Listing not found'}), 404
        if session.get('user_role') == 'manager' and listing['assigned_manager_id'] not in (None, session.get('user_id')):
            conn.close()
            return jsonify({'error': 'Access denied'}), 403

        conn.execute('DELETE FROM avito_listings WHERE id = ?', (listing_id,))
        conn.commit()
        conn.close()
        return jsonify({'success': True})
    except Exception as e:
        conn.close()
        return jsonify({'error': str(e)}), 500

