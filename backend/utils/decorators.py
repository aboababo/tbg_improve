"""
Декораторы для Flask приложения
"""
from functools import wraps
from flask import session, request, jsonify, redirect, render_template
import logging

logger = logging.getLogger(__name__)


def require_auth(f):
    """
    Декоратор для проверки аутентификации пользователя
    
    Проверяет наличие user_id в сессии. Если пользователь не авторизован,
    возвращает ошибку 401 для API запросов или перенаправляет на страницу входа.
    
    Использование:
        @app.route('/api/data')
        @require_auth
        def get_data():
            # Этот код выполнится только если пользователь авторизован
            return jsonify({'data': 'secret'})
    
    Args:
        f: Функция-обработчик маршрута
    
    Returns:
        decorated_function: Обернутая функция с проверкой аутентификации
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # Проверяем наличие user_id в сессии
        if 'user_id' not in session:
            # Диагностика для API запросов
            if request.path.startswith('/api/'):
                cookie_header = request.headers.get('Cookie', 'None')
                session_keys = list(session.keys())
                logger.warning(f"[REQUIRE_AUTH] API запрос без аутентификации: {request.path}")
                logger.warning(f"[REQUIRE_AUTH] Cookie present: {cookie_header != 'None'}, Session keys: {session_keys}")
                if cookie_header != 'None' and len(session_keys) == 0:
                    logger.warning(f"[REQUIRE_AUTH] ⚠️ Cookie отправлен, но сессия не расшифрована! Вероятно, SECRET_KEY изменился.")
                return jsonify({'error': 'Not authenticated', 'message': 'Session expired or invalid. Please login again.'}), 401
            # Иначе перенаправляем на страницу входа
            return redirect('/login')
        # Если пользователь авторизован, выполняем оригинальную функцию
        return f(*args, **kwargs)
    return decorated_function


def require_role(role):
    """
    Декоратор для проверки роли пользователя
    
    Поддерживает иерархию ролей: super_admin > admin > manager
    Супер-админ имеет доступ ко всем функциям админа и менеджера.
    
    Args:
        role (str): Требуемая роль ('manager', 'admin', 'super_admin')
    
    Returns:
        decorator: Декоратор для применения к функции
    """
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            user_role = session.get('user_role')

            # Иерархия ролей
            role_hierarchy = {
                'manager': 1,
                'admin': 2,
                'super_admin': 3
            }

            required_level = role_hierarchy.get(role, 999)
            user_level = role_hierarchy.get(user_role, 0)

            if user_level < required_level:
                if request.is_json:
                    return jsonify({'error': 'Access denied'}), 403
                return redirect('/login')
            return f(*args, **kwargs)
        return decorated_function
    return decorator


def handle_errors(f):
    """
    Декоратор для обработки ошибок в функциях-обработчиках
    
    Перехватывает все исключения, логирует их и возвращает понятный ответ
    пользователю. Предотвращает отображение технических деталей ошибок.
    
    Использование:
        @app.route('/api/data')
        @handle_errors
        def get_data():
            # Если здесь произойдет ошибка, она будет обработана
            return jsonify({'data': data})
    
    Args:
        f: Функция-обработчик маршрута
    
    Returns:
        decorated_function: Обернутая функция с обработкой ошибок
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        try:
            # Пытаемся выполнить функцию
            return f(*args, **kwargs)
        except Exception as error:
            # Логируем ошибку с полным стеком вызовов
            logger.error(f'Error in {f.__name__}: {str(error)}', exc_info=True)
            
            # Возвращаем ошибку в формате, соответствующем типу запроса
            # Если это API запрос (начинается с /api/), всегда возвращаем JSON
            if request.path.startswith('/api/'):
                return jsonify({'error': 'Internal server error', 'message': str(error)}), 500
            # Для HTML запросов возвращаем страницу ошибки
            return render_template('error.html', error=str(error)), 500
    return decorated_function
