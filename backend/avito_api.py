# pyright: reportMissingImports=false, reportMissingModuleSource=false
"""
OsaGaming CRM - Модуль работы с API Авито
==========================================

Интеграция с официальным API Авито для:
- Получения и отправки сообщений в чатах
- Управления объявлениями
- Получения информации о магазинах
- Webhook для получения уведомлений

Документация API: https://developers.avito.ru/api-catalog
Messenger API: https://developers.avito.ru/api-catalog/messenger/documentation#
Автор: OsaGaming Development Team
Версия: 2.1

Доступные методы Messenger API:
================================

ЧАТЫ:
- get_chats() - Получить список чатов (v2)
  * Согласно документации: https://developers.avito.ru/api-catalog/messenger/documentation
  * GET /messenger/v2/accounts/{user_id}/chats
- get_chat_by_id() - Получить информацию о конкретном чате (v3/v2)
  * Согласно документации: https://developers.avito.ru/api-catalog/messenger/documentation#operation/getChatByIdV2
  * Поддерживает опциональные параметры: include_messages, include_users
- get_chats_with_filters() - Получить список чатов с фильтрами (v3/v2)
- get_archived_chats() - Получить список архивных чатов (v3/v2)
- archive_chat() - Архивировать чат (v3/v2)
- unarchive_chat() - Разархивировать чат (v3/v2)

СООБЩЕНИЯ:
- get_chat_messages() - Получить сообщения чата (v3)
  * Согласно документации: https://developers.avito.ru/api-catalog/messenger/documentation
  * GET /messenger/v3/accounts/{user_id}/chats/{chat_id}/messages/
  * Поддерживает пагинацию через limit (1-100) и offset (0-1000)
- send_message() - Отправить сообщение (v3/v2, с поддержкой attachments)
  * Поддерживает отправку только текста, только вложений, или текста с вложениями
  * Согласно документации: https://developers.avito.ru/api-catalog/messenger/documentation#operation/postSendMessage
- get_message_by_id() - Получить информацию о сообщении
- update_message() - Обновить текст сообщения
- delete_message() - Удалить сообщение

МЕДИА (ФОТО, ВИДЕО, ГОЛОСОВЫЕ):
- upload_media() - Загрузить медиа-файл с диска (автоматически использует upload_images для фото)
- upload_media_from_bytes() - Загрузить медиа-файл из байтов
- upload_images() - Загрузить одно или несколько изображений (v1)
  * Согласно документации: https://developers.avito.ru/api-catalog/messenger/documentation
  * POST /messenger/v1/accounts/{user_id}/uploadImages
  * Поддерживает только одиночные изображения (для нескольких нужно несколько запросов)
  * Максимальный размер: 24 МБ, форматы: JPEG, HEIC, GIF, BMP, PNG
- upload_images_from_bytes() - Загрузить одно или несколько изображений из байтов (v1)
- send_message_with_media() - Отправить сообщение с медиа (автоматическая загрузка)
- send_image_message() - Отправить сообщение с изображением (v1)
  * Согласно документации: https://developers.avito.ru/api-catalog/messenger/documentation
  * Загружает изображение и отправляет через v1 endpoint
- send_image_message_direct() - Отправить изображение через специальный v1 endpoint с image_id
  * POST /messenger/v1/accounts/{user_id}/chats/{chat_id}/messages/image
  * Request Body: {"image_id": "string"}
- get_media_info() - Получить информацию о загруженном медиа
- download_media() - Скачать медиа-файл по attachment_id
- get_voice_files() - Получить ссылки на файлы голосовых сообщений по voice_id (v1)
  * Согласно документации: https://developers.avito.ru/api-catalog/messenger/documentation
  * GET /messenger/v1/accounts/{user_id}/getVoiceFiles
  * Query Parameters: voice_ids (Array of strings) - required
- download_voice_file() - Скачать голосовой файл по ID
- get_voice_file_info() - Получить информацию о голосовом файле

СТАТУСЫ И УВЕДОМЛЕНИЯ:
- get_unread_count() - Получить количество непрочитанных в чате (v3/v2)
- get_all_unread_count() - Получить общее количество непрочитанных
- mute_chat() - Включить/выключить уведомления для чата (v3/v2)

БЛОКИРОВКА И ЧЕРНЫЙ СПИСОК:
- block_user() - Заблокировать/разблокировать пользователя (v3/v2)
- add_to_blacklist() - Добавить в черный список (v2)
  * Согласно документации: https://developers.avito.ru/api-catalog/messenger/documentation
  * POST /messenger/v2/accounts/{user_id}/blacklist
  * Request Body: {"users": [Array of objects]}
  * Поддерживает блокировку по телефону или user_id, с опциональной причиной
- get_blacklist() - Получить список черного списка (v3/v2, с пагинацией)
- remove_from_blacklist() - Удалить из черного списка (v3/v2)
  * Поддерживает разблокировку по телефону, user_id или ID записи

ПОЛЬЗОВАТЕЛИ:
- get_chat_users() - Получить список пользователей в чате (v3/v2)

WEBHOOK:
- setup_webhook() - Настроить webhook v2 (устаревший)
- register_webhook_v3() - Зарегистрировать webhook v3 (рекомендуемый)
  * Согласно документации: https://developers.avito.ru/api-catalog/messenger/documentation#operation/postWebhookV3
  * Поддерживает типы событий: 'message', 'chat', 'user'
  * Валидация URL (только HTTPS) и типов событий
- get_webhooks() - Получить список webhook'ов v2
- get_webhook_v3() - Получить информацию о webhook v3
- update_webhook() - Обновить webhook v2 (устаревший)
- update_webhook_v3() - Обновить webhook v3
  * Согласно документации: https://developers.avito.ru/api-catalog/messenger/documentation#operation/postWebhookV3
  * Валидация URL (только HTTPS) и типов событий
- delete_webhook() - Удалить webhook v2
- delete_webhook_v3() - Удалить webhook v3
- verify_webhook_signature() - Проверить подпись webhook

Методы используют различные версии API согласно документации Avito:
- v1: upload_images, send_image_message, get_voice_files
- v2: get_chats, add_to_blacklist
- v3: get_chat_messages
- v3/v2: остальные методы с fallback для обратной совместимости
"""

import requests
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import hashlib
import hmac
from urllib.parse import urlencode

logger = logging.getLogger('app')  # Используем тот же logger, что и в app.py для консистентности


class AvitoAPI:
    """
    Класс для работы с API Авито
    
    Использует OAuth 2.0 авторизацию (Client Credentials flow)
    для получения access_token
    """
    
    BASE_URL = "https://api.avito.ru"
    TOKEN_URL = "https://api.avito.ru/token"
    DEFAULT_TIMEOUT = 30
    
    def __init__(self, client_id: str, client_secret: str, shop_id: Optional[str] = None):
        """
        Инициализация API клиента
        
        Args:
            client_id: Client ID из настроек приложения Авито
            client_secret: Client Secret из настроек приложения Авито
            shop_id: ID магазина (опционально, для некоторых методов)
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.shop_id = shop_id
        self.access_token = None
        self.token_expires_at = None
        self.session = requests.Session()
        # Быстрый флаг наличия корректных ключей
        self._has_credentials = bool(self.client_id and self.client_secret)

    def credentials_present(self) -> bool:
        """Проверка, что заданы все необходимые OAuth ключи."""
        return self._has_credentials
        
    def get_access_token(self) -> str:
        """
        Получение access_token через OAuth 2.0 Client Credentials flow
        
        Returns:
            str: Access token для использования в API запросах
            
        Raises:
            Exception: Если не удалось получить токен
        """
        if not self.client_id or not self.client_secret:
            raise ValueError("Отсутствуют client_id/client_secret для Avito API")

        # Проверяем, не истек ли текущий токен
        if self.access_token and self.token_expires_at:
            if datetime.now() < self.token_expires_at - timedelta(minutes=5):
                return self.access_token
        
        try:
            # Запрос токена
            response = self.session.post(
                self.TOKEN_URL,
                data={
                    'grant_type': 'client_credentials',
                    'client_id': self.client_id,
                    'client_secret': self.client_secret
                },
                headers={
                    'Content-Type': 'application/x-www-form-urlencoded'
                },
                timeout=10
            )
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    self.access_token = data.get('access_token')
                    expires_in = data.get('expires_in', 3600)  # По умолчанию 1 час
                    self.token_expires_at = datetime.now() + timedelta(seconds=expires_in - 300)  # -5 минут для запаса
                    logger.info("Access token получен успешно")
                    return self.access_token
                except ValueError as e:
                    # Если ответ не JSON, возможно это HTML страница ошибки
                    content_preview = response.text[:200] if response.text else str(response.content[:200])
                    error_msg = f"Ошибка получения токена: получен HTML вместо JSON. Статус: {response.status_code}. Начало ответа: {content_preview}"
                    logger.error(error_msg)
                    raise Exception(error_msg)
            else:
                # Пробуем получить JSON ошибки, если не получается - берем текст
                try:
                    if response.content:
                        error_data = response.json()
                        error_text = error_data.get('message', error_data.get('error', response.text[:200]))
                    else:
                        error_text = f"HTTP {response.status_code} - Пустой ответ"
                except ValueError:
                    # Если не JSON, берем текст
                    if response.text:
                        if response.text.strip().startswith('<!'):
                            error_text = f"Получен HTML вместо JSON (возможно страница ошибки). Статус: {response.status_code}"
                        else:
                            error_text = response.text[:200]
                    else:
                        error_text = f"HTTP {response.status_code}"
                
                error_msg = f"Ошибка получения токена: {response.status_code} - {error_text}"
                logger.error(error_msg)
                raise Exception(error_msg)
                
        except Exception as e:
            logger.error(f"Ошибка при получении access_token: {e}")
            raise

    def health_check(self) -> Dict[str, Any]:
        """
        Быстрая проверка валидности OAuth ключей.

        Returns:
            Dict: {status: ok|error, details: str, latency_ms: float}
        """
        start = time.time()
        if not self.credentials_present():
            return {
                'status': 'error',
                'details': 'Отсутствуют client_id/client_secret',
                'latency_ms': 0
            }
        try:
            token = self.get_access_token()
            latency = (time.time() - start) * 1000
            return {
                'status': 'ok' if token else 'error',
                'details': 'token_cached' if self.access_token else 'token_fetched',
                'latency_ms': round(latency, 2),
                'expires_at': self.token_expires_at.isoformat() if self.token_expires_at else None
            }
        except Exception as exc:
            latency = (time.time() - start) * 1000
            return {
                'status': 'error',
                'details': str(exc),
                'latency_ms': round(latency, 2)
            }
    
    def _make_request(self, method: str, endpoint: str, data: Optional[Dict] = None, 
                     params: Optional[Dict] = None, headers: Optional[Dict] = None, 
                     max_retries: int = 3, timeout: Optional[int] = None) -> Dict:
        """
        Выполнение запроса к API Авито с retry логикой
        
        Args:
            method: HTTP метод (GET, POST, PUT, DELETE)
            endpoint: Endpoint API (без базового URL)
            data: Данные для отправки (для POST/PUT)
            params: Query параметры
            headers: Дополнительные заголовки
            max_retries: Максимальное количество попыток (по умолчанию 3)
            
        Returns:
            Dict: Ответ от API
            
        Raises:
            Exception: Если запрос не удался после всех попыток
        """
        # time уже импортирован в начале файла
        
        # Получаем токен если нужно
        token = self.get_access_token()
        
        # Формируем заголовки
        request_headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        if headers:
            request_headers.update(headers)
        
        # Формируем URL
        url = f"{self.BASE_URL}{endpoint}"
        
        # Логируем полный URL для отладки
        if params:
            full_url = f"{url}?{urlencode(params)}"
        else:
            full_url = url
        
        # Определяем тип запроса для детального логирования
        is_listing_request = '/items/' in endpoint and method == 'GET'
        
        if is_listing_request:
            logger.info(f"[AVITO API] Запрос объявления: {method} {full_url}")
        else:
            logger.debug(f"[AVITO API] API запрос: {method} {full_url}")
        
        last_exception = None
        request_start_time = time.time()
        
        for attempt in range(max_retries):
            try:
                attempt_start = time.time()
                
                if is_listing_request and attempt > 0:
                    logger.info(f"[AVITO API] Повторная попытка {attempt + 1}/{max_retries} для получения объявления...")
                
                # Логирование для POST запросов к messages
                if method == 'POST' and '/messages' in endpoint:
                    logger.info(f"[AVITO API] POST {endpoint}")
                    if data:
                        logger.info(f"[AVITO API] Request body: {data}")
                
                response = self.session.request(
                    method=method,
                    url=url,
                    json=data,
                    params=params,
                    headers=request_headers,
                    timeout=timeout or self.DEFAULT_TIMEOUT
                )
                
                attempt_elapsed = time.time() - attempt_start
                
                # Логируем статус ответа
                if is_listing_request:
                    logger.info(f"[AVITO API] Ответ для объявления: статус {response.status_code} за {attempt_elapsed:.2f} сек (попытка {attempt + 1})")
                else:
                    logger.debug(f"[AVITO API] API ответ: {response.status_code} для {endpoint} (попытка {attempt + 1})")
                
                # Обработка успешного ответа
                if response.status_code in [200, 201, 204]:
                    total_elapsed = time.time() - request_start_time
                    
                    if response.content:
                        try:
                            result = response.json()
                            
                            if is_listing_request:
                                content_size = len(response.content)
                                logger.info(f"[AVITO API] Объявление получено успешно: размер ответа {content_size} байт, "
                                           f"всего времени {total_elapsed:.2f} сек")
                            
                            return result
                        except ValueError as e:
                            content_preview = response.text[:200] if response.text else str(response.content[:200])
                            error_msg = f"Ожидался JSON, получен HTML или другой формат. Статус: {response.status_code}. Начало ответа: {content_preview}"
                            
                            if is_listing_request:
                                logger.error(f"[AVITO API] {error_msg}")
                            else:
                                logger.error(error_msg)
                            
                            raise Exception(error_msg)
                    
                    if is_listing_request:
                        logger.warning(f"[AVITO API] Пустой ответ для объявления (статус {response.status_code})")
                    
                    return {}
                
                # Обработка 401 - токен истек
                elif response.status_code == 401:
                    if is_listing_request:
                        logger.warning(f"[AVITO API] Токен истек при запросе объявления, получаем новый...")
                    else:
                        logger.warning("Токен истек, получаем новый...")
                    self.access_token = None
                    token = self.get_access_token()
                    request_headers['Authorization'] = f'Bearer {token}'
                    # Повторяем запрос без задержки
                    continue
                
                # Обработка 403 - Forbidden (нет доступа к ресурсу)
                elif response.status_code == 403:
                    try:
                        error_data = response.json() if response.content else {}
                        error_message = error_data.get('message', error_data.get('error', 'Forbidden - нет доступа'))
                    except:
                        error_message = 'Forbidden - нет доступа к ресурсу'
                    
                    error_msg = f"403 Forbidden: {error_message}"
                    if is_listing_request:
                        logger.error(f"[AVITO API] {error_msg}")
                        logger.error(f"[AVITO API] Возможно, объявление не принадлежит пользователю или нет прав доступа")
                    else:
                        logger.error(error_msg)
                    raise Exception(error_msg)
                
                # Обработка 404 - Not Found (endpoint не найден)
                elif response.status_code == 404:
                    try:
                        error_data = response.json() if response.content else {}
                        error_message = error_data.get('message', error_data.get('error', 'Not Found - endpoint не найден'))
                    except:
                        error_message = 'Not Found - endpoint не найден'
                    
                    error_msg = f"404 Not Found: {error_message}"
                    logger.error(f"[AVITO API] {error_msg}")
                    logger.error(f"[AVITO API] Endpoint: {endpoint}")
                    logger.error(f"[AVITO API] Возможные причины:")
                    logger.error(f"[AVITO API] 1. Endpoint не доступен для данного типа аккаунта")
                    logger.error(f"[AVITO API] 2. Требуется специальный тариф или права доступа")
                    logger.error(f"[AVITO API] 3. Проверьте документацию: https://developers.avito.ru/api-catalog/messenger/documentation")
                    raise Exception(error_msg)
                
                # Обработка 405 - Method Not Allowed (метод не разрешен)
                elif response.status_code == 405:
                    try:
                        error_data = response.json() if response.content else {}
                        error_message = error_data.get('message', error_data.get('error', 'Method Not Allowed'))
                    except:
                        error_message = 'Method Not Allowed - метод не разрешен для этого endpoint'
                    
                    # Детальное логирование для диагностики
                    error_msg = f"405 - HTTP 405: {error_message}"
                    # Выводим в консоль для быстрой диагностики
                    print("\n" + "=" * 80)
                    print("[AVITO API] ========== ОШИБКА 405 ==========")
                    print(f"[AVITO API] {error_msg}")
                    print(f"[AVITO API] Endpoint: {endpoint}")
                    print(f"[AVITO API] Method: {method}")
                    print(f"[AVITO API] Full URL: {url}")
                    print(f"[AVITO API] Request headers: {request_headers}")
                    if data:
                        print(f"[AVITO API] Request body: {data}")
                    print(f"[AVITO API] Response status: {response.status_code}")
                    print(f"[AVITO API] Response headers: {dict(response.headers)}")
                    print(f"[AVITO API] Response body (first 500 chars): {response.text[:500] if response.text else 'empty'}")
                    print("[AVITO API] ==================================")
                    print("=" * 80 + "\n")
                    # Также логируем в logger
                    logger.error(f"[AVITO API] ========== ОШИБКА 405 ==========")
                    logger.error(f"[AVITO API] {error_msg}")
                    logger.error(f"[AVITO API] Endpoint: {endpoint}")
                    logger.error(f"[AVITO API] Method: {method}")
                    logger.error(f"[AVITO API] Full URL: {url}")
                    logger.error(f"[AVITO API] Request headers: {request_headers}")
                    if data:
                        logger.error(f"[AVITO API] Request body: {data}")
                    logger.error(f"[AVITO API] Response status: {response.status_code}")
                    logger.error(f"[AVITO API] Response headers: {dict(response.headers)}")
                    logger.error(f"[AVITO API] Response body (first 500 chars): {response.text[:500] if response.text else 'empty'}")
                    logger.error(f"[AVITO API] ==================================")
                    raise Exception(error_msg)
                
                # Обработка 404 - Not Found (endpoint не найден)
                elif response.status_code == 404:
                    try:
                        error_data = response.json() if response.content else {}
                        error_message = error_data.get('message', error_data.get('error', 'Not Found'))
                    except:
                        error_message = 'Not Found - endpoint не найден'
                    
                    # Детальное логирование для диагностики (особенно для POST /messages)
                    error_msg = f"404 - HTTP 404: {error_message}"
                    # Выводим в консоль для POST запросов к messages
                    if method == 'POST' and '/messages' in endpoint:
                        print("\n" + "=" * 80)
                        print("[AVITO API] ========== ОШИБКА 404 (POST /messages) ==========")
                        print(f"[AVITO API] {error_msg}")
                        print(f"[AVITO API] Endpoint: {endpoint}")
                        print(f"[AVITO API] Method: {method}")
                        print(f"[AVITO API] Full URL: {url}")
                        print(f"[AVITO API] Request headers: {request_headers}")
                        if data:
                            print(f"[AVITO API] Request body: {data}")
                        print(f"[AVITO API] Response status: {response.status_code}")
                        print(f"[AVITO API] Response headers: {dict(response.headers)}")
                        print(f"[AVITO API] Response body (first 500 chars): {response.text[:500] if response.text else 'empty'}")
                        print("[AVITO API] ==================================")
                        print("=" * 80 + "\n")
                    # Также логируем в logger
                    logger.error(f"[AVITO API] ========== ОШИБКА 404 ==========")
                    logger.error(f"[AVITO API] {error_msg}")
                    logger.error(f"[AVITO API] Endpoint: {endpoint}")
                    logger.error(f"[AVITO API] Method: {method}")
                    logger.error(f"[AVITO API] Full URL: {url}")
                    if data:
                        logger.error(f"[AVITO API] Request body: {data}")
                    logger.error(f"[AVITO API] Response status: {response.status_code}")
                    logger.error(f"[AVITO API] Response body (first 500 chars): {response.text[:500] if response.text else 'empty'}")
                    logger.error(f"[AVITO API] ==================================")
                    raise Exception(error_msg)
                
                # Обработка 422 - Unprocessable Entity (ошибка валидации)
                elif response.status_code == 422:
                    try:
                        error_data = response.json() if response.content else {}
                        error_message = error_data.get('message', error_data.get('error', 'Validation error'))
                        # Извлекаем детали ошибки валидации
                        if 'errors' in error_data:
                            validation_errors = error_data.get('errors', {})
                            error_message += f" - {validation_errors}"
                    except:
                        error_message = 'Validation error - ошибка валидации запроса'
                    
                    error_msg = f"422 Unprocessable Entity: {error_message}"
                    if is_listing_request:
                        logger.warning(f"[AVITO API] {error_msg}")
                        logger.warning(f"[AVITO API] Возможно, объявление не принадлежит пользователю или item_id неверный")
                    else:
                        logger.error(error_msg)
                    raise Exception(error_msg)
                
                # Обработка 429 - rate limit
                elif response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    if attempt < max_retries - 1:
                        logger.warning(f"Rate limit достигнут. Ожидание {retry_after} секунд...")
                        time.sleep(retry_after)
                        continue
                    else:
                        error_msg = f"Rate limit достигнут после {max_retries} попыток"
                        logger.error(error_msg)
                        raise Exception(error_msg)
                
                # Обработка 500, 502, 503 - временные ошибки сервера
                # Улучшенная retry логика с exponential backoff и jitter
                elif response.status_code in [500, 502, 503]:
                    if attempt < max_retries - 1:
                        # Exponential backoff: 2^attempt секунд, максимум 30 секунд
                        base_wait = min(2 ** attempt, 30)
                        # Jitter: случайное значение от 0 до 1 секунды для распределения нагрузки
                        jitter = time.time() % 1
                        wait_time = base_wait + jitter
                        logger.warning(f"Временная ошибка сервера {response.status_code}. Повтор через {wait_time:.2f} сек... (попытка {attempt + 1}/{max_retries})")
                        time.sleep(wait_time)
                        continue
                    else:
                        # Последняя попытка не удалась
                        error_msg = f"Временная ошибка сервера {response.status_code} после {max_retries} попыток"
                        logger.error(error_msg)
                        raise Exception(error_msg)
                
                # Обработка 408 - Request Timeout (может быть временной)
                elif response.status_code == 408:
                    if attempt < max_retries - 1:
                        wait_time = min(2 ** attempt, 10) + (time.time() % 1)
                        logger.warning(f"Request Timeout (408). Повтор через {wait_time:.2f} сек... (попытка {attempt + 1}/{max_retries})")
                        time.sleep(wait_time)
                        continue
                
                # Обработка 504 - Gateway Timeout (может быть временной)
                elif response.status_code == 504:
                    if attempt < max_retries - 1:
                        wait_time = min(2 ** attempt, 15) + (time.time() % 1)
                        logger.warning(f"Gateway Timeout (504). Повтор через {wait_time:.2f} сек... (попытка {attempt + 1}/{max_retries})")
                        time.sleep(wait_time)
                        continue
                
                # Обработка других ошибок
                error_message = None
                try:
                    if response.content:
                        error_data = response.json()
                        error_message = error_data.get('message', error_data.get('error', 'Unknown error'))
                except ValueError:
                    if response.text:
                        if response.text.strip().startswith('<!'):
                            error_message = f"Получен HTML вместо JSON. Статус: {response.status_code}"
                        else:
                            error_message = response.text[:500]
                    else:
                        error_message = f"HTTP {response.status_code} - Пустой ответ"
                
                if not error_message:
                    error_message = f"HTTP {response.status_code}"
                
                # Для 4xx ошибок не делаем retry (кроме 429)
                if 400 <= response.status_code < 500 and response.status_code != 429:
                    total_elapsed = time.time() - request_start_time
                    error_msg = f"Ошибка API запроса: {response.status_code} - {error_message}"
                    
                    if is_listing_request:
                        logger.error(f"[AVITO API] ОШИБКА при получении объявления (время {total_elapsed:.2f} сек): {error_msg}")
                        logger.error(f"[AVITO API] Endpoint: {endpoint}, Status: {response.status_code}")
                    else:
                        logger.error(error_msg)
                    
                    raise Exception(error_msg)
                
                # Для других ошибок делаем retry
                if attempt < max_retries - 1:
                    wait_time = (2 ** attempt) + (time.time() % 1)
                    
                    if is_listing_request:
                        logger.warning(f"[AVITO API] Ошибка {response.status_code} при запросе объявления. "
                                     f"Повтор через {wait_time:.2f} сек... (попытка {attempt + 1}/{max_retries})")
                    else:
                        logger.warning(f"Ошибка {response.status_code}. Повтор через {wait_time:.2f} сек...")
                    
                    time.sleep(wait_time)
                    continue
                else:
                    total_elapsed = time.time() - request_start_time
                    error_msg = f"Ошибка API запроса после {max_retries} попыток: {response.status_code} - {error_message}"
                    
                    if is_listing_request:
                        logger.error(f"[AVITO API] ОШИБКА при получении объявления после {max_retries} попыток "
                                   f"(всего времени {total_elapsed:.2f} сек): {error_msg}")
                        logger.error(f"[AVITO API] Endpoint: {endpoint}")
                    else:
                        logger.error(error_msg)
                    
                    raise Exception(error_msg)
                    
            except requests.exceptions.Timeout as e:
                last_exception = e
                total_elapsed = time.time() - request_start_time
                
                if attempt < max_retries - 1:
                    wait_time = (2 ** attempt) + (time.time() % 1)
                    
                    if is_listing_request:
                        logger.warning(f"[AVITO API] Timeout при запросе объявления (время {total_elapsed:.2f} сек). "
                                     f"Повтор через {wait_time:.2f} сек... (попытка {attempt + 1}/{max_retries})")
                    else:
                        logger.warning(f"Timeout при запросе. Повтор через {wait_time:.2f} сек...")
                    
                    time.sleep(wait_time)
                    continue
                else:
                    if is_listing_request:
                        logger.error(f"[AVITO API] Timeout после {max_retries} попыток при запросе объявления "
                                   f"(всего времени {total_elapsed:.2f} сек): {e}")
                        logger.error(f"[AVITO API] Endpoint: {endpoint}")
                    else:
                        logger.error(f"Timeout после {max_retries} попыток: {e}")
                    
                    raise Exception(f"Timeout при запросе к API после {max_retries} попыток: {e}")
            
            except requests.exceptions.RequestException as e:
                last_exception = e
                total_elapsed = time.time() - request_start_time
                
                if attempt < max_retries - 1:
                    wait_time = (2 ** attempt) + (time.time() % 1)
                    
                    if is_listing_request:
                        logger.warning(f"[AVITO API] Ошибка запроса объявления (время {total_elapsed:.2f} сек). "
                                     f"Повтор через {wait_time:.2f} сек... (попытка {attempt + 1}/{max_retries})")
                    else:
                        logger.warning(f"Ошибка запроса. Повтор через {wait_time:.2f} сек...")
                    logger.warning(f"Ошибка сетевого запроса. Повтор через {wait_time:.2f} сек...")
                    time.sleep(wait_time)
                    continue
                else:
                    total_elapsed = time.time() - request_start_time
                    
                    if is_listing_request:
                        logger.error(f"[AVITO API] Ошибка сетевого запроса объявления после {max_retries} попыток "
                                   f"(всего времени {total_elapsed:.2f} сек): {e}")
                        logger.error(f"[AVITO API] Endpoint: {endpoint}")
                    else:
                        logger.error(f"Ошибка сетевого запроса после {max_retries} попыток: {e}")
                    
                    raise
        
        # Если дошли сюда, значит все попытки исчерпаны
        total_elapsed = time.time() - request_start_time
        
        if last_exception:
            if is_listing_request:
                logger.error(f"[AVITO API] Все попытки исчерпаны при запросе объявления "
                           f"(всего времени {total_elapsed:.2f} сек): {last_exception}")
                logger.error(f"[AVITO API] Endpoint: {endpoint}")
            raise last_exception
        
        if is_listing_request:
            logger.error(f"[AVITO API] Не удалось выполнить запрос объявления после {max_retries} попыток "
                       f"(время {total_elapsed:.2f} сек)")
            logger.error(f"[AVITO API] Endpoint: {endpoint}")
        
        raise Exception(f"Не удалось выполнить запрос после {max_retries} попыток")
    
    # ==================== МЕТОДЫ ДЛЯ РАБОТЫ С ЧАТАМИ ====================
    
    def get_chats(self, user_id: Optional[str] = None, limit: int = 50, offset: int = 0) -> Dict:
        """
        Получение списка чатов
        
        Согласно документации Avito Messenger API v2:
        https://developers.avito.ru/api-catalog/messenger/documentation
        
        GET /messenger/v2/accounts/{user_id}/chats
        
        Args:
            user_id: ID пользователя (обязательно)
            limit: Количество чатов (максимум 100, по умолчанию 50)
            offset: Смещение для пагинации (по умолчанию 0)
            
        Returns:
            Dict: Список чатов и метаданные в формате {"chats": [...], "meta": {...}}
        """
        # API Авито требует user_id для получения чатов
        if not user_id:
            raise ValueError("user_id is required for get_chats")
        
        # Согласно документации Avito API v2:
        # GET /messenger/v2/accounts/{user_id}/chats
        # Параметры: limit (максимум 100), offset
        endpoint = f"/messenger/v2/accounts/{user_id}/chats"
        
        # Проверяем валидность параметров согласно документации
        if limit < 1:
            limit = 50  # Значение по умолчанию
        if limit > 100:
            logger.warning(f"limit={limit} превышает максимум 100, устанавливаем 100")
            limit = 100
        if offset < 0:
            offset = 0
        
        params = {
            'limit': limit,
            'offset': offset
        }
        
        # Выполняем запрос согласно документации
        response = self._make_request('GET', endpoint, params=params)
        
        # Логируем структуру ответа
        if isinstance(response, dict):
            logger.debug(f"get_chats response keys: {list(response.keys())}")
            if 'chats' in response:
                chats_count = len(response['chats']) if isinstance(response['chats'], list) else 0
                logger.debug(f"get_chats: found {chats_count} chats")
            if 'meta' in response:
                meta = response['meta']
                logger.debug(f"get_chats meta: {meta}")
            
        return response
    
    def get_chat_messages(self, user_id: str, chat_id: str, 
                         limit: int = 100, 
                         offset: int = 0) -> Dict:
        """
        Получение сообщений чата
        
        Согласно документации Avito Messenger API v3:
        https://developers.avito.ru/api-catalog/messenger/documentation
        
        GET /messenger/v3/accounts/{user_id}/chats/{chat_id}/messages/
        
        Не помечает чат прочитанным. После успешного получения списка сообщений 
        необходимо вызвать метод, который сделает сообщения прочитанными.
        
        Args:
            user_id: ID пользователя Авито (integer <int64>)
            chat_id: ID чата (string)
            limit: Количество сообщений (1-100, по умолчанию 100)
            offset: Сдвиг сообщений (0-1000, по умолчанию 0)
            
        Returns:
            Dict: Список сообщений и метаданные
            
        Документация:
            https://developers.avito.ru/api-catalog/messenger/documentation
        """
        from urllib.parse import quote
        
        # URL-кодируем chat_id, так как он может содержать спецсимволы (например, ~)
        encoded_chat_id = quote(chat_id, safe='')
        
        # Согласно документации Avito API v3:
        # GET /messenger/v3/accounts/{user_id}/chats/{chat_id}/messages/
        # Параметры: limit (1-100, по умолчанию 100), offset (0-1000, по умолчанию 0)
        
        # Проверяем валидность параметров согласно документации
        if limit < 1:
            limit = 100  # Значение по умолчанию согласно документации
        if limit > 100:
            logger.warning(f"limit={limit} превышает максимум 100, устанавливаем 100")
            limit = 100
        if offset < 0:
            offset = 0
        if offset > 1000:
            logger.warning(f"offset={offset} превышает максимум 1000, устанавливаем 1000")
            offset = 1000
        
        # Формируем параметры запроса согласно документации v3
        params = {
            'limit': limit,
            'offset': offset
        }
        
        logger.info(f"Запрос сообщений: user_id={user_id}, chat_id={chat_id}, limit={limit}, offset={offset}")
        
        # Используем только v3 endpoint согласно документации
        # Пробуем разные варианты кодирования chat_id
        endpoints_to_try = [
            f"/messenger/v3/accounts/{user_id}/chats/{encoded_chat_id}/messages/",
            f"/messenger/v3/accounts/{user_id}/chats/{chat_id}/messages/",
        ]
        
        # Если chat_id содержит спецсимволы, пробуем разные варианты кодирования
        if '~' in chat_id or '/' in chat_id or '=' in chat_id:
            safe_encoded = quote(chat_id, safe='~')
            if safe_encoded != chat_id and safe_encoded != encoded_chat_id:
                endpoints_to_try.insert(1, f"/messenger/v3/accounts/{user_id}/chats/{safe_encoded}/messages/")
        
        last_error = None
        for endpoint in endpoints_to_try:
            try:
                logger.debug(f"Пробуем endpoint: {endpoint} с параметрами: {params}")
                result = self._make_request('GET', endpoint, params=params)
                logger.info(f"Успешно получены сообщения с endpoint: {endpoint}")
                return result
            except Exception as e:
                last_error = e
                error_str = str(e)
                # Если это не 404, сразу возвращаем ошибку
                if '404' not in error_str and '400' not in error_str:
                    logger.error(f"Ошибка (не 404/400): {error_str}")
                    raise
                logger.warning(f"404/400 с endpoint {endpoint}, пробуем следующий...")
                continue
        
        # Если все варианты не сработали, возвращаем последнюю ошибку
        if last_error:
            logger.error(f"Все варианты endpoint не сработали. Последняя ошибка: {last_error}")
            raise last_error
        raise Exception("Не удалось получить сообщения: все варианты endpoint вернули ошибку")
    
    def send_message(self, user_id: str, chat_id: str, message: Optional[str] = None, 
                    attachments: Optional[List[Dict]] = None) -> Dict:
        """
        Отправка сообщения в чат
        
        Согласно официальной документации Avito Messenger API v1:
        POST /messenger/v1/accounts/{user_id}/chats/{chat_id}/messages
        Body: {
            "message": {
                "text": "текст сообщения"
            },
            "type": "text"
        }
        
        Документация: https://developers.avito.ru/api-catalog/messenger/documentation#operation/postSendMessage
        
        Args:
            user_id: ID пользователя Авито (integer)
            chat_id: ID чата (string)
            message: Текст сообщения (обязательно для v1 API)
            attachments: Вложения (не поддерживается в v1 API, только текст)
            
        Returns:
            Dict: Информация об отправленном сообщении, включая:
                - id: ID сообщения
                - content: {"text": "текст"}
                - created: timestamp
                - direction: "out"
                - type: "text"
        """
        from urllib.parse import quote
        
        # Явное логирование входа в метод
        print(f"\n[SEND MESSAGE] === НАЧАЛО ОТПРАВКИ ===")
        print(f"[SEND MESSAGE] user_id={user_id}, chat_id={chat_id}, message_length={len(message) if message else 0}")
        logger.info(f"[SEND MESSAGE] === НАЧАЛО ОТПРАВКИ ===")
        logger.info(f"[SEND MESSAGE] user_id={user_id}, chat_id={chat_id}, message_length={len(message) if message else 0}")
        
        # Валидация: должно быть либо сообщение, либо вложения
        if not message and not attachments:
            raise ValueError("Необходимо указать либо текст сообщения, либо вложения (attachments)")
        
        # Преобразуем в строки
        user_id = str(user_id)
        chat_id = str(chat_id)
        
        # Используем ТОЧНО ТОТ ЖЕ подход к кодированию, что и в get_chat_messages (там работает!)
        # В get_chat_messages: encoded_chat_id = quote(chat_id, safe='')
        encoded_chat_id = quote(chat_id, safe='')
        
        # Формируем тело запроса согласно документации Avito API v1
        # POST /messenger/v1/accounts/{user_id}/chats/{chat_id}/messages
        # Формат v1: {"message": {"text": "текст"}, "type": "text"}
        if not message:
            raise ValueError("Для отправки сообщения необходим текст (message)")
        
        # Согласно документации v1 API:
        request_data_v1 = {
            "message": {
                "text": str(message)
            },
            "type": "text"
        }
        
        # Формат для v3/v2 (fallback): {"message": "текст"}
        request_data_v3 = {
            "message": str(message)
        }
        if attachments:
            if not isinstance(attachments, list):
                raise ValueError("attachments должен быть списком")
            validated_attachments = []
            for i, attachment in enumerate(attachments):
                if not isinstance(attachment, dict):
                    raise ValueError(f"Attachment {i} должен быть словарем")
                if 'id' not in attachment:
                    raise ValueError(f"Attachment {i} должен содержать поле 'id'")
                validated_attachments.append({'id': str(attachment['id'])})
            request_data_v3['attachments'] = validated_attachments
        
        # Примечание: v1 API поддерживает только текстовые сообщения
        if attachments:
            logger.warning("v1 API не поддерживает attachments, используем v3/v2 для fallback")
        
        # Согласно документации: POST /messenger/v1/accounts/{user_id}/chats/{chat_id}/messages
        # В логах видно, что GET работает с chat_id БЕЗ кодирования!
        # Используем v1 API (официальная документация для отправки сообщений)
        endpoints_to_try = []
        
        # Приоритет 1: v1 API БЕЗ кодирования (как в успешном GET запросе)
        endpoints_to_try.append(("v1", f"/messenger/v1/accounts/{user_id}/chats/{chat_id}/messages"))
        
        # Приоритет 2: v1 API с кодированием (safe='')
        if encoded_chat_id != chat_id:
            endpoints_to_try.append(("v1", f"/messenger/v1/accounts/{user_id}/chats/{encoded_chat_id}/messages"))
        
        # Приоритет 3: Если есть спецсимволы, пробуем v1 с safe='~'
        if '~' in chat_id or '/' in chat_id or '=' in chat_id:
            safe_encoded = quote(chat_id, safe='~')
            if safe_encoded != chat_id and safe_encoded != encoded_chat_id:
                endpoints_to_try.append(("v1", f"/messenger/v1/accounts/{user_id}/chats/{safe_encoded}/messages"))
        
        # Fallback на v3/v2 (на случай, если v1 не работает)
        endpoints_to_try.append(("v3", f"/messenger/v3/accounts/{user_id}/chats/{chat_id}/messages"))
        if encoded_chat_id != chat_id:
            endpoints_to_try.append(("v3", f"/messenger/v3/accounts/{user_id}/chats/{encoded_chat_id}/messages"))
        
        endpoints_to_try.append(("v2", f"/messenger/v2/accounts/{user_id}/chats/{chat_id}/messages"))
        if encoded_chat_id != chat_id:
            endpoints_to_try.append(("v2", f"/messenger/v2/accounts/{user_id}/chats/{encoded_chat_id}/messages"))
        
        # Пробуем endpoints по очереди
        last_error = None
        last_status_code = None
        
        for idx, (api_version, endpoint) in enumerate(endpoints_to_try, 1):
            try:
                # Выбираем правильный формат данных в зависимости от версии API
                if api_version == "v1":
                    request_data = request_data_v1
                else:
                    request_data = request_data_v3
                
                print(f"[SEND MESSAGE] Попытка {idx}/{len(endpoints_to_try)}: {endpoint} (API {api_version})")
                print(f"[SEND MESSAGE] Request data: {request_data}")
                logger.info(f"[SEND MESSAGE] Попытка {idx}/{len(endpoints_to_try)}: {endpoint} (API {api_version})")
                logger.info(f"[SEND MESSAGE] Data: {request_data}")
                
                result = self._make_request('POST', endpoint, data=request_data)
                
                print(f"[SEND MESSAGE] ✅ УСПЕШНО: {endpoint}")
                logger.info(f"[SEND MESSAGE] ✅ Успешно: {endpoint}")
                return result
                
            except Exception as e:
                last_error = e
                error_str = str(e)
                
                # Извлекаем статус код
                import re
                status_match = re.search(r'(\d{3})', error_str)
                if status_match:
                    last_status_code = int(status_match.group(1))
                
                # Если это 404 или 405, пробуем следующий endpoint
                if '404' in error_str or '405' in error_str:
                    print(f"[SEND MESSAGE] ⚠️ {last_status_code} с {endpoint}, пробуем следующий...")
                    logger.warning(f"[SEND MESSAGE] ⚠️ {last_status_code} с {endpoint}, пробуем следующий...")
                    continue
                # Если это 400 (ошибка валидации), тоже пробуем следующий
                elif '400' in error_str:
                    print(f"[SEND MESSAGE] ⚠️ 400 (валидация) с {endpoint}, пробуем следующий...")
                    logger.warning(f"[SEND MESSAGE] ⚠️ 400 (валидация) с {endpoint}, пробуем следующий...")
                    continue
                # Для других ошибок сразу возвращаем
                else:
                    print(f"[SEND MESSAGE] ❌ Критическая ошибка: {error_str}")
                    logger.error(f"[SEND MESSAGE] ❌ Критическая ошибка: {error_str}")
                    raise
        
        # Если все варианты не сработали
        if last_error:
            error_msg = f"Не удалось отправить сообщение (последний статус: {last_status_code})"
            endpoints_list = [endpoint for _, endpoint in endpoints_to_try]
            print("\n" + "=" * 80)
            print("[SEND MESSAGE] ========== ВСЕ ПОПЫТКИ НЕУДАЧНЫ ==========")
            print(f"[SEND MESSAGE] ❌ {error_msg}")
            print(f"[SEND MESSAGE] Последняя ошибка: {last_error}")
            print(f"[SEND MESSAGE] Последний статус: {last_status_code}")
            print(f"[SEND MESSAGE] Пробовали endpoints: {endpoints_list}")
            print(f"[SEND MESSAGE] v1 Request data: {request_data_v1}")
            print(f"[SEND MESSAGE] v3/v2 Request data: {request_data_v3}")
            print(f"[SEND MESSAGE] user_id: {user_id}, chat_id: {chat_id}, encoded_chat_id: {encoded_chat_id}")
            print("[SEND MESSAGE] ==========================================")
            print("=" * 80 + "\n")
            logger.error(f"[SEND MESSAGE] ========== ВСЕ ПОПЫТКИ НЕУДАЧНЫ ==========")
            logger.error(f"[SEND MESSAGE] ❌ {error_msg}")
            logger.error(f"[SEND MESSAGE] Последняя ошибка: {last_error}")
            logger.error(f"[SEND MESSAGE] Последний статус: {last_status_code}")
            logger.error(f"[SEND MESSAGE] Пробовали endpoints: {endpoints_list}")
            logger.error(f"[SEND MESSAGE] ==========================================")
            raise Exception(error_msg) from last_error
        
        raise Exception("Не удалось отправить сообщение: все варианты endpoint вернули ошибку")
    
    def get_chat_by_id(self, user_id: str, chat_id: str, 
                      include_messages: bool = False,
                      include_users: bool = False) -> Dict:
        """
        Получить информацию о конкретном чате
        
        Согласно документации Avito Messenger API v2/v3:
        https://developers.avito.ru/api-catalog/messenger/documentation#operation/getChatByIdV2
        
        Используем v3 как основной (актуальная версия), v2 как fallback
        
        Args:
            user_id: ID пользователя Авито
            chat_id: ID чата
            include_messages: Включить последние сообщения в ответ (опционально)
            include_users: Включить информацию о пользователях чата (опционально)
            
        Returns:
            Dict: Информация о чате, включая:
                - Основная информация (id, status, created_at, updated_at)
                - Информация о клиенте (имя, телефон, user_id)
                - Информация об объявлении (product_url, item_id)
                - Последние сообщения (если include_messages=True)
                - Пользователи чата (если include_users=True)
                - Статистика (unread_count, last_message и т.д.)
                
        Пример:
            # Базовая информация о чате
            chat_info = api.get_chat_by_id(user_id, chat_id)
            
            # С последними сообщениями
            chat_info = api.get_chat_by_id(user_id, chat_id, include_messages=True)
            
            # С полной информацией
            chat_info = api.get_chat_by_id(user_id, chat_id, include_messages=True, include_users=True)
        """
        from urllib.parse import quote
        
        encoded_chat_id = quote(chat_id, safe='')
        
        # Формируем параметры запроса
        params = {}
        if include_messages:
            params['include_messages'] = 'true'
        if include_users:
            params['include_users'] = 'true'
        
        # Пробуем v3 (актуальная версия), затем v2 (fallback)
        endpoints_to_try = [
            f"/messenger/v3/accounts/{user_id}/chats/{encoded_chat_id}",
            f"/messenger/v3/accounts/{user_id}/chats/{chat_id}",
            f"/messenger/v2/accounts/{user_id}/chats/{encoded_chat_id}",
            f"/messenger/v2/accounts/{user_id}/chats/{chat_id}"
        ]
        
        # ВАЖНО: Если chat_id содержит спецсимволы (например, ~), пробуем разные варианты кодирования
        # Это исправляет проблему с 404 для чатов, которые раньше работали
        if '~' in chat_id or '/' in chat_id or '=' in chat_id:
            safe_encoded = quote(chat_id, safe='~')
            # Добавляем варианты с safe='~' в начало списка (приоритет)
            endpoints_to_try.insert(0, f"/messenger/v3/accounts/{user_id}/chats/{safe_encoded}")
            endpoints_to_try.insert(1, f"/messenger/v2/accounts/{user_id}/chats/{safe_encoded}")
            logger.info(f"[AVITO API] chat_id содержит спецсимволы, добавляем варианты с safe='~': {safe_encoded}")
        
        last_error = None
        for endpoint in endpoints_to_try:
            try:
                result = self._make_request('GET', endpoint, params=params if params else None)
                logger.info(f"Информация о чате успешно получена через: {endpoint}, params: {params}")
                return result
            except Exception as e:
                last_error = e
                error_str = str(e)
                if '404' not in error_str:
                    logger.error(f"Ошибка get_chat_by_id (не 404): {error_str}")
                    raise
                logger.warning(f"404 с {endpoint}, пробуем следующий...")
                continue
        
        if last_error:
            raise last_error
        raise Exception("Не удалось получить информацию о чате: все варианты endpoint вернули 404")
    
    def block_user(self, user_id: str, chat_id: str, block: bool = True) -> bool:
        """
        Заблокировать/разблокировать пользователя в чате
        
        Согласно документации: https://developers.avito.ru/api-catalog/messenger/documentation#
        Используем v3 как основной (актуальная версия), v2 как fallback
        
        Args:
            user_id: ID пользователя Авито (магазина)
            chat_id: ID чата
            block: True для блокировки, False для разблокировки
            
        Returns:
            bool: True если успешно
        """
        from urllib.parse import quote
        
        encoded_chat_id = quote(chat_id, safe='')
        action = 'block' if block else 'unblock'
        
        # Пробуем v3, затем v2
        endpoints_to_try = [
            f"/messenger/v3/accounts/{user_id}/chats/{encoded_chat_id}/{action}",
            f"/messenger/v3/accounts/{user_id}/chats/{chat_id}/{action}",
            f"/messenger/v2/accounts/{user_id}/chats/{encoded_chat_id}/{action}",
            f"/messenger/v2/accounts/{user_id}/chats/{chat_id}/{action}"
        ]
        
        for endpoint in endpoints_to_try:
            try:
                self._make_request('POST', endpoint)
                return True
            except Exception as e:
                if '404' not in str(e):
                    logger.error(f"Ошибка блокировки/разблокировки пользователя: {e}")
                    return False
                continue
        return False

    def add_to_blacklist(self, user_id: str, phone: Optional[str] = None, 
                        user_id_to_block: Optional[str] = None,
                        reason: Optional[str] = None) -> Dict:
        """
        Добавить пользователя в черный список

        Согласно документации Avito Messenger API v2:
        https://developers.avito.ru/api-catalog/messenger/documentation
        
        POST /messenger/v2/accounts/{user_id}/blacklist
        Request Body: {"users": [Array of objects]}

        Args:
            user_id: ID пользователя Авито (магазина) - integer <int64>
            phone: Номер телефона для блокировки (опционально, формат: +7XXXXXXXXXX)
            user_id_to_block: ID пользователя Авито для блокировки (опционально)
            reason: Причина блокировки (опционально)

        Returns:
            Dict: Информация о добавленном в черный список пользователе
            
        Примеры:
            # Блокировка по телефону
            api.add_to_blacklist(user_id, phone='+79991234567', reason='Спам')
            
            # Блокировка по user_id
            api.add_to_blacklist(user_id, user_id_to_block='12345', reason='Нарушение правил')
        """
        # Согласно документации: POST /messenger/v2/accounts/{user_id}/blacklist
        endpoint = f"/messenger/v2/accounts/{user_id}/blacklist"

        # Формируем объект пользователя для добавления в blacklist
        user_data = {}
        if phone:
            # Нормализуем формат телефона (убираем пробелы, дефисы и т.д.)
            normalized_phone = phone.replace(' ', '').replace('-', '').replace('(', '').replace(')', '')
            if not normalized_phone.startswith('+'):
                # Если номер начинается с 7 или 8, добавляем +
                if normalized_phone.startswith('7'):
                    normalized_phone = '+' + normalized_phone
                elif normalized_phone.startswith('8'):
                    normalized_phone = '+7' + normalized_phone[1:]
                else:
                    normalized_phone = '+7' + normalized_phone
            user_data['phone'] = normalized_phone
            
        if user_id_to_block:
            user_data['user_id'] = str(user_id_to_block)
            
        if reason:
            user_data['reason'] = reason

        if not user_data or (not user_data.get('phone') and not user_data.get('user_id')):
            raise ValueError("Необходимо указать либо телефон (phone), либо user_id для блокировки")

        # Согласно документации: Request Body должен содержать массив "users"
        data = {
            'users': [user_data]
        }

        try:
            result = self._make_request('POST', endpoint, data=data)
            logger.info(f"Пользователь успешно добавлен в черный список через: {endpoint}")
            return result if isinstance(result, dict) else {'success': True, 'data': result}
        except Exception as e:
            logger.error(f"Ошибка добавления в черный список: {e}")
            raise
    
    def delete_message(self, user_id: str, message_id: str) -> bool:
        """
        Удалить сообщение
        
        Args:
            user_id: ID пользователя Авито
            message_id: ID сообщения
            
        Returns:
            bool: True если успешно
        """
        endpoint = f"/messenger/v1/accounts/{user_id}/messages/{message_id}"
        
        try:
            self._make_request('DELETE', endpoint)
            return True
        except:
            return False
    
    def mute_chat(self, user_id: str, chat_id: str, mute: bool = True) -> bool:
        """
        Включить/выключить уведомления для чата
        
        Согласно документации: https://developers.avito.ru/api-catalog/messenger/documentation#
        Используем v3 как основной (актуальная версия), v2 как fallback
        
        Args:
            user_id: ID пользователя Авито
            chat_id: ID чата
            mute: True для отключения уведомлений, False для включения
            
        Returns:
            bool: True если успешно
        """
        from urllib.parse import quote
        
        encoded_chat_id = quote(chat_id, safe='')
        action = 'mute' if mute else 'unmute'
        
        # Пробуем v3, затем v2
        endpoints_to_try = [
            f"/messenger/v3/accounts/{user_id}/chats/{encoded_chat_id}/{action}",
            f"/messenger/v3/accounts/{user_id}/chats/{chat_id}/{action}",
            f"/messenger/v2/accounts/{user_id}/chats/{encoded_chat_id}/{action}",
            f"/messenger/v2/accounts/{user_id}/chats/{chat_id}/{action}"
        ]
        
        for endpoint in endpoints_to_try:
            try:
                self._make_request('POST', endpoint)
                return True
            except Exception as e:
                if '404' not in str(e):
                    logger.error(f"Ошибка mute/unmute чата: {e}")
                    return False
                continue
        return False
    
    # ==================== МЕТОДЫ ДЛЯ РАБОТЫ С ОБЪЯВЛЕНИЯМИ ====================
    
    def get_listings(self, user_id: str, limit: int = 50, offset: int = 0, 
                    status: Optional[str] = None) -> Dict:
        """
        Получение списка объявлений
        
        Args:
            user_id: ID пользователя Авито
            limit: Количество объявлений
            offset: Смещение для пагинации
            status: Статус объявлений (active, removed, blocked и т.д.)
            
        Returns:
            Dict: Список объявлений
        """
        endpoint = f"/core/v1/accounts/{user_id}/items"
        
        params = {
            'limit': min(limit, 100),
            'offset': offset
        }
        
        if status:
            params['status'] = status
        
        return self._make_request('GET', endpoint, params=params)
    
    def get_listing(self, user_id: str, item_id: str, params: Optional[Dict] = None) -> Dict:
        """
        Получение информации об объявлении
        
        Согласно документации Avito API:
        GET /core/v1/accounts/{user_id}/items/{item_id}
        
        Args:
            user_id: ID пользователя Авито
            item_id: ID объявления
            params: Дополнительные query параметры (например, fields для указания нужных полей)
            
        Returns:
            Dict: Информация об объявлении
            
        Примечание: 
        - API может возвращать неполные данные по умолчанию
        - Проверьте документацию на наличие параметров для получения полных данных
        - Возможно, нужны дополнительные права доступа или параметры запроса
        """
        logger.info(f"[AVITO API] ========== НАЧАЛО ЗАГРУЗКИ ОБЪЯВЛЕНИЯ ==========")
        logger.info(f"[AVITO API] Получение информации об объявлении: user_id='{user_id}', item_id='{item_id}'")
        
        endpoint = f"/core/v1/accounts/{user_id}/items/{item_id}"
        logger.debug(f"[AVITO API] Endpoint: {endpoint}")
        
        if params:
            logger.info(f"[AVITO API] Query параметры: {params}")
        
        import time
        request_start = time.time()
        
        try:
            result = self._make_request('GET', endpoint, params=params)
            
            request_elapsed = time.time() - request_start
            logger.info(f"[AVITO API] Запрос выполнен за {request_elapsed:.2f} сек.")
            
            # Логируем информацию об объявлении
            if isinstance(result, dict):
                listing_title = result.get('title') or result.get('name') or 'N/A'
                if listing_title != 'N/A' and len(str(listing_title)) > 100:
                    listing_title = str(listing_title)[:100]
                
                # Проверяем разные варианты поля цены
                listing_price = result.get('price')
                if listing_price is None:
                    price_info = result.get('price_info', {})
                    if isinstance(price_info, dict):
                        listing_price = price_info.get('value') or price_info.get('price')
                if listing_price is None:
                    listing_price = 'N/A'
                
                listing_status = result.get('status', 'N/A')
                listing_id = result.get('id', 'N/A')
                
                logger.info(f"[AVITO API] Объявление получено: id={listing_id}, title='{listing_title}', "
                           f"price={listing_price}, status={listing_status}")
                
                # Логируем дополнительные поля
                available_keys = list(result.keys())
                logger.info(f"[AVITO API] Доступные поля в ответе: {available_keys}")
                
                # Детальное логирование структуры цены
                if 'price' in result:
                    logger.info(f"[AVITO API] Поле 'price': {result.get('price')} (тип: {type(result.get('price'))})")
                if 'price_info' in result:
                    logger.info(f"[AVITO API] Поле 'price_info': {result.get('price_info')} (тип: {type(result.get('price_info'))})")
                if 'title' in result:
                    logger.info(f"[AVITO API] Поле 'title': {result.get('title')} (тип: {type(result.get('title'))})")
                if 'name' in result:
                    logger.info(f"[AVITO API] Поле 'name': {result.get('name')} (тип: {type(result.get('name'))})")
                
                # Логируем важные поля
                if 'description' in result:
                    desc_len = len(str(result.get('description', '')))
                    logger.debug(f"[AVITO API] Описание: {desc_len} символов")
                
                if 'images' in result:
                    images_count = len(result.get('images', []))
                    logger.debug(f"[AVITO API] Изображений: {images_count}")
                
                if 'location' in result:
                    location = result.get('location', {})
                    if isinstance(location, dict):
                        logger.debug(f"[AVITO API] Локация: {location}")
                    else:
                        logger.debug(f"[AVITO API] Локация: {location}")
            else:
                logger.warning(f"[AVITO API] Неожиданный формат ответа: {type(result)}")
            
            # Дополнительное логирование структуры ответа
            if isinstance(result, dict):
                logger.info(f"[AVITO API] ========== СТРУКТУРА ОТВЕТА API ==========")
                logger.info(f"[AVITO API] Все ключи: {list(result.keys())}")
                
                # Логируем важные поля детально
                for key in ['title', 'name', 'price', 'price_info', 'description', 'text', 'images', 'photos', 'location', 'category']:
                    if key in result:
                        value = result[key]
                        if isinstance(value, (list, dict)):
                            logger.info(f"[AVITO API] {key}: {type(value).__name__}, длина/размер: {len(value) if hasattr(value, '__len__') else 'N/A'}")
                            if isinstance(value, list) and len(value) > 0:
                                logger.info(f"[AVITO API] {key}[0]: {value[0]}")
                        else:
                            logger.info(f"[AVITO API] {key}: {str(value)[:100] if value else 'None'}")
            
            logger.info(f"[AVITO API] ========== ЗАГРУЗКА ОБЪЯВЛЕНИЯ ЗАВЕРШЕНА УСПЕШНО ==========")
            
            return result
            
        except Exception as e:
            request_elapsed = time.time() - request_start
            logger.error(f"[AVITO API] ОШИБКА при получении объявления за {request_elapsed:.2f} сек: {str(e)}", exc_info=True)
            logger.error(f"[AVITO API] Параметры запроса: user_id='{user_id}', item_id='{item_id}', endpoint='{endpoint}'")
            raise
    
    def update_listing_price(self, user_id: str, item_id: str, price: float) -> Dict:
        """
        Обновление цены объявления
        
        Args:
            user_id: ID пользователя Авито
            item_id: ID объявления
            price: Новая цена
            
        Returns:
            Dict: Результат обновления
        """
        endpoint = f"/core/v1/accounts/{user_id}/items/{item_id}/price"
        
        data = {
            'price': price
        }
        
        return self._make_request('PUT', endpoint, data=data)
    
    # ==================== МЕТОДЫ ДЛЯ WEBHOOK ====================
    
    def verify_webhook_signature(self, signature: str, body: str, secret: str) -> bool:
        """
        Проверка подписи webhook от Авито
        
        Args:
            signature: Подпись из заголовка X-Avito-Signature
            body: Тело запроса (JSON строка)
            secret: Секретный ключ для проверки
            
        Returns:
            bool: True если подпись валидна
        """
        try:
            # Авито использует HMAC-SHA256
            expected_signature = hmac.new(
                secret.encode('utf-8'),
                body.encode('utf-8'),
                hashlib.sha256
            ).hexdigest()
            
            return hmac.compare_digest(signature, expected_signature)
        except Exception as e:
            logger.error(f"Ошибка проверки подписи: {e}")
            return False
    
    def setup_webhook(self, user_id: str, url: str, events: List[str]) -> Dict:
        """
        Настройка webhook для получения уведомлений (v2 - устаревший)
        
        Args:
            user_id: ID пользователя Авито
            url: URL для отправки webhook
            events: Список событий для подписки
            
        Returns:
            Dict: Информация о настроенном webhook
        """
        endpoint = f"/messenger/v2/accounts/{user_id}/webhooks"
        
        data = {
            'url': url,
            'events': events
        }
        
        return self._make_request('POST', endpoint, data=data)
    
    def register_webhook_v3(self, url: str, types: Optional[List[str]] = None) -> Dict:
        """
        Регистрация webhook через Messenger API v3 (рекомендуемый метод)
        
        Согласно документации Avito Messenger API v3:
        https://developers.avito.ru/api-catalog/messenger/documentation#operation/postWebhookV3
        
        Args:
            url: URL для отправки webhook (обязательный параметр)
                 Должен быть валидным HTTPS URL
            types: Список типов событий для подписки (опционально)
                   Возможные значения:
                   - 'message' - события сообщений (новые сообщения, обновления, удаления)
                   - 'chat' - события чатов (создание, обновление, архивация)
                   - 'user' - события пользователей (блокировка, разблокировка)
                   По умолчанию: ['message', 'chat'] если не указано
            
        Returns:
            Dict: Информация о зарегистрированном webhook, включая:
                - 'id' - ID webhook
                - 'url' - URL для отправки webhook
                - 'types' - список типов событий
                - 'active' - статус активности webhook
                - 'created_at' - дата создания
                - 'updated_at' - дата последнего обновления
                
        Примеры:
            # Регистрация webhook с настройками по умолчанию
            webhook = api.register_webhook_v3('https://example.com/webhook')
            
            # Регистрация webhook только для событий сообщений
            webhook = api.register_webhook_v3(
                'https://example.com/webhook',
                types=['message']
            )
            
            # Регистрация webhook для всех типов событий
            webhook = api.register_webhook_v3(
                'https://example.com/webhook',
                types=['message', 'chat', 'user']
            )
            
        Примечания:
            - Webhook v3 является глобальным для всего аккаунта (не привязан к user_id)
            - При регистрации нового webhook старый автоматически удаляется
            - URL должен быть доступен по HTTPS
            - Webhook должен отвечать 200 OK в течение 5 секунд
        """
        if types is None:
            types = ['message', 'chat']
        
        # Валидация URL
        if not url or not isinstance(url, str):
            raise ValueError("URL обязателен и должен быть строкой")
        
        if not url.startswith('https://'):
            raise ValueError("URL должен начинаться с https://")
        
        # Валидация типов событий
        valid_types = ['message', 'chat', 'user']
        if types:
            invalid_types = [t for t in types if t not in valid_types]
            if invalid_types:
                raise ValueError(f"Недопустимые типы событий: {invalid_types}. Допустимые: {valid_types}")
        
        endpoint = "/messenger/v3/webhook"
        
        data = {
            'url': url
        }
        
        if types:
            data['types'] = types
        
        try:
            result = self._make_request('POST', endpoint, data=data)
            logger.info(f"Webhook v3 успешно зарегистрирован: url={url}, types={types}")
            return result
        except Exception as e:
            logger.error(f"Ошибка регистрации webhook v3: {e}")
            raise
    
    # ==================== ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ ====================
    
    def get_user_info(self, user_id: str) -> Dict:
        """
        Получение информации о пользователе
        
        Args:
            user_id: ID пользователя Авито
            
        Returns:
            Dict: Информация о пользователе
        """
        endpoint = f"/core/v1/accounts/{user_id}"
        return self._make_request('GET', endpoint)
    
    def test_connection(self) -> bool:
        """
        Проверка подключения к API
        
        Returns:
            bool: True если подключение работает
        """
        try:
            self.get_access_token()
            return True
        except Exception as e:
            logger.error(f"Ошибка подключения к API: {e}")
            return False
    
    # ==================== МЕТОДЫ ДЛЯ РАБОТЫ С ОБЪЯВЛЕНИЯМИ (Core API) ====================
    
    def get_item_info(self, user_id: str, item_id: str) -> Dict:
        """
        Получение информации по объявлению (Core API)
        
        Args:
            user_id: ID пользователя Авито
            item_id: ID объявления на сайте
            
        Returns:
            Dict: Информация об объявлении (статус, услуги, статистика)
        """
        endpoint = f"/core/v1/accounts/{user_id}/items/{item_id}"
        return self._make_request('GET', endpoint)
    
    def get_items_stats(self, user_id: str, item_ids: List[int]) -> Dict:
        """
        Получение статистики по нескольким объявлениям
        
        Args:
            user_id: ID пользователя Авито
            item_ids: Список идентификаторов объявлений
            
        Returns:
            Dict: Статистика по объявлениям (просмотры, контакты)
        """
        endpoint = f"/core/v1/accounts/{user_id}/stats/items"
        data = {
            'item_ids': item_ids
        }
        return self._make_request('POST', endpoint, data=data)
    
    def get_vas_prices(self, user_id: str, item_ids: List[int]) -> Dict:
        """
        Получение информации о стоимости дополнительных услуг (VAS)
        
        Args:
            user_id: ID пользователя Авито
            item_ids: Список идентификаторов объявлений
            
        Returns:
            Dict: Стоимость услуг (premium, vip, pushup, highlight, xl) для каждого объявления
        """
        endpoint = f"/core/v1/accounts/{user_id}/price/vas"
        data = {
            'item_ids': item_ids
        }
        return self._make_request('POST', endpoint, data=data)
    
    def get_vas_package_prices(self, user_id: str, item_ids: List[int]) -> Dict:
        """
        Получение информации о стоимости пакетов дополнительных услуг
        
        Args:
            user_id: ID пользователя Авито
            item_ids: Список идентификаторов объявлений
            
        Returns:
            Dict: Стоимость пакетов (turbo_sale, quick_sale) для каждого объявления
        """
        endpoint = f"/core/v1/accounts/{user_id}/price/vas_packages"
        data = {
            'item_ids': item_ids
        }
        return self._make_request('POST', endpoint, data=data)
    
    def apply_vas(self, user_id: str, item_id: str, vas_id: str) -> Dict:
        """
        Применение дополнительной услуги к объявлению
        
        Args:
            user_id: ID пользователя Авито
            item_id: ID объявления
            vas_id: Идентификатор услуги (premium, vip, pushup, highlight, xl)
            
        Returns:
            Dict: Информация о примененной услуге и сумма списания
        """
        endpoint = f"/core/v1/accounts/{user_id}/items/{item_id}/vas"
        data = {
            'vas_id': vas_id
        }
        return self._make_request('PUT', endpoint, data=data)
    
    def apply_vas_package(self, user_id: str, item_id: str, package_id: str) -> Dict:
        """
        Применение пакета дополнительных услуг к объявлению
        
        Args:
            user_id: ID пользователя Авито
            item_id: ID объявления
            package_id: Идентификатор пакета (turbo_sale, quick_sale)
            
        Returns:
            Dict: Информация о примененном пакете и сумма списания
        """
        endpoint = f"/core/v1/accounts/{user_id}/items/{item_id}/vas_packages"
        data = {
            'package_id': package_id
        }
        return self._make_request('PUT', endpoint, data=data)
    
    # ==================== МЕТОДЫ ДЛЯ АВТОЗАГРУЗКИ (Autoload API) ====================
    
    def get_autoload_reports(self, user_id: str, per_page: int = 25, page: int = 1) -> Dict:
        """
        Получение списка отчетов об автозагрузке
        
        Args:
            user_id: ID пользователя Авито
            per_page: Количество отчетов на страницу
            page: Номер страницы
            
        Returns:
            Dict: Список отчетов об автозагрузке
        """
        endpoint = f"/autoload/v1/accounts/{user_id}/reports"
        params = {
            'per_page': per_page,
            'page': page
        }
        return self._make_request('GET', endpoint, params=params)
    
    def get_autoload_report(self, user_id: str, report_id: int) -> Dict:
        """
        Получение данных отчета по ID
        
        Args:
            user_id: ID пользователя Авито
            report_id: ID отчета
            
        Returns:
            Dict: Данные отчета об автозагрузке
        """
        endpoint = f"/autoload/v1/accounts/{user_id}/reports/{report_id}"
        return self._make_request('GET', endpoint)
    
    def get_last_autoload_report(self, user_id: str) -> Dict:
        """
        Получение данных последнего актуального отчета
        
        Args:
            user_id: ID пользователя Авито
            
        Returns:
            Dict: Данные последнего отчета
        """
        endpoint = f"/autoload/v1/accounts/{user_id}/reports/last_report"
        return self._make_request('GET', endpoint)
    
    def get_autoload_item_info(self, user_id: str, ad_id: str) -> Dict:
        """
        Получение информации о выгрузке объявления
        
        Args:
            user_id: ID пользователя Авито
            ad_id: Идентификатор объявления из XML
            
        Returns:
            Dict: Информация о статусе выгрузки объявления
        """
        endpoint = f"/autoload/v1/accounts/{user_id}/items/{ad_id}"
        return self._make_request('GET', endpoint)
    
    # ==================== МЕТОДЫ ДЛЯ СТАТИСТИКИ (Stats API) ====================
    
    def get_listing_statistics(self, user_id: str, item_id: str, 
                              date_from: str, date_to: str) -> Dict:
        """
        Получение статистики по объявлению
        
        Args:
            user_id: ID пользователя Авито
            item_id: ID объявления
            date_from: Дата начала периода (YYYY-MM-DD)
            date_to: Дата окончания периода (YYYY-MM-DD)
            
        Returns:
            Dict: Статистика объявления (просмотры, звонки, избранное и т.д.)
        """
        endpoint = f"/core/v1/accounts/{user_id}/items/{item_id}/statistics"
        
        params = {
            'dateFrom': date_from,
            'dateTo': date_to
        }
        
        return self._make_request('GET', endpoint, params=params)
    
    def get_account_statistics(self, user_id: str, date_from: str, date_to: str) -> Dict:
        """
        Получение общей статистики аккаунта
        
        Args:
            user_id: ID пользователя Авито
            date_from: Дата начала периода (YYYY-MM-DD)
            date_to: Дата окончания периода (YYYY-MM-DD)
            
        Returns:
            Dict: Общая статистика аккаунта
        """
        endpoint = f"/core/v1/accounts/{user_id}/statistics"
        
        params = {
            'dateFrom': date_from,
            'dateTo': date_to
        }
        
        return self._make_request('GET', endpoint, params=params)
    
    # ==================== РАСШИРЕННЫЕ МЕТОДЫ MESSENGER API ====================
    
    def archive_chat(self, user_id: str, chat_id: str) -> bool:
        """
        Архивировать чат
        
        Согласно документации: https://developers.avito.ru/api-catalog/messenger/documentation#
        Используем v3 как основной (актуальная версия), v2 как fallback
        
        Args:
            user_id: ID пользователя Авито
            chat_id: ID чата
            
        Returns:
            bool: True если успешно
        """
        from urllib.parse import quote
        
        encoded_chat_id = quote(chat_id, safe='')
        
        # Пробуем v3, затем v2
        endpoints_to_try = [
            f"/messenger/v3/accounts/{user_id}/chats/{encoded_chat_id}/archive",
            f"/messenger/v3/accounts/{user_id}/chats/{chat_id}/archive",
            f"/messenger/v2/accounts/{user_id}/chats/{encoded_chat_id}/archive",
            f"/messenger/v2/accounts/{user_id}/chats/{chat_id}/archive"
        ]
        
        for endpoint in endpoints_to_try:
            try:
                self._make_request('POST', endpoint)
                return True
            except Exception as e:
                if '404' not in str(e):
                    logger.error(f"Ошибка архивирования чата: {e}")
                    return False
                continue
        return False
    
    def unarchive_chat(self, user_id: str, chat_id: str) -> bool:
        """
        Разархивировать чат
        
        Согласно документации: https://developers.avito.ru/api-catalog/messenger/documentation#
        Используем v3 как основной (актуальная версия), v2 как fallback
        
        Args:
            user_id: ID пользователя Авито
            chat_id: ID чата
            
        Returns:
            bool: True если успешно
        """
        from urllib.parse import quote
        
        encoded_chat_id = quote(chat_id, safe='')
        
        # Пробуем v3, затем v2
        endpoints_to_try = [
            f"/messenger/v3/accounts/{user_id}/chats/{encoded_chat_id}/unarchive",
            f"/messenger/v3/accounts/{user_id}/chats/{chat_id}/unarchive",
            f"/messenger/v2/accounts/{user_id}/chats/{encoded_chat_id}/unarchive",
            f"/messenger/v2/accounts/{user_id}/chats/{chat_id}/unarchive"
        ]
        
        for endpoint in endpoints_to_try:
            try:
                self._make_request('POST', endpoint)
                return True
            except Exception as e:
                if '404' not in str(e):
                    logger.error(f"Ошибка разархивирования чата: {e}")
                    return False
                continue
        return False
    
    def get_archived_chats(self, user_id: str, limit: int = 50, offset: int = 0) -> Dict:
        """
        Получение списка архивных чатов
        
        Согласно документации: https://developers.avito.ru/api-catalog/messenger/documentation#
        Используем v3 как основной (актуальная версия), v2 как fallback
        
        Args:
            user_id: ID пользователя Авито
            limit: Количество чатов
            offset: Смещение для пагинации
            
        Returns:
            Dict: Список архивных чатов
        """
        params = {
            'limit': min(limit, 100),
            'offset': offset
        }
        
        # Пробуем v3, затем v2
        endpoints_to_try = [
            f"/messenger/v3/accounts/{user_id}/chats/archived",
            f"/messenger/v2/accounts/{user_id}/chats/archived"
        ]
        
        for endpoint in endpoints_to_try:
            try:
                return self._make_request('GET', endpoint, params=params)
            except Exception as e:
                if '404' not in str(e):
                    logger.error(f"Ошибка получения архивных чатов: {e}")
                    raise
                continue
        
        return {}
    
    def get_unread_count(self, user_id: str, chat_id: str) -> int:
        """
        Получение количества непрочитанных сообщений в чате
        
        Согласно документации: https://developers.avito.ru/api-catalog/messenger/documentation#
        Используем v3 как основной (актуальная версия), v2 как fallback
        
        Args:
            user_id: ID пользователя Авито
            chat_id: ID чата
            
        Returns:
            int: Количество непрочитанных сообщений
        """
        from urllib.parse import quote
        
        encoded_chat_id = quote(chat_id, safe='')
        
        # Пробуем v3, затем v2
        endpoints_to_try = [
            f"/messenger/v3/accounts/{user_id}/chats/{encoded_chat_id}/unread",
            f"/messenger/v3/accounts/{user_id}/chats/{chat_id}/unread",
            f"/messenger/v2/accounts/{user_id}/chats/{encoded_chat_id}/unread",
            f"/messenger/v2/accounts/{user_id}/chats/{chat_id}/unread"
        ]
        
        for endpoint in endpoints_to_try:
            try:
                result = self._make_request('GET', endpoint)
                return result.get('count', 0)
            except Exception as e:
                if '404' not in str(e):
                    logger.error(f"Ошибка получения количества непрочитанных: {e}")
                    return 0
                continue
        return 0
    
    # ==================== ДОПОЛНИТЕЛЬНЫЕ МЕТОДЫ MESSENGER API ====================
    
    def get_blacklist(self, user_id: str, limit: int = 100, offset: int = 0) -> Dict:
        """
        Получить список пользователей в черном списке
        
        Согласно документации Avito Messenger API v2/v3
        
        Args:
            user_id: ID пользователя Авито
            limit: Количество записей (максимум 100)
            offset: Смещение для пагинации
            
        Returns:
            Dict: Список заблокированных пользователей с метаданными
            
        Пример:
            blacklist = api.get_blacklist(user_id)
            for entry in blacklist.get('items', []):
                phone = entry.get('phone')
                user_id_blocked = entry.get('user_id')
                reason = entry.get('reason')
        """
        # Пробуем v3, затем v2
        endpoints_to_try = [
            f"/messenger/v3/accounts/{user_id}/blacklist",
            f"/messenger/v2/accounts/{user_id}/blacklist",
        ]
        
        params = {
            'limit': min(limit, 100),
            'offset': offset
        }
        
        last_error = None
        for endpoint in endpoints_to_try:
            try:
                result = self._make_request('GET', endpoint, params=params)
                logger.info(f"Черный список успешно получен через: {endpoint}")
                return result
            except Exception as e:
                last_error = e
                error_str = str(e)
                if '404' not in error_str:
                    logger.error(f"Ошибка получения черного списка (не 404): {error_str}")
                    raise
                logger.warning(f"404 с {endpoint}, пробуем следующий...")
                continue
        
        if last_error:
            raise last_error
        # Если все варианты не сработали, возвращаем пустой список
        logger.warning(f"Не удалось получить черный список для user_id {user_id}, все endpoints вернули 404")
        return {'items': [], 'total': 0}
    
    def remove_from_blacklist(self, user_id: str, phone: Optional[str] = None, 
                              user_id_to_unblock: Optional[str] = None,
                              blacklist_entry_id: Optional[str] = None) -> bool:
        """
        Удалить пользователя из черного списка
        
        Согласно документации Avito Messenger API v2/v3
        
        Args:
            user_id: ID пользователя Авито (магазина)
            phone: Номер телефона для разблокировки (опционально, формат: +7XXXXXXXXXX)
            user_id_to_unblock: ID пользователя Авито для разблокировки (опционально)
            blacklist_entry_id: ID записи в черном списке (опционально, если известен)
            
        Returns:
            bool: True если успешно
            
        Примеры:
            # Разблокировка по телефону
            api.remove_from_blacklist(user_id, phone='+79991234567')
            
            # Разблокировка по user_id
            api.remove_from_blacklist(user_id, user_id_to_unblock='12345')
            
            # Разблокировка по ID записи
            api.remove_from_blacklist(user_id, blacklist_entry_id='entry_123')
        """
        # Пробуем v3, затем v2
        endpoints_to_try = [
            f"/messenger/v3/accounts/{user_id}/blacklist",
            f"/messenger/v2/accounts/{user_id}/blacklist",
        ]
        
        data = {}
        if phone:
            # Нормализуем формат телефона
            normalized_phone = phone.replace(' ', '').replace('-', '').replace('(', '').replace(')', '')
            if not normalized_phone.startswith('+'):
                if normalized_phone.startswith('7'):
                    normalized_phone = '+' + normalized_phone
                elif normalized_phone.startswith('8'):
                    normalized_phone = '+7' + normalized_phone[1:]
                else:
                    normalized_phone = '+7' + normalized_phone
            data['phone'] = normalized_phone
            
        if user_id_to_unblock:
            data['user_id'] = str(user_id_to_unblock)
            
        if blacklist_entry_id:
            data['id'] = str(blacklist_entry_id)
        
        if not data:
            raise ValueError("Необходимо указать либо телефон (phone), либо user_id, либо blacklist_entry_id для разблокировки")
        
        last_error = None
        for endpoint in endpoints_to_try:
            try:
                # Если есть ID записи, можно использовать DELETE с ID в пути
                if blacklist_entry_id and not phone and not user_id_to_unblock:
                    delete_endpoint = f"{endpoint}/{blacklist_entry_id}"
                    self._make_request('DELETE', delete_endpoint)
                else:
                    self._make_request('DELETE', endpoint, data=data)
                logger.info(f"Пользователь успешно удален из черного списка через: {endpoint}")
                return True
            except Exception as e:
                last_error = e
                error_str = str(e)
                if '404' not in error_str:
                    logger.error(f"Ошибка удаления из черного списка (не 404): {error_str}")
                    raise
                logger.warning(f"404 с {endpoint}, пробуем следующий...")
                continue
        
        if last_error:
            raise last_error
        raise Exception("Не удалось удалить из черного списка: все варианты endpoint вернули ошибку")
    
    def get_webhooks(self, user_id: str) -> Dict:
        """
        Получить список настроенных webhook'ов
        
        Args:
            user_id: ID пользователя Авито
            
        Returns:
            Dict: Список webhook'ов
        """
        endpoint = f"/messenger/v2/accounts/{user_id}/webhooks"
        
        try:
            return self._make_request('GET', endpoint)
        except Exception as e:
            logger.error(f"Ошибка получения webhook'ов: {e}")
            return {}
    
    def delete_webhook(self, user_id: str, webhook_id: str) -> bool:
        """
        Удалить webhook
        
        Args:
            user_id: ID пользователя Авито
            webhook_id: ID webhook'а
            
        Returns:
            bool: True если успешно
        """
        endpoint = f"/messenger/v2/accounts/{user_id}/webhooks/{webhook_id}"
        
        try:
            self._make_request('DELETE', endpoint)
            return True
        except Exception as e:
            logger.error(f"Ошибка удаления webhook: {e}")
            return False
    
    def update_webhook(self, user_id: str, webhook_id: str, url: str, 
                      events: List[str]) -> Dict:
        """
        Обновить настройки webhook
        
        Args:
            user_id: ID пользователя Авито
            webhook_id: ID webhook'а
            url: Новый URL для отправки webhook
            events: Список событий для подписки
            
        Returns:
            Dict: Информация об обновленном webhook
        """
        endpoint = f"/messenger/v2/accounts/{user_id}/webhooks/{webhook_id}"
        
        data = {
            'url': url,
            'events': events
        }
        
        try:
            return self._make_request('PUT', endpoint, data=data)
        except Exception as e:
            logger.error(f"Ошибка обновления webhook: {e}")
            raise
    
    def get_webhook_v3(self) -> Dict:
        """
        Получить информацию о зарегистрированном webhook v3
        
        Returns:
            Dict: Информация о webhook
        """
        endpoint = "/messenger/v3/webhook"
        
        try:
            return self._make_request('GET', endpoint)
        except Exception as e:
            logger.error(f"Ошибка получения webhook v3: {e}")
            return {}
    
    def delete_webhook_v3(self) -> bool:
        """
        Удалить зарегистрированный webhook v3
        
        Returns:
            bool: True если успешно
        """
        endpoint = "/messenger/v3/webhook"
        
        try:
            self._make_request('DELETE', endpoint)
            return True
        except Exception as e:
            logger.error(f"Ошибка удаления webhook v3: {e}")
            return False
    
    def update_webhook_v3(self, url: str, types: Optional[List[str]] = None) -> Dict:
        """
        Обновить webhook v3
        
        Согласно документации Avito Messenger API v3:
        https://developers.avito.ru/api-catalog/messenger/documentation#operation/postWebhookV3
        
        Args:
            url: URL для отправки webhook (обязательный параметр)
                 Должен быть валидным HTTPS URL
            types: Список типов событий для подписки (опционально)
                   Возможные значения:
                   - 'message' - события сообщений
                   - 'chat' - события чатов
                   - 'user' - события пользователей
                   По умолчанию: ['message', 'chat'] если не указано
            
        Returns:
            Dict: Информация об обновленном webhook
            
        Примеры:
            # Обновление только URL
            webhook = api.update_webhook_v3('https://new-example.com/webhook')
            
            # Обновление URL и типов событий
            webhook = api.update_webhook_v3(
                'https://new-example.com/webhook',
                types=['message', 'chat', 'user']
            )
        """
        if types is None:
            types = ['message', 'chat']
        
        # Валидация URL
        if not url or not isinstance(url, str):
            raise ValueError("URL обязателен и должен быть строкой")
        
        if not url.startswith('https://'):
            raise ValueError("URL должен начинаться с https://")
        
        # Валидация типов событий
        valid_types = ['message', 'chat', 'user']
        if types:
            invalid_types = [t for t in types if t not in valid_types]
            if invalid_types:
                raise ValueError(f"Недопустимые типы событий: {invalid_types}. Допустимые: {valid_types}")
        
        endpoint = "/messenger/v3/webhook"
        
        data = {
            'url': url
        }
        
        if types:
            data['types'] = types
        
        try:
            result = self._make_request('PUT', endpoint, data=data)
            logger.info(f"Webhook v3 успешно обновлен: url={url}, types={types}")
            return result
        except Exception as e:
            logger.error(f"Ошибка обновления webhook v3: {e}")
            raise
    
    def get_chat_users(self, user_id: str, chat_id: str) -> Dict:
        """
        Получить список пользователей в чате
        
        Args:
            user_id: ID пользователя Авито
            chat_id: ID чата
            
        Returns:
            Dict: Информация о пользователях чата
        """
        from urllib.parse import quote
        
        encoded_chat_id = quote(chat_id, safe='')
        
        # Пробуем v3, затем v2
        endpoints_to_try = [
            f"/messenger/v3/accounts/{user_id}/chats/{encoded_chat_id}/users",
            f"/messenger/v3/accounts/{user_id}/chats/{chat_id}/users",
            f"/messenger/v2/accounts/{user_id}/chats/{encoded_chat_id}/users",
            f"/messenger/v2/accounts/{user_id}/chats/{chat_id}/users"
        ]
        
        for endpoint in endpoints_to_try:
            try:
                return self._make_request('GET', endpoint)
            except Exception as e:
                if '404' not in str(e):
                    logger.error(f"Ошибка получения пользователей чата: {e}")
                    raise
                continue
        
        return {}
    
    def get_message_by_id(self, user_id: str, message_id: str) -> Dict:
        """
        Получить информацию о конкретном сообщении
        
        Args:
            user_id: ID пользователя Авито
            message_id: ID сообщения
            
        Returns:
            Dict: Информация о сообщении
        """
        endpoint = f"/messenger/v1/accounts/{user_id}/messages/{message_id}"
        
        try:
            return self._make_request('GET', endpoint)
        except Exception as e:
            logger.error(f"Ошибка получения сообщения: {e}")
            raise
    
    def update_message(self, user_id: str, message_id: str, text: str) -> Dict:
        """
        Обновить текст сообщения (если поддерживается API)
        
        Args:
            user_id: ID пользователя Авито
            message_id: ID сообщения
            text: Новый текст сообщения
            
        Returns:
            Dict: Информация об обновленном сообщении
        """
        endpoint = f"/messenger/v1/accounts/{user_id}/messages/{message_id}"
        
        data = {
            'text': text
        }
        
        try:
            return self._make_request('PUT', endpoint, data=data)
        except Exception as e:
            logger.error(f"Ошибка обновления сообщения: {e}")
            raise
    
    def get_chats_with_filters(self, user_id: str, limit: int = 50, offset: int = 0,
                               unread_only: bool = False, archived: bool = False) -> Dict:
        """
        Получить список чатов с фильтрами
        
        Args:
            user_id: ID пользователя Авито
            limit: Количество чатов
            offset: Смещение для пагинации
            unread_only: Только непрочитанные чаты
            archived: Только архивные чаты
            
        Returns:
            Dict: Список чатов
        """
        # Используем соответствующие endpoints в зависимости от фильтров
        if archived:
            return self.get_archived_chats(user_id, limit, offset)
        
        endpoint = f"/messenger/v3/accounts/{user_id}/chats"
        
        params = {
            'limit': min(limit, 100),
            'offset': offset
        }
        
        if unread_only:
            params['unread_only'] = True
        
        try:
            return self._make_request('GET', endpoint, params=params)
        except Exception as e:
            # Fallback на v2
            logger.warning(f"Ошибка v3, пробуем v2: {e}")
            endpoint = f"/messenger/v2/accounts/{user_id}/chats"
            return self._make_request('GET', endpoint, params=params)
    
    def get_all_unread_count(self, user_id: str) -> int:
        """
        Получить общее количество непрочитанных сообщений во всех чатах
        
        Args:
            user_id: ID пользователя Авито
            
        Returns:
            int: Общее количество непрочитанных сообщений
        """
        endpoint = f"/messenger/v2/accounts/{user_id}/chats/unread"
        
        try:
            result = self._make_request('GET', endpoint)
            return result.get('count', 0)
        except Exception as e:
            logger.error(f"Ошибка получения общего количества непрочитанных: {e}")
            return 0
    
    # ==================== МЕТОДЫ ДЛЯ РАБОТЫ С МЕДИА (ФОТО, ВИДЕО, ГОЛОСОВЫЕ) ====================
    
    def upload_media(self, user_id: str, file_path: str, file_type: Optional[str] = None) -> Dict:
        """
        Загрузить медиа-файл (фото, видео, голосовое сообщение) для отправки в чат
        
        Согласно документации Avito API:
        - Поддерживаются форматы: jpg, jpeg, png, gif (фото), mp4, mov (видео), ogg, m4a, mp3 (аудио)
        - Максимальный размер: 10 МБ для фото, 50 МБ для видео, 5 МБ для аудио
        
        Args:
            user_id: ID пользователя Авито
            file_path: Путь к файлу для загрузки
            file_type: Тип файла ('photo', 'video', 'audio'). Если не указан, определяется автоматически
            
        Returns:
            Dict: Информация о загруженном файле, включая attachment_id для использования в send_message
            
        Пример:
            result = api.upload_media(user_id, '/path/to/photo.jpg')
            attachment_id = result.get('attachment_id') or result.get('id')
            api.send_message(user_id, chat_id, 'Смотри фото!', attachments=[{'id': attachment_id}])
        """
        import os
        import mimetypes
        
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Файл не найден: {file_path}")
        
        # Определяем тип файла автоматически, если не указан
        if not file_type:
            mime_type, _ = mimetypes.guess_type(file_path)
            if mime_type:
                if mime_type.startswith('image/'):
                    file_type = 'photo'
                elif mime_type.startswith('video/'):
                    file_type = 'video'
                elif mime_type.startswith('audio/'):
                    file_type = 'audio'
                else:
                    # Пробуем определить по расширению
                    ext = os.path.splitext(file_path)[1].lower()
                    if ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp']:
                        file_type = 'photo'
                    elif ext in ['.mp4', '.mov', '.avi', '.mkv']:
                        file_type = 'video'
                    elif ext in ['.ogg', '.m4a', '.mp3', '.wav', '.aac']:
                        file_type = 'audio'
                    else:
                        file_type = 'photo'  # По умолчанию
            else:
                file_type = 'photo'
        
        # Проверяем размер файла
        file_size = os.path.getsize(file_path)
        max_sizes = {
            'photo': 10 * 1024 * 1024,  # 10 МБ
            'video': 50 * 1024 * 1024,  # 50 МБ
            'audio': 5 * 1024 * 1024    # 5 МБ
        }
        
        max_size = max_sizes.get(file_type, 10 * 1024 * 1024)
        if file_size > max_size:
            raise ValueError(f"Файл слишком большой: {file_size} байт. Максимум для {file_type}: {max_size} байт")
        
        # Пробуем специальный endpoint для изображений, если это фото
        if file_type == 'photo':
            # Сначала пробуем специальный endpoint для изображений
            try:
                return self.upload_images(user_id, [file_path])[0]
            except Exception as e:
                logger.warning(f"Не удалось использовать uploadImages endpoint, используем обычный upload: {e}")
        
        endpoint = f"/messenger/v2/accounts/{user_id}/uploads"
        
        # Открываем файл для загрузки
        with open(file_path, 'rb') as f:
            files = {
                'file': (os.path.basename(file_path), f, mimetypes.guess_type(file_path)[0] or 'application/octet-stream')
            }
            
            # Дополнительные параметры
            data = {
                'type': file_type
            }
            
            try:
                # Используем multipart/form-data для загрузки файла
                headers = {
                    'Authorization': f'Bearer {self.get_access_token()}'
                }
                
                url = f"{self.base_url}{endpoint}"
                response = requests.post(url, files=files, data=data, headers=headers, timeout=60)
                
                if response.status_code == 200 or response.status_code == 201:
                    result = response.json()
                    logger.info(f"Медиа-файл успешно загружен: {file_path}, attachment_id: {result.get('attachment_id') or result.get('id')}")
                    return result
                else:
                    error_msg = f"Ошибка загрузки медиа: {response.status_code} - {response.text}"
                    logger.error(error_msg)
                    raise Exception(error_msg)
            except requests.exceptions.RequestException as e:
                logger.error(f"Ошибка при загрузке медиа-файла: {e}")
                raise
    
    def upload_media_from_bytes(self, user_id: str, file_data: bytes, filename: str, 
                                file_type: Optional[str] = None, mime_type: Optional[str] = None) -> Dict:
        """
        Загрузить медиа-файл из байтов (для использования с загруженными файлами через веб-интерфейс)
        
        Args:
            user_id: ID пользователя Авито
            file_data: Байты файла
            filename: Имя файла
            file_type: Тип файла ('photo', 'video', 'audio')
            mime_type: MIME-тип файла (опционально)
            
        Returns:
            Dict: Информация о загруженном файле
        """
        import io
        import os
        import mimetypes
        
        # Определяем тип файла
        if not file_type:
            if mime_type:
                if mime_type.startswith('image/'):
                    file_type = 'photo'
                elif mime_type.startswith('video/'):
                    file_type = 'video'
                elif mime_type.startswith('audio/'):
                    file_type = 'audio'
                else:
                    file_type = 'photo'
            else:
                ext = os.path.splitext(filename)[1].lower()
                if ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp']:
                    file_type = 'photo'
                elif ext in ['.mp4', '.mov', '.avi', '.mkv']:
                    file_type = 'video'
                elif ext in ['.ogg', '.m4a', '.mp3', '.wav', '.aac']:
                    file_type = 'audio'
                else:
                    file_type = 'photo'
        
        # Проверяем размер
        file_size = len(file_data)
        max_sizes = {
            'photo': 10 * 1024 * 1024,
            'video': 50 * 1024 * 1024,
            'audio': 5 * 1024 * 1024
        }
        max_size = max_sizes.get(file_type, 10 * 1024 * 1024)
        if file_size > max_size:
            raise ValueError(f"Файл слишком большой: {file_size} байт. Максимум для {file_type}: {max_size} байт")
        
        # Для изображений используем специальный метод upload_images_from_bytes
        if file_type == 'photo':
            try:
                # Используем upload_images_from_bytes для работы с байтами
                images_data = [{'data': file_data, 'filename': filename, 'mime_type': mime_type}]
                results = self.upload_images_from_bytes(user_id, images_data)
                if results and len(results) > 0:
                    return results[0]
            except Exception as e:
                logger.warning(f"Не удалось использовать uploadImages endpoint, используем обычный upload: {e}")
        
        endpoint = f"/messenger/v2/accounts/{user_id}/uploads"
        
        files = {
            'file': (filename, io.BytesIO(file_data), mime_type or mimetypes.guess_type(filename)[0] or 'application/octet-stream')
        }
        
        data = {
            'type': file_type
        }
        
        headers = {
            'Authorization': f'Bearer {self.get_access_token()}'
        }
        
        url = f"{self.base_url}{endpoint}"
        response = requests.post(url, files=files, data=data, headers=headers, timeout=60)
        
        if response.status_code == 200 or response.status_code == 201:
            result = response.json()
            logger.info(f"Медиа-файл успешно загружен из байтов: {filename}, attachment_id: {result.get('attachment_id') or result.get('id')}")
            return result
        else:
            error_msg = f"Ошибка загрузки медиа: {response.status_code} - {response.text}"
            logger.error(error_msg)
            raise Exception(error_msg)
    
    def upload_images(self, user_id: str, image_paths: List[str]) -> List[Dict]:
        """
        Загрузить одно или несколько изображений
        
        Согласно документации Avito Messenger API v1:
        https://developers.avito.ru/api-catalog/messenger/documentation
        
        POST /messenger/v1/accounts/{user_id}/uploadImages
        Request Body: multipart/form-data с полем uploadfile[]
        
        Особенности:
        - Метод поддерживает только одиночные изображения; для загрузки нескольких картинок необходимо сделать несколько запросов
        - Максимальный размер файла — 24 МБ
        - Максимальное разрешение — 75 мегапикселей
        - Поддерживаемые форматы: JPEG, HEIC, GIF, BMP, PNG
        
        Args:
            user_id: ID пользователя Авито (integer <int64>)
            image_paths: Список путей к файлам изображений для загрузки
            
        Returns:
            List[Dict]: Список информации о загруженных изображениях, включая attachment_id
            
        Пример:
            results = api.upload_images(user_id, ['/path/to/photo1.jpg', '/path/to/photo2.jpg'])
            attachment_ids = [r.get('attachment_id') or r.get('id') for r in results]
            api.send_message(user_id, chat_id, 'Смотри фото!', attachments=[{'id': aid} for aid in attachment_ids])
        """
        import os
        import mimetypes
        
        if not image_paths:
            raise ValueError("Необходимо указать хотя бы один путь к изображению")
        
        # Согласно документации: POST /messenger/v1/accounts/{user_id}/uploadImages
        endpoint = f"/messenger/v1/accounts/{user_id}/uploadImages"
        url = f"{self.base_url}{endpoint}"
        
        headers = {
            'Authorization': f'Bearer {self.get_access_token()}'
        }
        
        results = []
        
        # Согласно документации: метод поддерживает только одиночные изображения
        # Для загрузки нескольких картинок необходимо сделать несколько запросов
        for image_path in image_paths:
            if not os.path.exists(image_path):
                raise FileNotFoundError(f"Файл не найден: {image_path}")
            
            # Проверяем, что это изображение (JPEG, HEIC, GIF, BMP, PNG)
            ext = os.path.splitext(image_path)[1].lower()
            supported_formats = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.heic']
            if ext not in supported_formats:
                raise ValueError(f"Файл {image_path} не является изображением поддерживаемого формата (поддерживаются: JPEG, HEIC, GIF, BMP, PNG)")
            
            # Проверяем размер (максимум 24 МБ согласно документации)
            file_size = os.path.getsize(image_path)
            max_size = 24 * 1024 * 1024  # 24 МБ
            if file_size > max_size:
                raise ValueError(f"Файл {image_path} слишком большой: {file_size} байт. Максимум: {max_size} байт (24 МБ)")
        
            # Подготавливаем файл для загрузки
            # Согласно документации: Request Body schema: multipart/form-data с полем uploadfile[]
            mime_type = mimetypes.guess_type(image_path)[0] or 'image/jpeg'
            with open(image_path, 'rb') as file_handle:
                files = {
                    'uploadfile[]': (os.path.basename(image_path), file_handle, mime_type)
                }
                
                try:
                    # Отправляем multipart/form-data запрос
                    response = requests.post(url, files=files, headers=headers, timeout=120)
                    
                    if response.status_code in [200, 201]:
                        result = response.json()
                        
                        # API может вернуть либо массив, либо объект
                        if isinstance(result, list):
                            results.extend(result)
                        elif isinstance(result, dict):
                            # Пробуем разные ключи
                            items = result.get('images', result.get('items', result.get('data', [result])))
                            if isinstance(items, list):
                                results.extend(items)
                            else:
                                results.append(result)
                        else:
                            results.append(result)
                        
                        logger.info(f"Изображение {os.path.basename(image_path)} успешно загружено через {endpoint}")
                    else:
                        error_msg = f"Ошибка загрузки изображения {image_path}: {response.status_code} - {response.text}"
                        logger.error(error_msg)
                        raise Exception(error_msg)
                except Exception as e:
                    logger.error(f"Ошибка загрузки изображения {image_path}: {e}")
                    raise
        
        return results
    
    def upload_images_from_bytes(self, user_id: str, images_data: List[Dict]) -> List[Dict]:
        """
        Загрузить одно или несколько изображений из байтов
        
        Согласно документации Avito Messenger API v1:
        https://developers.avito.ru/api-catalog/messenger/documentation
        
        POST /messenger/v1/accounts/{user_id}/uploadImages
        Request Body: multipart/form-data с полем uploadfile[]
        
        Особенности:
        - Метод поддерживает только одиночные изображения; для загрузки нескольких картинок необходимо сделать несколько запросов
        - Максимальный размер файла — 24 МБ
        - Максимальное разрешение — 75 мегапикселей
        - Поддерживаемые форматы: JPEG, HEIC, GIF, BMP, PNG
        
        Args:
            user_id: ID пользователя Авито (integer <int64>)
            images_data: Список словарей с данными изображений:
                        [{'data': bytes, 'filename': str, 'mime_type': str (опционально)}, ...]
            
        Returns:
            List[Dict]: Список информации о загруженных изображениях
            
        Пример:
            results = api.upload_images_from_bytes(user_id, [
                {'data': image1_bytes, 'filename': 'photo1.jpg'},
                {'data': image2_bytes, 'filename': 'photo2.jpg'}
            ])
        """
        import io
        import os
        import mimetypes
        
        if not images_data:
            raise ValueError("Необходимо указать хотя бы одно изображение")
        
        # Согласно документации: POST /messenger/v1/accounts/{user_id}/uploadImages
        endpoint = f"/messenger/v1/accounts/{user_id}/uploadImages"
        url = f"{self.base_url}{endpoint}"
        
        headers = {
            'Authorization': f'Bearer {self.get_access_token()}'
        }
        
        results = []
        
        # Согласно документации: метод поддерживает только одиночные изображения
        # Для загрузки нескольких картинок необходимо сделать несколько запросов
        for i, img_data in enumerate(images_data):
            if not isinstance(img_data, dict):
                raise ValueError(f"Элемент {i} должен быть словарем")
            if 'data' not in img_data or 'filename' not in img_data:
                raise ValueError(f"Элемент {i} должен содержать поля 'data' и 'filename'")
            
            file_data = img_data['data']
            filename = img_data['filename']
            mime_type = img_data.get('mime_type') or mimetypes.guess_type(filename)[0] or 'image/jpeg'
            
            # Проверяем формат файла (JPEG, HEIC, GIF, BMP, PNG)
            ext = os.path.splitext(filename)[1].lower()
            supported_formats = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.heic']
            if ext not in supported_formats:
                raise ValueError(f"Изображение {filename} не является поддерживаемого формата (поддерживаются: JPEG, HEIC, GIF, BMP, PNG)")
            
            # Проверяем размер (максимум 24 МБ согласно документации)
            file_size = len(file_data)
            max_size = 24 * 1024 * 1024  # 24 МБ
            if file_size > max_size:
                raise ValueError(f"Изображение {filename} слишком большое: {file_size} байт. Максимум: {max_size} байт (24 МБ)")
        
            # Согласно документации: Request Body schema: multipart/form-data с полем uploadfile[]
            files = {
                'uploadfile[]': (filename, io.BytesIO(file_data), mime_type)
            }
            
            try:
                response = requests.post(url, files=files, headers=headers, timeout=120)
                
                if response.status_code in [200, 201]:
                    result = response.json()
                    
                    # API может вернуть либо массив, либо объект
                    if isinstance(result, list):
                        results.extend(result)
                    elif isinstance(result, dict):
                        items = result.get('images', result.get('items', result.get('data', [result])))
                        if isinstance(items, list):
                            results.extend(items)
                        else:
                            results.append(result)
                    else:
                        results.append(result)
                    
                    logger.info(f"Изображение {filename} успешно загружено из байтов через {endpoint}")
                else:
                    error_msg = f"Ошибка загрузки изображения {filename}: {response.status_code} - {response.text}"
                    logger.error(error_msg)
                    raise Exception(error_msg)
            except Exception as e:
                logger.error(f"Ошибка загрузки изображения {filename}: {e}")
                raise
        
        return results
    
    def get_media_info(self, user_id: str, attachment_id: str) -> Dict:
        """
        Получить информацию о загруженном медиа-файле
        
        Args:
            user_id: ID пользователя Авито
            attachment_id: ID вложения
            
        Returns:
            Dict: Информация о медиа-файле (URL, размер, тип и т.д.)
        """
        endpoint = f"/messenger/v2/accounts/{user_id}/uploads/{attachment_id}"
        
        try:
            return self._make_request('GET', endpoint)
        except Exception as e:
            logger.error(f"Ошибка получения информации о медиа: {e}")
            raise
    
    def download_media(self, user_id: str, attachment_id: str, save_path: Optional[str] = None) -> bytes:
        """
        Скачать медиа-файл по attachment_id
        
        Args:
            user_id: ID пользователя Авито
            attachment_id: ID вложения
            save_path: Путь для сохранения файла (опционально)
            
        Returns:
            bytes: Содержимое файла
        """
        endpoint = f"/messenger/v2/accounts/{user_id}/uploads/{attachment_id}/download"
        
        try:
            headers = {
                'Authorization': f'Bearer {self.get_access_token()}'
            }
            
            url = f"{self.base_url}{endpoint}"
            response = requests.get(url, headers=headers, timeout=60)
            
            if response.status_code == 200:
                file_data = response.content
                
                if save_path:
                    with open(save_path, 'wb') as f:
                        f.write(file_data)
                    logger.info(f"Медиа-файл сохранен: {save_path}")
                
                return file_data
            else:
                error_msg = f"Ошибка скачивания медиа: {response.status_code} - {response.text}"
                logger.error(error_msg)
                raise Exception(error_msg)
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка при скачивании медиа-файла: {e}")
            raise
    
    def get_voice_files(self, user_id: str, voice_ids: List[str]) -> Dict:
        """
        Получить ссылки на файлы голосовых сообщений по идентификаторам voice_id
        
        Согласно документации Avito Messenger API v1:
        https://developers.avito.ru/api-catalog/messenger/documentation
        
        GET /messenger/v1/accounts/{user_id}/getVoiceFiles
        Query Parameters: voice_ids (Array of strings) - required
        
        Особенности работы с голосовыми сообщениями:
        - Голосовые сообщения Авито используют кодек opus внутри .mp4 контейнера
        - Ссылка на голосовое сообщение доступна в течение одного часа с момента запроса
        - Попытка получить файл по ссылке спустя это время приведёт к ошибке
        - Для восстановления доступа необходимо получить новую ссылку на файл
        - Получение ссылки на файл доступно только для пользователей, находящихся в беседе,
          где голосовое сообщение было отправлено
        
        Args:
            user_id: ID пользователя Авито (integer <int64>)
            voice_ids: Список идентификаторов voice_id (получаются из тела сообщения с типом voice)
            
        Returns:
            Dict: Информация о голосовых файлах с ссылками для скачивания
            
        Пример:
            # voice_id получается из сообщения с типом voice
            voice_files = api.get_voice_files(user_id, ['voice_id_1', 'voice_id_2'])
            for file_info in voice_files.get('items', []):
                file_url = file_info.get('url')
                # Ссылка действительна в течение 1 часа
        """
        if not voice_ids:
            raise ValueError("Необходимо указать хотя бы один voice_id")
        
        # Согласно документации: GET /messenger/v1/accounts/{user_id}/getVoiceFiles
        endpoint = f"/messenger/v1/accounts/{user_id}/getVoiceFiles"
        
        # Согласно документации: query Parameters: voice_ids (Array of strings) - required
        # Для передачи массива в query параметрах используем список кортежей
        # Формат: voice_ids[]=value1&voice_ids[]=value2
        params = [('voice_ids[]', voice_id) for voice_id in voice_ids]
        
        # Используем прямой вызов requests, так как _make_request не поддерживает списки кортежей для params
        url = f"{self.base_url}{endpoint}"
        headers = {
            'Authorization': f'Bearer {self.get_access_token()}'
        }
        
        try:
            response = requests.get(url, params=params, headers=headers, timeout=30)
            
            if response.status_code == 200:
                result = response.json()
                logger.info(f"Ссылки на голосовые файлы успешно получены для {len(voice_ids)} voice_id")
                return result
            else:
                error_msg = f"Ошибка получения голосовых файлов: {response.status_code} - {response.text}"
                logger.error(error_msg)
                raise Exception(error_msg)
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка запроса голосовых файлов: {e}")
            raise
    
    def download_voice_file(self, user_id: str, voice_file_id: str, save_path: Optional[str] = None) -> bytes:
        """
        Скачать голосовой файл по ID
        
        Args:
            user_id: ID пользователя Авито
            voice_file_id: ID голосового файла
            save_path: Путь для сохранения файла (опционально)
            
        Returns:
            bytes: Содержимое голосового файла (обычно в формате ogg, m4a или mp3)
        """
        # Пробуем разные варианты endpoint
        endpoints_to_try = [
            f"/messenger/v3/accounts/{user_id}/voice/{voice_file_id}/download",
            f"/messenger/v2/accounts/{user_id}/voice/{voice_file_id}/download",
            f"/messenger/v3/accounts/{user_id}/voice/{voice_file_id}",
            f"/messenger/v2/accounts/{user_id}/voice/{voice_file_id}",
        ]
        
        headers = {
            'Authorization': f'Bearer {self.get_access_token()}'
        }
        
        for endpoint in endpoints_to_try:
            try:
                url = f"{self.base_url}{endpoint}"
                response = requests.get(url, headers=headers, timeout=60)
                
                if response.status_code == 200:
                    file_data = response.content
                    
                    if save_path:
                        with open(save_path, 'wb') as f:
                            f.write(file_data)
                        logger.info(f"Голосовой файл сохранен: {save_path}")
                    
                    return file_data
                elif response.status_code == 404:
                    continue
                else:
                    error_msg = f"Ошибка скачивания голосового файла: {response.status_code} - {response.text}"
                    logger.error(error_msg)
                    raise Exception(error_msg)
            except requests.exceptions.RequestException as e:
                if '404' not in str(e):
                    logger.error(f"Ошибка при скачивании голосового файла: {e}")
                    raise
                continue
        
        raise Exception(f"Не удалось скачать голосовой файл {voice_file_id}: все endpoints вернули 404")
    
    def get_voice_file_info(self, user_id: str, voice_file_id: str) -> Dict:
        """
        Получить информацию о голосовом файле
        
        Args:
            user_id: ID пользователя Авито
            voice_file_id: ID голосового файла
            
        Returns:
            Dict: Информация о голосовом файле (URL, размер, длительность, формат и т.д.)
        """
        # Пробуем разные варианты endpoint
        endpoints_to_try = [
            f"/messenger/v3/accounts/{user_id}/voice/{voice_file_id}",
            f"/messenger/v2/accounts/{user_id}/voice/{voice_file_id}",
        ]
        
        for endpoint in endpoints_to_try:
            try:
                result = self._make_request('GET', endpoint)
                logger.info(f"Информация о голосовом файле получена через: {endpoint}")
                return result
            except Exception as e:
                error_str = str(e)
                if '404' not in error_str:
                    logger.error(f"Ошибка получения информации о голосовом файле (не 404): {error_str}")
                    raise
                logger.warning(f"404 с {endpoint}, пробуем следующий...")
                continue
        
        raise Exception(f"Не удалось получить информацию о голосовом файле {voice_file_id}: все endpoints вернули 404")
    
    def send_message_with_media(self, user_id: str, chat_id: str, message: str, 
                                 media_paths: Optional[List[str]] = None,
                                 media_data: Optional[List[Dict]] = None) -> Dict:
        """
        Отправить сообщение с медиа-файлами (удобный метод, который сам загружает файлы)
        
        Args:
            user_id: ID пользователя Авито
            chat_id: ID чата
            message: Текст сообщения
            media_paths: Список путей к файлам для загрузки
            media_data: Список словарей с данными файлов {'data': bytes, 'filename': str, 'type': str}
            
        Returns:
            Dict: Информация об отправленном сообщении
            
        Пример:
            # Из файлов
            api.send_message_with_media(user_id, chat_id, 'Смотри фото!', 
                                      media_paths=['/path/to/photo1.jpg', '/path/to/photo2.jpg'])
            
            # Из байтов
            api.send_message_with_media(user_id, chat_id, 'Голосовое сообщение',
                                      media_data=[{'data': audio_bytes, 'filename': 'voice.ogg', 'type': 'audio'}])
        """
        attachments = []
        
        # Загружаем файлы из путей
        if media_paths:
            for media_path in media_paths:
                try:
                    upload_result = self.upload_media(user_id, media_path)
                    attachment_id = upload_result.get('attachment_id') or upload_result.get('id')
                    if attachment_id:
                        attachments.append({'id': attachment_id})
                except Exception as e:
                    logger.error(f"Ошибка загрузки медиа из {media_path}: {e}")
                    # Продолжаем с другими файлами
        
        # Загружаем файлы из байтов
        if media_data:
            for media_item in media_data:
                try:
                    file_data = media_item.get('data')
                    filename = media_item.get('filename', 'file')
                    file_type = media_item.get('type')
                    mime_type = media_item.get('mime_type')
                    
                    upload_result = self.upload_media_from_bytes(
                        user_id, file_data, filename, file_type, mime_type
                    )
                    attachment_id = upload_result.get('attachment_id') or upload_result.get('id')
                    if attachment_id:
                        attachments.append({'id': attachment_id})
                except Exception as e:
                    logger.error(f"Ошибка загрузки медиа из байтов: {e}")
                    # Продолжаем с другими файлами
        
        # Отправляем сообщение с вложениями
        return self.send_message(user_id, chat_id, message, attachments=attachments if attachments else None)
    
    def send_image_message(self, user_id: str, chat_id: str, image_path: Optional[str] = None,
                           image_data: Optional[bytes] = None, filename: Optional[str] = None,
                           message: Optional[str] = None) -> Dict:
        """
        Отправить сообщение с изображением
        
        Согласно документации Avito Messenger API v1:
        https://developers.avito.ru/api-catalog/messenger/documentation
        
        POST /messenger/v1/accounts/{user_id}/chats/{chat_id}/messages/image
        Request Body: {"image_id": "string"} (required)
        
        Для отправки сообщения с изображением необходимо:
        1. Загрузить изображение через upload_images() для получения image_id
        2. Отправить изображение через специальный endpoint с image_id
        
        Args:
            user_id: ID пользователя Авито (integer <int64>)
            chat_id: ID чата (string)
            image_path: Путь к файлу изображения (опционально, если используется image_data)
            image_data: Байты изображения (опционально, если используется image_path)
            filename: Имя файла (обязательно, если используется image_data)
            message: Текст сообщения (опционально, не поддерживается в v1 endpoint для изображений)
            
        Returns:
            Dict: Информация об отправленном сообщении
            
        Примеры:
            # Из файла
            api.send_image_message(user_id, chat_id, image_path='/path/to/photo.jpg')
            
            # Из байтов
            api.send_image_message(user_id, chat_id, image_data=image_bytes, filename='photo.jpg')
        """
        import os
        
        # Валидация входных данных
        if not image_path and not image_data:
            raise ValueError("Необходимо указать либо image_path, либо image_data")
        
        if image_data and not filename:
            raise ValueError("filename обязателен при использовании image_data")
        
        # Загружаем изображение через upload_images для получения image_id
        if image_path:
            upload_results = self.upload_images(user_id, [image_path])
        else:
            upload_results = self.upload_images_from_bytes(
                user_id, [{'data': image_data, 'filename': filename}]
            )
        
        if not upload_results or len(upload_results) == 0:
            raise ValueError("Не удалось загрузить изображение")
        
        # Получаем image_id из результата загрузки
        upload_result = upload_results[0]
        image_id = upload_result.get('id') or upload_result.get('image_id') or upload_result.get('attachment_id')
        
        if not image_id:
            raise ValueError("Не удалось получить image_id после загрузки изображения")
        
        # Отправляем изображение через v1 endpoint
        return self.send_image_message_direct(user_id, chat_id, str(image_id))
    
    def send_image_message_direct(self, user_id: str, chat_id: str, image_id: str) -> Dict:
        """
        Отправить сообщение с изображением через специальный endpoint
        
        Согласно документации Avito Messenger API v1:
        https://developers.avito.ru/api-catalog/messenger/documentation
        
        POST /messenger/v1/accounts/{user_id}/chats/{chat_id}/messages/image
        Request Body: {"image_id": "string"} (required)
        
        Для отправки сообщения с изображением необходимо передать в запросе id изображения,
        полученного после загрузки через метод upload_images().
        
        Args:
            user_id: ID пользователя Авито (integer <int64>)
            chat_id: ID чата (string)
            image_id: Идентификатор загруженного изображения (получен после загрузки через upload_images)
            
        Returns:
            Dict: Информация об отправленном сообщении
            
        Пример:
            # Сначала загружаем изображение
            upload_result = api.upload_images(user_id, ['/path/to/photo.jpg'])
            image_id = upload_result[0].get('id') or upload_result[0].get('image_id')
            
            # Затем отправляем его
            result = api.send_image_message_direct(user_id, chat_id, image_id)
        """
        from urllib.parse import quote
        
        if not image_id:
            raise ValueError("Необходимо указать image_id (идентификатор загруженного изображения)")
        
        # URL-кодируем chat_id, так как он может содержать спецсимволы
        encoded_chat_id = quote(chat_id, safe='')
        
        # Согласно документации: POST /messenger/v1/accounts/{user_id}/chats/{chat_id}/messages/image
        endpoint = f"/messenger/v1/accounts/{user_id}/chats/{encoded_chat_id}/messages/image"
        
        # Согласно документации: Request Body: {"image_id": "string"} (required)
        data = {
            'image_id': str(image_id)
        }
        
        try:
            result = self._make_request('POST', endpoint, data=data)
            logger.info(f"Изображение с image_id={image_id} успешно отправлено в чат {chat_id}")
            return result
        except Exception as e:
            logger.error(f"Ошибка отправки изображения с image_id={image_id}: {e}")
            raise

