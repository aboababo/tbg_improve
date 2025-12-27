"""
OsaGaming CRM - Парсер публичных объявлений Авито
==================================================

Парсер для поиска объявлений на всем Авито по заданным параметрам.
Использует веб-поиск Авито и API для обработки результатов.

Автор: OsaGaming Development Team
Версия: 2.0
"""

import requests
import json
import time
import logging
from typing import Dict, List, Optional, Any
from urllib.parse import urlencode, quote
import re
from bs4 import BeautifulSoup

logger = logging.getLogger('avito_parser')


class AvitoPublicParser:
    """
    Парсер для поиска объявлений на всем Авито
    
    Использует публичный поиск Авито через веб-интерфейс
    """
    
    BASE_URL = "https://www.avito.ru"
    SEARCH_URL = "https://www.avito.ru/all"
    
    def __init__(self):
        """Инициализация парсера"""
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
    
    def search_listings(self, 
                       query: Optional[str] = None,
                       category_id: Optional[int] = None,
                       location_id: Optional[int] = None,
                       price_min: Optional[float] = None,
                       price_max: Optional[float] = None,
                       params: Optional[Dict] = None,
                       limit: int = 50) -> Dict:
        """
        Поиск объявлений на Авито по заданным параметрам
        
        Args:
            query: Поисковый запрос
            category_id: ID категории
            location_id: ID локации (региона)
            price_min: Минимальная цена
            price_max: Максимальная цена
            params: Дополнительные параметры поиска
            limit: Максимальное количество результатов
            
        Returns:
            Dict: Список найденных объявлений
        """
        logger.info(f"[AVITO PARSER] ========== НАЧАЛО ПОИСКА ОБЪЯВЛЕНИЙ ==========")
        logger.info(f"[AVITO PARSER] Параметры поиска: query='{query}', category_id={category_id}, "
                   f"location_id={location_id}, price_min={price_min}, price_max={price_max}, limit={limit}")
        
        try:
            # Формируем URL для поиска
            search_params = {}
            
            if query:
                search_params['q'] = query
                logger.debug(f"[AVITO PARSER] Добавлен параметр поиска: q='{query}'")
            
            if category_id:
                search_params['categoryId'] = category_id
                logger.debug(f"[AVITO PARSER] Добавлен параметр: categoryId={category_id}")
            
            if location_id:
                search_params['locationId'] = location_id
                logger.debug(f"[AVITO PARSER] Добавлен параметр: locationId={location_id}")
            
            if price_min:
                search_params['pmin'] = int(price_min)
                logger.debug(f"[AVITO PARSER] Добавлен параметр: pmin={int(price_min)}")
            
            if price_max:
                search_params['pmax'] = int(price_max)
                logger.debug(f"[AVITO PARSER] Добавлен параметр: pmax={int(price_max)}")
            
            # Добавляем дополнительные параметры
            if params:
                search_params.update(params)
                logger.debug(f"[AVITO PARSER] Добавлены дополнительные параметры: {params}")
            
            # Формируем полный URL
            url = f"{self.SEARCH_URL}?{urlencode(search_params)}"
            
            logger.info(f"[AVITO PARSER] Сформирован URL для запроса: {url}")
            logger.debug(f"[AVITO PARSER] Все параметры поиска: {search_params}")
            
            # Выполняем запрос
            import time
            request_start = time.time()
            
            logger.info(f"[AVITO PARSER] Отправка HTTP GET запроса к Avito...")
            logger.debug(f"[AVITO PARSER] User-Agent: {self.session.headers.get('User-Agent', 'N/A')[:80]}")
            
            response = self.session.get(url, timeout=30)
            
            request_time = time.time() - request_start
            logger.info(f"[AVITO PARSER] HTTP запрос выполнен за {request_time:.2f} сек. "
                       f"Статус: {response.status_code}")
            
            response.raise_for_status()
            
            # Анализируем ответ
            content_length = len(response.content)
            text_length = len(response.text)
            logger.info(f"[AVITO PARSER] Размер ответа: {content_length} байт (текст: {text_length} символов)")
            logger.debug(f"[AVITO PARSER] Content-Type: {response.headers.get('Content-Type', 'N/A')}")
            logger.debug(f"[AVITO PARSER] Первые 200 символов ответа: {response.text[:200]}")
            
            # Парсим HTML
            parse_start = time.time()
            logger.info(f"[AVITO PARSER] Начало парсинга HTML...")
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            parse_time = time.time() - parse_start
            logger.info(f"[AVITO PARSER] HTML распарсен за {parse_time:.2f} сек.")
            
            # Ищем объявления в HTML
            logger.info(f"[AVITO PARSER] Поиск объявлений в HTML (лимит: {limit})...")
            listings = self._parse_listings_from_html(soup, limit)
            
            listings_count = len(listings)
            logger.info(f"[AVITO PARSER] Найдено объявлений: {listings_count}")
            
            if listings_count > 0:
                logger.debug(f"[AVITO PARSER] Примеры найденных объявлений:")
                for idx, listing in enumerate(listings[:5], 1):  # Первые 5
                    logger.debug(f"[AVITO PARSER]   {idx}. ID={listing.get('listing_id', 'N/A')}, "
                                f"title='{listing.get('title', 'N/A')[:50]}', "
                                f"price={listing.get('price', 'N/A')}")
            
            result = {
                'listings': listings,
                'total': listings_count,
                'query': query,
                'params': search_params
            }
            
            logger.info(f"[AVITO PARSER] ========== ПОИСК ЗАВЕРШЕН УСПЕШНО ==========")
            logger.info(f"[AVITO PARSER] Итого: найдено {listings_count} объявлений")
            
            return result
            
        except requests.exceptions.Timeout as e:
            logger.error(f"[AVITO PARSER] ТАЙМАУТ при запросе к Avito: {str(e)}")
            raise
        except requests.exceptions.HTTPError as e:
            logger.error(f"[AVITO PARSER] HTTP ОШИБКА при запросе к Avito: {e.response.status_code} - {str(e)}")
            logger.error(f"[AVITO PARSER] URL запроса: {url if 'url' in locals() else 'N/A'}")
            raise
        except Exception as e:
            logger.error(f"[AVITO PARSER] КРИТИЧЕСКАЯ ОШИБКА при поиске объявлений: {str(e)}", exc_info=True)
            logger.error(f"[AVITO PARSER] URL запроса: {url if 'url' in locals() else 'N/A'}")
            raise
    
    def _parse_listings_from_html(self, soup: BeautifulSoup, limit: int) -> List[Dict]:
        """
        Парсинг объявлений из HTML страницы
        
        Args:
            soup: BeautifulSoup объект с HTML
            limit: Максимальное количество объявлений
            
        Returns:
            List[Dict]: Список объявлений
        """
        logger.debug(f"[AVITO PARSER] Начало парсинга HTML, лимит: {limit}")
        listings = []
        
        try:
            # Ищем объявления в разных форматах (Авито может менять структуру)
            # Вариант 1: Поиск по data-marker
            logger.debug(f"[AVITO PARSER] Вариант 1: Поиск по data-marker='item'...")
            items = soup.find_all('div', {'data-marker': re.compile(r'item')})
            logger.debug(f"[AVITO PARSER] Вариант 1: найдено {len(items)} элементов")
            
            # Вариант 2: Поиск по классам
            if not items:
                logger.debug(f"[AVITO PARSER] Вариант 2: Поиск по классам 'item|listing|ad'...")
                items = soup.find_all('div', class_=re.compile(r'item|listing|ad'))
                logger.debug(f"[AVITO PARSER] Вариант 2: найдено {len(items)} элементов")
            
            # Вариант 3: Поиск по структуре
            if not items:
                logger.debug(f"[AVITO PARSER] Вариант 3: Поиск по ссылкам с паттерном '/.*/\\d+$'...")
                items = soup.find_all('a', href=re.compile(r'/.*/\d+$'))
                logger.debug(f"[AVITO PARSER] Вариант 3: найдено {len(items)} элементов")
            
            total_items = len(items)
            logger.info(f"[AVITO PARSER] Всего найдено потенциальных элементов: {total_items}, "
                       f"будет обработано: {min(total_items, limit)}")
            
            if total_items == 0:
                logger.warning(f"[AVITO PARSER] Не найдено элементов для парсинга через HTML")
            
            successful_parses = 0
            failed_parses = 0
            
            for idx, item in enumerate(items[:limit], 1):
                try:
                    logger.debug(f"[AVITO PARSER] Парсинг элемента {idx}/{min(total_items, limit)}...")
                    listing = self._parse_single_listing(item)
                    if listing:
                        listings.append(listing)
                        successful_parses += 1
                        logger.debug(f"[AVITO PARSER] Элемент {idx} успешно распарсен: "
                                   f"ID={listing.get('listing_id', 'N/A')}")
                    else:
                        failed_parses += 1
                        logger.debug(f"[AVITO PARSER] Элемент {idx} не прошел валидацию (нет URL или title)")
                except Exception as e:
                    failed_parses += 1
                    logger.debug(f"[AVITO PARSER] Ошибка парсинга элемента {idx}: {str(e)}")
                    continue
            
            logger.info(f"[AVITO PARSER] Парсинг HTML завершен: успешно={successful_parses}, "
                       f"ошибок={failed_parses}, всего валидных={len(listings)}")
            
            # Если не нашли через стандартные методы, пробуем парсить JSON из скриптов
            if not listings:
                logger.warning(f"[AVITO PARSER] Не найдено объявлений через HTML парсинг, "
                              f"пробуем парсить JSON из скриптов...")
                listings = self._parse_listings_from_json(soup, limit)
                if listings:
                    logger.info(f"[AVITO PARSER] Найдено {len(listings)} объявлений через JSON парсинг")
            
        except Exception as e:
            logger.error(f"[AVITO PARSER] КРИТИЧЕСКАЯ ОШИБКА при парсинге HTML: {str(e)}", exc_info=True)
        
        logger.debug(f"[AVITO PARSER] Итого распарсено объявлений: {len(listings)}")
        return listings
    
    def _parse_single_listing(self, item) -> Optional[Dict]:
        """
        Парсинг одного объявления
        
        Args:
            item: BeautifulSoup элемент с объявлением
            
        Returns:
            Dict: Данные объявления или None
        """
        try:
            listing = {}
            
            # Ищем ссылку
            logger.debug(f"[AVITO PARSER] Поиск ссылки в элементе...")
            link_elem = item.find('a', href=True)
            if link_elem:
                href = link_elem.get('href', '')
                logger.debug(f"[AVITO PARSER] Найдена ссылка: {href[:100]}")
                if href.startswith('/'):
                    listing['url'] = f"{self.BASE_URL}{href}"
                else:
                    listing['url'] = href
                
                # Извлекаем ID из URL
                match = re.search(r'/(\d+)$', href)
                if match:
                    listing['listing_id'] = match.group(1)
                    logger.debug(f"[AVITO PARSER] Извлечен listing_id: {listing['listing_id']}")
                else:
                    logger.debug(f"[AVITO PARSER] Не удалось извлечь listing_id из URL")
            else:
                logger.debug(f"[AVITO PARSER] Ссылка не найдена")
            
            # Ищем заголовок
            logger.debug(f"[AVITO PARSER] Поиск заголовка...")
            title_elem = item.find(['h3', 'span', 'div'], class_=re.compile(r'title|name|heading'))
            if not title_elem:
                title_elem = item.find('a', class_=re.compile(r'title|name'))
            if title_elem:
                listing['title'] = title_elem.get_text(strip=True)
                logger.debug(f"[AVITO PARSER] Найден заголовок: '{listing['title'][:50]}'")
            else:
                logger.debug(f"[AVITO PARSER] Заголовок не найден")
            
            # Ищем цену
            logger.debug(f"[AVITO PARSER] Поиск цены...")
            price_elem = item.find(['span', 'div', 'meta'], class_=re.compile(r'price|cost'))
            if not price_elem:
                price_elem = item.find(string=re.compile(r'\d+\s*₽'))
            if price_elem:
                price_text = price_elem.get_text(strip=True) if hasattr(price_elem, 'get_text') else str(price_elem)
                price_match = re.search(r'(\d[\d\s]*)', price_text.replace(' ', ''))
                if price_match:
                    listing['price'] = float(price_match.group(1).replace(' ', ''))
                    logger.debug(f"[AVITO PARSER] Найдена цена: {listing['price']}")
                else:
                    logger.debug(f"[AVITO PARSER] Цена найдена, но не распознана: '{price_text[:30]}'")
            else:
                logger.debug(f"[AVITO PARSER] Цена не найдена")
            
            # Ищем изображение
            logger.debug(f"[AVITO PARSER] Поиск изображения...")
            img_elem = item.find('img', src=True)
            if img_elem:
                listing['image_url'] = img_elem.get('src', '')
                logger.debug(f"[AVITO PARSER] Найдено изображение: {listing['image_url'][:80]}")
            else:
                logger.debug(f"[AVITO PARSER] Изображение не найдено")
            
            # Ищем локацию
            logger.debug(f"[AVITO PARSER] Поиск локации...")
            location_elem = item.find(['span', 'div'], class_=re.compile(r'location|address|geo'))
            if location_elem:
                listing['location'] = location_elem.get_text(strip=True)
                logger.debug(f"[AVITO PARSER] Найдена локация: '{listing['location']}'")
            else:
                logger.debug(f"[AVITO PARSER] Локация не найдена")
            
            # Ищем описание
            logger.debug(f"[AVITO PARSER] Поиск описания...")
            desc_elem = item.find(['div', 'span'], class_=re.compile(r'description|text|content'))
            if desc_elem:
                listing['description'] = desc_elem.get_text(strip=True)
                logger.debug(f"[AVITO PARSER] Найдено описание: {len(listing['description'])} символов")
            else:
                logger.debug(f"[AVITO PARSER] Описание не найдено")
            
            # Если есть хотя бы URL и заголовок, считаем объявление валидным
            has_url = bool(listing.get('url'))
            has_title = bool(listing.get('title'))
            
            logger.debug(f"[AVITO PARSER] Валидация: has_url={has_url}, has_title={has_title}")
            
            if has_url and has_title:
                logger.debug(f"[AVITO PARSER] Объявление валидно, возвращаем результат")
                return listing
            else:
                logger.debug(f"[AVITO PARSER] Объявление не валидно (нет обязательных полей)")
            
        except Exception as e:
            logger.debug(f"[AVITO PARSER] Ошибка парсинга объявления: {str(e)}", exc_info=True)
        
        return None
    
    def _parse_listings_from_json(self, soup: BeautifulSoup, limit: int) -> List[Dict]:
        """
        Парсинг объявлений из JSON данных в скриптах страницы
        
        Args:
            soup: BeautifulSoup объект
            limit: Максимальное количество
            
        Returns:
            List[Dict]: Список объявлений
        """
        logger.debug(f"[AVITO PARSER] Начало парсинга JSON из скриптов, лимит: {limit}")
        listings = []
        
        try:
            # Ищем скрипты с JSON данными
            logger.debug(f"[AVITO PARSER] Поиск script тегов с type='application/json'...")
            scripts = soup.find_all('script', type='application/json')
            logger.info(f"[AVITO PARSER] Найдено JSON скриптов: {len(scripts)}")
            
            processed_scripts = 0
            failed_scripts = 0
            
            for script_idx, script in enumerate(scripts, 1):
                try:
                    logger.debug(f"[AVITO PARSER] Обработка скрипта {script_idx}/{len(scripts)}...")
                    script_length = len(script.string) if script.string else 0
                    logger.debug(f"[AVITO PARSER] Размер скрипта {script_idx}: {script_length} символов")
                    
                    data = json.loads(script.string)
                    logger.debug(f"[AVITO PARSER] JSON скрипта {script_idx} успешно распарсен")
                    
                    # Рекурсивно ищем объявления в JSON
                    logger.debug(f"[AVITO PARSER] Извлечение элементов из JSON скрипта {script_idx}...")
                    items = self._extract_items_from_json(data)
                    logger.debug(f"[AVITO PARSER] Из скрипта {script_idx} извлечено элементов: {len(items)}")
                    
                    valid_items = 0
                    for item_idx, item in enumerate(items[:limit], 1):
                        try:
                            listing = {
                                'listing_id': str(item.get('id', '')),
                                'title': item.get('title', ''),
                                'price': item.get('price', 0),
                                'url': item.get('url', ''),
                                'image_url': item.get('image', {}).get('url', '') if isinstance(item.get('image'), dict) else item.get('image', ''),
                                'location': item.get('location', {}).get('name', '') if isinstance(item.get('location'), dict) else item.get('location', ''),
                                'description': item.get('description', '')
                            }
                            
                            if listing.get('url'):
                                if listing['url'].startswith('/'):
                                    listing['url'] = f"{self.BASE_URL}{listing['url']}"
                                
                                listings.append(listing)
                                valid_items += 1
                                logger.debug(f"[AVITO PARSER] Добавлено объявление из JSON: "
                                           f"ID={listing['listing_id']}, title='{listing['title'][:40]}'")
                        except Exception as item_error:
                            logger.debug(f"[AVITO PARSER] Ошибка обработки элемента {item_idx} из скрипта {script_idx}: {str(item_error)}")
                            continue
                    
                    processed_scripts += 1
                    logger.debug(f"[AVITO PARSER] Скрипт {script_idx} обработан: валидных объявлений={valid_items}")
                    
                    if len(listings) >= limit:
                        logger.info(f"[AVITO PARSER] Достигнут лимит объявлений ({limit}), остановка парсинга")
                        break
                            
                except json.JSONDecodeError as json_error:
                    failed_scripts += 1
                    logger.debug(f"[AVITO PARSER] Ошибка парсинга JSON в скрипте {script_idx}: {str(json_error)}")
                    continue
                except KeyError as key_error:
                    failed_scripts += 1
                    logger.debug(f"[AVITO PARSER] Ошибка ключа в скрипте {script_idx}: {str(key_error)}")
                    continue
                except Exception as script_error:
                    failed_scripts += 1
                    logger.debug(f"[AVITO PARSER] Ошибка обработки скрипта {script_idx}: {str(script_error)}")
                    continue
            
            logger.info(f"[AVITO PARSER] JSON парсинг завершен: обработано скриптов={processed_scripts}, "
                       f"ошибок={failed_scripts}, найдено объявлений={len(listings)}")
                    
        except Exception as e:
            logger.error(f"[AVITO PARSER] КРИТИЧЕСКАЯ ОШИБКА при парсинге JSON: {str(e)}", exc_info=True)
        
        return listings
    
    def _extract_items_from_json(self, data: Any, items: Optional[List] = None) -> List[Dict]:
        """
        Рекурсивное извлечение объявлений из JSON
        
        Args:
            data: JSON данные
            items: Список для накопления результатов
            
        Returns:
            List[Dict]: Список объявлений
        """
        if items is None:
            items = []
        
        if isinstance(data, dict):
            # Проверяем, является ли это объявлением
            if 'id' in data and ('title' in data or 'name' in data):
                items.append(data)
            else:
                for value in data.values():
                    self._extract_items_from_json(value, items)
        elif isinstance(data, list):
            for item in data:
                self._extract_items_from_json(item, items)
        
        return items
    
    def get_listing_details(self, url: str, max_retries: int = 3) -> Dict:
        """
        Получение детальной информации об объявлении с публичной страницы
        
        Args:
            url: URL объявления
            max_retries: Максимальное количество попыток при ошибках
            
        Returns:
            Dict: Детальная информация
        """
        logger.info(f"[AVITO PARSER] Парсинг публичной страницы объявления: {url}")
        
        last_exception = None
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, timeout=30)
                
                # Обработка 429 - Too Many Requests
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    if attempt < max_retries - 1:
                        wait_time = retry_after + (attempt * 10)  # Добавляем дополнительную задержку
                        logger.warning(f"[AVITO PARSER] Rate limit (429), ожидание {wait_time} сек... (попытка {attempt + 1}/{max_retries})")
                        time.sleep(wait_time)
                        continue
                    else:
                        error_msg = f"Rate limit достигнут после {max_retries} попыток"
                        logger.error(f"[AVITO PARSER] {error_msg}")
                        raise requests.exceptions.HTTPError(error_msg, response=response)
                
                response.raise_for_status()
                
                soup = BeautifulSoup(response.text, 'html.parser')
                
                details = {
                    'url': url,
                    'title': '',
                    'price': 0,
                    'description': '',
                    'images': [],
                    'seller': {},
                    'location': '',
                    'category': '',
                    'parameters': {}
                }
                
                # Парсим заголовок - пробуем разные варианты
                title_elem = soup.find('h1', class_=re.compile(r'title|item-title'))
                if not title_elem:
                    title_elem = soup.find('span', {'data-marker': re.compile(r'item-title|title')})
                if not title_elem:
                    title_elem = soup.find('h1')
                if title_elem:
                    details['title'] = title_elem.get_text(strip=True)
                    logger.info(f"[AVITO PARSER] Найден заголовок: {details['title'][:50]}")
                
                # Парсим цену - пробуем разные варианты
                price_elem = soup.find('span', class_=re.compile(r'price|item-price'))
                if not price_elem:
                    price_elem = soup.find('span', {'data-marker': re.compile(r'item-price|price')})
                if not price_elem:
                    price_elem = soup.find('div', class_=re.compile(r'price'))
                if not price_elem:
                    # Пробуем найти через itemprop="price"
                    price_elem = soup.find('meta', {'itemprop': 'price'})
                    if price_elem:
                        price_content = price_elem.get('content', '')
                        if price_content:
                            try:
                                details['price'] = float(price_content)
                                logger.info(f"[AVITO PARSER] Найдена цена через itemprop: {details['price']}")
                            except:
                                pass
                if price_elem and not details['price']:
                    price_text = price_elem.get_text(strip=True) if hasattr(price_elem, 'get_text') else str(price_elem.get('content', ''))
                    # Извлекаем число из текста (убираем все кроме цифр)
                    price_match = re.search(r'(\d[\d\s]*)', price_text.replace(' ', '').replace('\xa0', '').replace(',', ''))
                    if price_match:
                        try:
                            price_str = price_match.group(1).replace(' ', '').replace('\xa0', '').replace(',', '')
                            details['price'] = float(price_str)
                            logger.info(f"[AVITO PARSER] Найдена цена: {details['price']}")
                        except Exception as e:
                            logger.debug(f"[AVITO PARSER] Не удалось преобразовать цену '{price_text}': {e}")
                
                # Парсим описание - пробуем разные варианты
                desc_elem = soup.find('div', class_=re.compile(r'description|item-description'))
                if not desc_elem:
                    desc_elem = soup.find('div', {'data-marker': re.compile(r'item-description|description')})
                if not desc_elem:
                    desc_elem = soup.find('div', {'itemprop': 'description'})
                if desc_elem:
                    details['description'] = desc_elem.get_text(strip=True)
                    logger.info(f"[AVITO PARSER] Найдено описание: {len(details['description'])} символов")
                
                # Парсим изображения - пробуем разные варианты
                # Вариант 1: data-marker="gallery-image"
                img_elems = soup.find_all('img', {'data-marker': re.compile(r'gallery-image|photo')})
                if not img_elems:
                    # Вариант 2: классы с gallery/photo
                    img_elems = soup.find_all('img', class_=re.compile(r'gallery|photo|image'))
                if not img_elems:
                    # Вариант 3: все img в галерее
                    gallery = soup.find('div', class_=re.compile(r'gallery'))
                    if gallery:
                        img_elems = gallery.find_all('img')
                if not img_elems:
                    # Вариант 4: ищем в скриптах с JSON данными
                    scripts = soup.find_all('script', type='application/json')
                    for script in scripts:
                        try:
                            data = json.loads(script.string)
                            if isinstance(data, dict):
                                # Ищем изображения в разных местах JSON
                                if 'images' in data:
                                    images_data = data['images']
                                    if isinstance(images_data, list):
                                        for img_data in images_data:
                                            if isinstance(img_data, dict):
                                                img_url = img_data.get('url') or img_data.get('src') or img_data.get('full')
                                            elif isinstance(img_data, str):
                                                img_url = img_data
                                            else:
                                                continue
                                            if img_url and img_url not in details['images']:
                                                details['images'].append(img_url)
                                # Также пробуем найти в других полях
                                if 'photo' in data:
                                    photo_data = data['photo']
                                    if isinstance(photo_data, list):
                                        for photo in photo_data:
                                            img_url = photo.get('url') if isinstance(photo, dict) else str(photo) if photo else None
                                            if img_url and img_url not in details['images']:
                                                details['images'].append(img_url)
                        except:
                            continue
                
                for img in img_elems:
                    img_url = img.get('src') or img.get('data-src') or img.get('data-lazy-src') or img.get('data-url')
                    if img_url:
                        # Пропускаем маленькие иконки и placeholder'ы
                        if 'placeholder' in img_url.lower() or 'icon' in img_url.lower() or 'logo' in img_url.lower():
                            continue
                        if img_url.startswith('//'):
                            img_url = 'https:' + img_url
                        elif img_url.startswith('/'):
                            img_url = 'https://www.avito.ru' + img_url
                        # Убираем параметры размера из URL для получения оригинала
                        if '64x48' in img_url or '128x96' in img_url:
                            img_url = re.sub(r'_\d+x\d+', '', img_url)
                        if img_url not in details['images']:
                            details['images'].append(img_url)
                
                logger.info(f"[AVITO PARSER] Найдено изображений: {len(details['images'])}")
                
                # Парсим локацию
                location_elem = soup.find('span', class_=re.compile(r'location|address'))
                if not location_elem:
                    location_elem = soup.find('div', {'data-marker': re.compile(r'location|address')})
                if location_elem:
                    details['location'] = location_elem.get_text(strip=True)
                    logger.info(f"[AVITO PARSER] Найдена локация: {details['location']}")
                
                # Парсим категорию
                category_elem = soup.find('a', class_=re.compile(r'category|breadcrumb'))
                if category_elem:
                    details['category'] = category_elem.get_text(strip=True)
                
                # Парсим JSON данные из скриптов (если есть)
                scripts = soup.find_all('script', type='application/json')
                for script in scripts:
                    try:
                        data = json.loads(script.string)
                        # Ищем данные объявления в JSON
                        if isinstance(data, dict):
                            # Пробуем найти title, price, description в JSON
                            if not details['title'] and data.get('title'):
                                details['title'] = data['title']
                            if not details['price'] and data.get('price'):
                                details['price'] = data['price']
                            if not details['description'] and data.get('description'):
                                details['description'] = data['description']
                            if not details['images'] and data.get('images'):
                                details['images'] = data['images']
                    except:
                        continue
                
                logger.info(f"[AVITO PARSER] Парсинг завершен: title={bool(details['title'])}, price={bool(details['price'])}, "
                           f"description={bool(details['description'])}, images={len(details['images'])}")
                
                return details
                
            except requests.exceptions.HTTPError as http_error:
                last_exception = http_error
                if http_error.response and http_error.response.status_code == 429:
                    # 429 уже обработано выше, но если попали сюда - это последняя попытка
                    if attempt < max_retries - 1:
                        continue
                    else:
                        logger.error(f"[AVITO PARSER] Rate limit после {max_retries} попыток для {url}")
                        raise
                else:
                    # Другие HTTP ошибки
                    if attempt < max_retries - 1:
                        wait_time = (2 ** attempt) + 1
                        logger.warning(f"[AVITO PARSER] HTTP ошибка {http_error.response.status_code if http_error.response else 'unknown'}, повтор через {wait_time} сек...")
                        time.sleep(wait_time)
                        continue
                    else:
                        raise
            except Exception as e:
                last_exception = e
                if attempt < max_retries - 1:
                    wait_time = (2 ** attempt) + 1
                    logger.warning(f"[AVITO PARSER] Ошибка при запросе, повтор через {wait_time} сек...: {e}")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"[AVITO PARSER] ❌ Ошибка получения деталей объявления {url} после {max_retries} попыток: {e}", exc_info=True)
                    raise
        
        # Если все попытки исчерпаны
        if last_exception:
            raise last_exception
        
        # Возвращаем пустой словарь если что-то пошло не так
        logger.error(f"[AVITO PARSER] Не удалось получить данные после {max_retries} попыток")
        return {
            'url': url,
            'title': '',
            'price': 0,
            'description': '',
            'images': [],
            'location': '',
            'category': ''
        }

