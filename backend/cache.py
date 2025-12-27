"""
OsaGaming CRM - Модуль кэширования
==================================

Кэширование для оптимизации производительности:
- Кэширование частых запросов к БД
- TTL (Time To Live) для автоматической инвалидации
- Оптимизация повторяющихся запросов
- Поддержка Redis с fallback на in-memory

Автор: OsaGaming Development Team
Версия: 3.0
"""

# Импортируем из нового модуля Redis кэширования
from cache_redis import (
    get_cached,
    set_cached,
    invalidate_cache,
    cached,
    get_cache_key,
    get_redis_client,
    CACHE_TTL
)

# Обратная совместимость - экспортируем те же функции
__all__ = [
    'get_cached',
    'set_cached',
    'invalidate_cache',
    'cached',
    'get_cache_key',
    'get_redis_client',
    'CACHE_TTL'
]

