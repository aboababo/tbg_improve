#!/bin/bash
# Автоматический запуск всех компонентов
# Запуск: bash start.sh

set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}🚀 Запуск OsaGaming CRM...${NC}"

cd "$(dirname "$0")"
cd backend

# Базовые проверки окружения
if ! command -v python3 &> /dev/null; then
    echo -e "${YELLOW}❌ Python3 не найден. Установите Python 3.9+${NC}"
    exit 1
fi

if ! python3 - <<'PY' >/dev/null 2>&1
import sys
sys.exit(0 if sys.version_info >= (3,11) else 1)
PY
then
    echo -e "${RED}❌ Требуется Python 3.11+${NC}"
    exit 1
fi

# Проверка pip / зависимостей
if ! python3 -m pip --version >/dev/null 2>&1; then
    echo -e "${YELLOW}⚠️  pip не найден. Установите pip перед запуском.${NC}"
    exit 1
fi

# Проверка .env
if [ ! -f "../.env" ]; then
    echo -e "${YELLOW}⚠️  .env не найден. Запустите: bash install.sh${NC}"
    exit 1
fi

# Запуск RQ Worker отключён (файл удалён). Если потребуется — верните worker и Redis.
echo -e "${YELLOW}RQ Worker пропущен (нет файла rq_worker.py)${NC}"

# Перезапуск Passenger приложения
cd ..
if [ -d "tmp" ]; then
    touch tmp/restart.txt
    echo -e "${GREEN}✅ Приложение перезапущено${NC}"
fi

echo -e "${GREEN}✅ Все компоненты запущены!${NC}"

