#!/bin/bash
# Автоматическая установка OsaGaming CRM
# Запуск: bash install.sh

set -e  # Остановка при ошибке

echo "🚀 Начало установки OsaGaming CRM..."

# Цвета для вывода
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Проверка Python
echo -e "${YELLOW}Проверка Python...${NC}"
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}❌ Python3 не найден. Установите Python 3.8+${NC}"
    exit 1
fi
PYTHON_VERSION=$(python3 --version | cut -d' ' -f2 | cut -d'.' -f1,2)
echo -e "${GREEN}✅ Python $PYTHON_VERSION найден${NC}"

# Переход в директорию backend
cd "$(dirname "$0")"
cd backend

# Установка зависимостей
echo -e "${YELLOW}Установка зависимостей...${NC}"
if [ -d "../packages" ]; then
    pip3 install --target=../packages -r requirements.txt --quiet
    echo -e "${GREEN}✅ Зависимости установлены в packages/${NC}"
else
    pip3 install -r requirements.txt --quiet
    echo -e "${GREEN}✅ Зависимости установлены${NC}"
fi

# Создание .env если не существует
cd ..
if [ ! -f ".env" ]; then
    echo -e "${YELLOW}Создание .env файла...${NC}"
    cp backend/env.sample .env
    
    # Генерация SECRET_KEY
    SECRET_KEY=$(python3 -c "import secrets; print(secrets.token_urlsafe(32))" 2>/dev/null || echo "change_me_$(date +%s)")
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        sed -i '' "s|your_secret_key_here|$SECRET_KEY|" .env
    else
        # Linux
        sed -i "s|your_secret_key_here|$SECRET_KEY|" .env
    fi
    echo -e "${GREEN}✅ .env файл создан с автоматически сгенерированным SECRET_KEY${NC}"
    echo -e "${YELLOW}⚠️  ВАЖНО: Отредактируйте .env и заполните Avito API ключи!${NC}"
else
    echo -e "${GREEN}✅ .env файл уже существует${NC}"
fi

# Инициализация БД
echo -e "${YELLOW}Инициализация базы данных...${NC}"
cd backend
python3 -c "from database import init_database; init_database()" 2>/dev/null || {
    echo -e "${YELLOW}Попытка с альтернативным путем...${NC}"
    export PYTHONPATH="${PYTHONPATH}:$(pwd)"
    python3 -c "import sys; sys.path.insert(0, '.'); from database import init_database; init_database()"
}
echo -e "${GREEN}✅ База данных инициализирована${NC}"

# Настройка Avito API ключей
cd ..
echo -e "${YELLOW}Настройка Avito API ключей...${NC}"
python3 << 'PYTHON_SCRIPT'
import sys
import os

# Определяем путь к backend (в heredoc __file__ не определен, используем текущую директорию)
current_dir = os.getcwd()
backend_path = os.path.join(current_dir, 'backend')
if not os.path.exists(backend_path):
    # Если мы в backend, поднимаемся на уровень выше
    backend_path = os.path.join(os.path.dirname(current_dir), 'backend')
sys.path.insert(0, backend_path)

from database import get_db_connection

AVITO_SHOPS = [
    {
        'name': 'Авито магазин 1',
        'client_id': 'ofYBR53s8Ly_OA0GwwNP',
        'client_secret': 'xPPRcLcgz_cxC8tvCgLNJrRnJHveC_ecYf5FfISf',
        'user_id': '175670880',
        'shop_url': 'https://www.avito.ru/user175670880'
    },
    {
        'name': 'Авито магазин 2',
        'client_id': 'sc-e29jqn8wDIsFl45Tz',
        'client_secret': 'UbRFuCONaAJ1fBG97D3z-eqsug8GRA-7fL7SWkQc',
        'user_id': '400428996',
        'shop_url': 'https://www.avito.ru/user400428996'
    },
    {
        'name': 'Авито магазин 3',
        'client_id': '-d3AqUOD91DpfUoFcXRq',
        'client_secret': 'Q4pocQIlGmws3RNcvzf1r5DwdiVgq-nQi8AEK08o',
        'user_id': '324908408',
        'shop_url': 'https://www.avito.ru/user324908408'
    }
]

try:
    conn = get_db_connection()
    cursor = conn.cursor()
    
    for shop in AVITO_SHOPS:
        cursor.execute('SELECT id FROM avito_shops WHERE client_id = ?', (shop['client_id'],))
        existing = cursor.fetchone()
        
        if existing:
            cursor.execute('''
                UPDATE avito_shops 
                SET name = ?, client_secret = ?, user_id = ?, shop_url = ?, is_active = 1
                WHERE client_id = ?
            ''', (shop['name'], shop['client_secret'], shop['user_id'], shop['shop_url'], shop['client_id']))
            print(f"✅ Обновлен: {shop['name']} (ID: {existing[0]})")
        else:
            cursor.execute('''
                INSERT INTO avito_shops (name, shop_url, client_id, client_secret, user_id, is_active)
                VALUES (?, ?, ?, ?, ?, 1)
            ''', (shop['name'], shop['shop_url'], shop['client_id'], shop['client_secret'], shop['user_id']))
            print(f"✅ Добавлен: {shop['name']} (ID: {cursor.lastrowid})")
    
    conn.commit()
    conn.close()
    print(f"✅ Всего магазинов настроено: {len(AVITO_SHOPS)}")
except Exception as e:
    print(f"⚠️  Ошибка настройки ключей: {e}")
    sys.exit(0)  # Не останавливаем установку из-за этого
PYTHON_SCRIPT
echo -e "${GREEN}✅ Avito API ключи настроены${NC}"

# Проверка Redis (опционально)
echo -e "${YELLOW}Проверка Redis...${NC}"
if command -v redis-cli &> /dev/null; then
    if redis-cli ping &> /dev/null; then
        echo -e "${GREEN}✅ Redis работает${NC}"
    else
        echo -e "${YELLOW}⚠️  Redis установлен, но не запущен${NC}"
        echo -e "${YELLOW}   Запустите: sudo systemctl start redis${NC}"
    fi
else
    echo -e "${YELLOW}⚠️  Redis не установлен (опционально для кэширования)${NC}"
fi

# Создание директории tmp для Passenger
cd ..
mkdir -p tmp
touch tmp/restart.txt
echo -e "${GREEN}✅ Директория tmp создана${NC}"

# Проверка прав доступа
chmod 644 backend/*.py 2>/dev/null || true
chmod 755 backend/ 2>/dev/null || true
chmod 644 passenger_wsgi.py 2>/dev/null || true
chmod 600 .env 2>/dev/null || true

echo ""
echo -e "${GREEN}════════════════════════════════════════${NC}"
echo -e "${GREEN}✅ Установка завершена успешно!${NC}"
echo -e "${GREEN}════════════════════════════════════════${NC}"
echo ""
echo -e "${YELLOW}Следующие шаги:${NC}"
echo "1. Отредактируйте .env и заполните Avito API ключи"
echo "2. Перезапустите приложение (touch tmp/restart.txt)"
echo "3. Откройте http://yourdomain.com/login"
echo ""
echo -e "${YELLOW}Тестовые данные для входа:${NC}"
echo "   Админ: admin@osagaming.store / admin123"
echo "   Менеджер: dannnnnbb@gmail.com / manager123"
echo ""

