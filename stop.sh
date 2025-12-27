#!/bin/bash
# Остановка всех компонентов
# Запуск: bash stop.sh

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}🛑 Остановка компонентов...${NC}"

cd "$(dirname "$0")"
cd backend

# Остановка RQ Worker
if [ -f "rq_worker.pid" ]; then
    PID=$(cat rq_worker.pid)
    if ps -p $PID > /dev/null 2>&1; then
        kill $PID
        rm rq_worker.pid
        echo -e "${GREEN}✅ RQ Worker остановлен${NC}"
    else
        rm rq_worker.pid
        echo -e "${YELLOW}⚠️  RQ Worker уже остановлен${NC}"
    fi
else
    echo -e "${YELLOW}⚠️  RQ Worker не запущен${NC}"
fi

echo -e "${GREEN}✅ Остановка завершена${NC}"

