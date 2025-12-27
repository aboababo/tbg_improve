#!/bin/bash
# Скрипт для запуска автосинхронизации в фоне

cd "$(dirname "$0")"

# Проверка Python
if ! command -v python3 &> /dev/null; then
    echo "Python3 не найден. Установите Python 3.11+"
    exit 1
fi

# Проверка .env (в корне или на уровень выше)
if [ ! -f ".env" ] && [ ! -f "../.env" ]; then
    echo ".env не найден. Запустите install.sh и повторите попытку."
    exit 1
fi

# Остановка старого процесса если есть
if [ -f auto_sync.pid ]; then
    OLD_PID=$(cat auto_sync.pid)
    if ps -p $OLD_PID > /dev/null 2>&1; then
        echo "Останавливаем старый процесс (PID: $OLD_PID)..."
        kill $OLD_PID
        sleep 2
    fi
    rm auto_sync.pid
fi

# Запуск нового процесса
echo "Запуск автосинхронизации..."
nohup python3 backend/auto_sync.py >> auto_sync.log 2>&1 &
echo $! > auto_sync.pid

echo "Автосинхронизация запущена (PID: $(cat auto_sync.pid))"
echo "Логи: tail -f auto_sync.log"

