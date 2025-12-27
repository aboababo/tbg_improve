#!/bin/bash
# Запуск автосинхронизации в бесконечном цикле

cd "$(dirname "$0")"

echo "Запуск автосинхронизации..."
echo "Логи: tail -f sync_forever.log"

# Останавливаем старый процесс
if [ -f sync.pid ]; then
    OLD_PID=$(cat sync.pid)
    if ps -p $OLD_PID > /dev/null 2>&1; then
        kill $OLD_PID 2>/dev/null
        sleep 1
    fi
fi

# Запускаем
nohup python3 backend/auto_sync.py >> sync_forever.log 2>&1 &
echo $! > sync.pid

echo "Процесс запущен (PID: $(cat sync.pid))"

