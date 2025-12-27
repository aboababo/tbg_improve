#!/bin/bash
# Остановка автосинхронизации

cd "$(dirname "$0")"

if [ -f sync.pid ]; then
    PID=$(cat sync.pid)
    if ps -p $PID > /dev/null 2>&1; then
        echo "Останавливаем процесс (PID: $PID)..."
        kill $PID
        sleep 2
        
        if ps -p $PID > /dev/null 2>&1; then
            kill -9 $PID
        fi
        
        echo "Процесс остановлен"
    else
        echo "Процесс не запущен"
    fi
    rm sync.pid
else
    echo "PID файл не найден"
fi

