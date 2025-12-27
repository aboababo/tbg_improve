#!/bin/bash
# Скрипт для остановки автосинхронизации

cd "$(dirname "$0")"

if [ -f auto_sync.pid ]; then
    PID=$(cat auto_sync.pid)
    if ps -p $PID > /dev/null 2>&1; then
        echo "Останавливаем процесс (PID: $PID)..."
        kill $PID
        sleep 2
        
        # Проверяем, остановился ли процесс
        if ps -p $PID > /dev/null 2>&1; then
            echo "Принудительная остановка..."
            kill -9 $PID
        fi
        
        echo "Процесс остановлен"
    else
        echo "Процесс не запущен"
    fi
    rm auto_sync.pid
else
    echo "PID файл не найден"
fi

