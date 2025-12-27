#!/bin/bash
# Скрипт для перезагрузки Passenger приложения

echo "Перезагрузка Passenger приложения..."

# Создаем директорию tmp если её нет
mkdir -p tmp

# Обновляем файл restart.txt
touch tmp/restart.txt

echo "✅ Файл tmp/restart.txt обновлен"
echo ""
echo "Приложение будет перезагружено при следующем запросе."
echo "Или выполните запрос к любому endpoint для немедленной перезагрузки."
