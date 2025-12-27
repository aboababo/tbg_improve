# Команды для перезагрузки Passenger на сервере

## 1. Быстрая перезагрузка через touch (рекомендуется)

```bash
# Подключитесь к серверу по SSH
ssh user@your-server.com

# Перейдите в директорию приложения
cd /var/www/tbg_improved  # или ваш путь к приложению

# Создайте/обновите файл restart.txt
touch tmp/restart.txt

# Или одной командой
touch /var/www/tbg_improved/tmp/restart.txt
```

## 2. Через passenger-config (если установлен)

```bash
# Перезагрузка конкретного приложения
passenger-config restart-app /var/www/tbg_improved

# Или с указанием пути к пассажиру
/usr/local/bin/passenger-config restart-app /var/www/tbg_improved
```

## 3. Через перезапуск веб-сервера

### Apache с Passenger:
```bash
sudo service apache2 restart
# или
sudo systemctl restart apache2
```

### Nginx с Passenger:
```bash
sudo service nginx restart
# или
sudo systemctl restart nginx
```

## 4. Через панель управления хостингом

Если используете shared hosting (cPanel, Plesk и т.д.):
- Войдите в панель управления
- Найдите раздел "Passenger" или "Application Restart"
- Нажмите кнопку "Restart Application"

## 5. Очистка кэша Python перед перезагрузкой

```bash
cd /var/www/tbg_improved

# Удаляем .pyc файлы
find . -type f -name "*.pyc" -delete
find . -type d -name "__pycache__" -exec rm -r {} +

# Затем перезагружаем
touch tmp/restart.txt
```

## 6. Полная перезагрузка (если touch не работает)

```bash
# Остановка Passenger
sudo passenger-config stop

# Очистка кэша
cd /var/www/tbg_improved
find . -type f -name "*.pyc" -delete
find . -type d -name "__pycache__" -exec rm -r {} +

# Запуск Passenger
sudo passenger-config start

# Или перезапуск веб-сервера
sudo service apache2 restart  # или nginx
```

## 7. Проверка статуса Passenger

```bash
# Проверка процессов Passenger
passenger-status

# Проверка конфигурации
passenger-config validate-install
```

## 8. Просмотр логов для диагностики

```bash
# Логи Passenger
tail -f /var/log/passenger.log

# Логи Apache
tail -f /var/log/apache2/error.log

# Логи Nginx
tail -f /var/log/nginx/error.log

# Логи приложения
tail -f /var/www/tbg_improved/logs/app.log
```
