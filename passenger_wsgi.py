# -*- coding: utf-8 -*-
# Passenger WSGI файл для Beget
# ИСПОЛЬЗУЕТ ЛОКАЛЬНЫЕ ПАКЕТЫ из директории packages/

import sys
import os

# Получаем абсолютный путь к корневой директории проекта
project_root = os.path.dirname(os.path.abspath(__file__))
backend_path = os.path.join(project_root, 'backend')
packages_path = os.path.join(project_root, 'packages')

# ВАЖНО: Добавляем локальные пакеты, но НЕ в начало sys.path
# Это предотвращает конфликты со встроенными модулями Python (typing, json и др.)
# Встроенные модули должны иметь приоритет
if os.path.exists(packages_path):
    # Добавляем packages в конец sys.path, чтобы встроенные модули имели приоритет
    sys.path.append(packages_path)
    # Также добавляем все поддиректории packages (для namespace пакетов)
    for root, dirs, files in os.walk(packages_path):
        if '__init__.py' in files or any(f.endswith('.pth') for f in files):
            if root not in sys.path:
                sys.path.append(root)

# Добавляем пути проекта
sys.path.insert(0, backend_path)
sys.path.insert(0, project_root)

# Сохраняем текущую рабочую директорию
original_cwd = os.getcwd()

# Меняем рабочую директорию на backend
os.chdir(backend_path)

# Импортируем приложение Flask
try:
    from app import app as application
    application.debug = False
except Exception as e:
    # Логируем любую ошибку (не только ImportError)
    os.chdir(original_cwd)
    import traceback
    error_msg = f"""
ОШИБКА ПРИ ИМПОРТЕ APP:
{e}

ТИП ОШИБКИ: {type(e).__name__}

ПУТИ PYTHON (sys.path):
{chr(10).join(f'  {p}' for p in sys.path)}

ТЕКУЩАЯ ДИРЕКТОРИЯ: {os.getcwd()}
PROJECT ROOT: {project_root}
PACKAGES PATH: {packages_path}
PACKAGES EXISTS: {os.path.exists(packages_path)}
FLASK EXISTS: {os.path.exists(os.path.join(packages_path, 'flask')) if packages_path else False}
PYTHON EXECUTABLE: {sys.executable}

TRACEBACK:
{traceback.format_exc()}

Проверьте:
1. Запустите: bash INSTALL_AND_FIX.sh
2. Проверьте наличие: ls -la {packages_path}/flask
3. Проверьте синтаксис: python3 -m py_compile backend/app.py
"""
    # Выводим в stderr для логирования Passenger
    import sys
    sys.stderr.write(error_msg)
    sys.stderr.flush()
    raise

# ВАЖНО: НЕ возвращаем рабочую директорию обратно!
# Flask должен работать с рабочей директорией в backend
# для правильного поиска шаблонов и статических файлов
# os.chdir(original_cwd)  # Закомментировано для исправления ошибки 500
