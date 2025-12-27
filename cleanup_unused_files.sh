#!/bin/bash
# Скрипт для удаления неиспользуемых файлов на сервере
# Удаляет те же файлы, которые были удалены локально

set -e  # Остановка при ошибке

echo "=========================================="
echo "Очистка неиспользуемых файлов на сервере"
echo "=========================================="
echo ""

# Переходим в корневую директорию проекта
cd "$(dirname "$0")"

# Счетчики
deleted_count=0
error_count=0

# Функция для безопасного удаления файла
delete_file() {
    local file="$1"
    if [ -f "$file" ]; then
        if rm -f "$file"; then
            echo "✅ Удален: $file"
            ((deleted_count++))
        else
            echo "❌ Ошибка удаления: $file"
            ((error_count++))
        fi
    else
        echo "ℹ️  Файл не найден (уже удален?): $file"
    fi
}

# Функция для удаления файлов по паттерну
delete_pattern() {
    local pattern="$1"
    local description="$2"
    echo ""
    echo "--- Удаление $description ---"
    find . -type f -name "$pattern" -not -path "./.git/*" -not -path "./__pycache__/*" -not -path "./node_modules/*" | while read -r file; do
        delete_file "$file"
    done
}

echo "Начинаем удаление неиспользуемых файлов..."
echo ""

# ========== ТЕСТОВЫЕ ФАЙЛЫ ==========
delete_pattern "test_*.py" "тестовых Python файлов"
delete_pattern "TEST_*.py" "тестовых Python файлов (заглавные)"

# ========== ДИАГНОСТИЧЕСКИЕ ФАЙЛЫ ==========
delete_pattern "check_*.py" "диагностических файлов (check)"
delete_pattern "diagnose_*.py" "диагностических файлов (diagnose)"
delete_pattern "debug_*.py" "диагностических файлов (debug)"
delete_pattern "analyze_*.py" "диагностических файлов (analyze)"

# ========== ВРЕМЕННЫЕ СКРИПТЫ ==========
delete_pattern "fix_*.py" "временных скриптов (fix)"
delete_pattern "clear_*.py" "временных скриптов (clear)"
delete_pattern "remove_*.py" "временных скриптов (remove)"
delete_pattern "backup_*.py" "временных скриптов (backup)"
delete_pattern "apply_*.py" "временных скриптов (apply)"
delete_pattern "update_*.py" "временных скриптов (update) - кроме update_avito_keys.py"
delete_pattern "find_*.py" "временных скриптов (find)"
delete_pattern "load_*.py" "временных скриптов (load)"
delete_pattern "recreate_*.py" "временных скриптов (recreate)"
delete_pattern "setup_*.py" "временных скриптов (setup)"

# Удаляем конкретные файлы из корня
echo ""
echo "--- Удаление конкретных файлов из корня ---"
delete_file "apply_correct_keys.py"
delete_file "apply_migration.py"
delete_file "backup_old_database.py"
delete_file "find_correct_key_combinations.py"
delete_file "find_old_database.py"
delete_file "load_all_messages.py"
delete_file "recreate_database.py"
delete_file "remove_old_database.py"
delete_file "remove_old_database_simple.py"
delete_file "setup_avito_keys.py"
delete_file "update_shop_keys.py"
delete_file "diagnose_database_error.py"
delete_file "CHECK_AVITO_API.py"

# Удаляем файлы из backend/
echo ""
echo "--- Удаление файлов из backend/ ---"
delete_file "backend/add_chat_listing_cache_table.py"
delete_file "backend/add_tab_visibility_settings.py"
delete_file "backend/analyze_avito_api_response.py"
delete_file "backend/check_avito_api_compliance.py"
delete_file "backend/check_chats_db.py"
delete_file "backend/check_chats.py"
delete_file "backend/check_db_status.py"
delete_file "backend/check_item_id_extraction.py"
delete_file "backend/check_listing_owner_from_chat.py"
delete_file "backend/check_listing_ownership.py"
delete_file "backend/check_real_chat_count.py"
delete_file "backend/check_user_id_from_token.py"
delete_file "backend/clean_incomplete_listings_cache.py"
delete_file "backend/clear_all_chats_messages_listings.py"
delete_file "backend/clear_listing_cache.py"
delete_file "backend/clear_listings_cache.py"
delete_file "backend/clear_listings_db.py"
delete_file "backend/debug_sync_issue.py"
delete_file "backend/diagnose_listing_loading.py"
delete_file "backend/diagnose_sync_issue.py"
delete_file "backend/fix_missing_product_urls.py"
delete_file "backend/reload_all_listings.py"
delete_file "backend/reset_admin_password.py"
delete_file "backend/update_avito_keys.py"
delete_file "backend/view_logs.py"
delete_file "backend/database_new.py"

# ========== ВРЕМЕННЫЕ .SH СКРИПТЫ ==========
echo ""
echo "--- Удаление временных .sh скриптов ---"
delete_file "CHECK_AFTER_RESTART.sh"
delete_file "CHECK_AND_FIX.sh"
delete_file "CHECK_APP.sh"
delete_file "CHECK_CURRENT_ERRORS.sh"
delete_file "CHECK_ENDPOINTS.sh"
delete_file "CHECK_ERRORS.sh"
delete_file "CHECK_ERRORS_DETAILED.sh"
delete_file "CHECK_EXTRACT_ENDPOINT.sh"
delete_file "CHECK_FILE_VERSION.sh"
delete_file "CHECK_LISTINGS_IN_CHATS.sh"
delete_file "CHECK_LOGIN.sh"
delete_file "CHECK_LOGIN_ERROR.sh"
delete_file "CHECK_LOGS.sh"
delete_file "CHECK_PASSENGER_ERRORS.sh"
delete_file "CHECK_PRODUCT_URLS.sh"
delete_file "CHECK_ROOT_APP_PY.sh"
delete_file "CHECK_SYNC_LOGS.sh"
delete_file "DEBUG_500_ERROR.sh"
delete_file "DEBUG_EXTRACT.sh"
delete_file "DEBUG_REAL_ERROR.sh"
delete_file "DIAGNOSE_LISTINGS_ISSUE.sh"
delete_file "DIAGNOSE_PRODUCT_URL.sh"
delete_file "EXTRACT_ALL_PRODUCT_URLS.sh"
delete_file "EXTRACT_ALL_PRODUCT_URLS_BATCH.sh"
delete_file "EXTRACT_QUICK.sh"
delete_file "EXTRACT_WITHOUT_SESSION.sh"
delete_file "FINAL_FIX.sh"
delete_file "FIX_ALL.sh"
delete_file "FIX_DATABASE_COMPLETE.sh"
delete_file "FIX_LINE_ENDINGS.sh"
delete_file "FIX_LISTINGS_COMPLETE.sh"
delete_file "FIX_LISTINGS_SIMPLE.sh"
delete_file "FIX_LOGIN.sh"
delete_file "FIX_TYPING_CONFLICT.sh"
delete_file "FORCE_RESTART.sh"
delete_file "FORCE_UPDATE_SERVER.sh"
delete_file "INSTALL_FLASK.sh"
delete_file "QUICK_TEST.sh"
delete_file "REINSTALL_FLASK.sh"
delete_file "restart_app.sh"
delete_file "restart_passenger_simple.sh"
delete_file "restart_quick.sh"
delete_file "restart_server.sh"
delete_file "restart_simple.sh"
delete_file "restart_with_cache_clear.sh"
delete_file "fix_restart_script.sh"
delete_file "fix_server_database.sh"
delete_file "fix_server_database_simple.sh"
delete_file "test_server_database.sh"
delete_file "test_webhook.sh"
delete_file "TEST_AUTH.sh"
delete_file "TEST_PASSENGER_IMPORT.sh"
delete_file "TEST_REAL_REQUEST.sh"
delete_file "VERIFY_AND_RESTART.sh"
delete_file "backend/restart.sh"
delete_file "backend/check_logs.sh"

# ========== ВРЕМЕННЫЕ .MD ФАЙЛЫ ==========
echo ""
echo "--- Удаление временных .md файлов ---"
delete_file "AUTO_UPDATE_SETTINGS.md"
delete_file "AUTO_UPDATE_SUMMARY.md"
delete_file "CHECK_WEBHOOK.md"
delete_file "DATABASE_FIX_README.md"
delete_file "DEBUG_INTERNAL_ERROR.md"
delete_file "EXTRACT_WITHOUT_SESSION.md"
delete_file "FIX_500_404_ERRORS.md"
delete_file "FIX_EXTRACT_ENDPOINT.md"
delete_file "FIX_LISTINGS_FINAL.md"
delete_file "FIX_RESTART_SCRIPT.md"
delete_file "FIX_SCRIPT_ON_SERVER.md"
delete_file "FINAL_DATABASE_FIX.md"
delete_file "GET_SESSION_COOKIE.md"
delete_file "IMPROVEMENTS.md"
delete_file "INSTALL_DATABASE.md"
delete_file "INSTALL_DEPENDENCIES.md"
delete_file "КОПИРОВАТЬ_НА_СЕРВЕР.md"
delete_file "QUICK_FIX.md"
delete_file "register_webhook_osagaming.md"
delete_file "RESTART_INSTRUCTIONS.md"
delete_file "RESTART_PASSENGER.md"
delete_file "SERVER_DATABASE_FIX.md"
delete_file "sync_chats_instructions.md"
delete_file "WEBHOOK_SIMPLE_GUIDE.md"
delete_file "ИСПРАВЛЕНИЕ_ВХОДА.md"
delete_file "ИСПРАВЛЕНИЕ_ДИЗАЙНА.md"
delete_file "ПОЛНОЕ_ИСПРАВЛЕНИЕ_ДИЗАЙНА.md"
delete_file "РЕШЕНИЕ_ПРОБЛЕМЫ.md"
delete_file "COPY_DATABASE_FILES.md"
delete_file "backend/AVITO_API_ANALYSIS.md"
delete_file "backend/AVITO_API_COMPLIANCE_REPORT.md"
delete_file "backend/where_are_logs.md"
delete_file "AUTO_SYNC_AND_WEBHOOKS.md"

# ========== ВРЕМЕННЫЕ .BAT/.PS1/.TXT ФАЙЛЫ ==========
echo ""
echo "--- Удаление временных .bat/.ps1/.txt файлов ---"
delete_file "restart_with_cache_clear.bat"
delete_file "restart_passenger.bat"
delete_file "backend/restart.bat"
delete_file "backend/start_server.bat"
delete_file "restart.ps1"
delete_file "restart_with_cache_clear.ps1"
delete_file "backend/check_logs.ps1"
delete_file "restart_commands.txt"
delete_file "backend/restart_command.txt"
delete_file "backend/restart.txt"

# Итоги
echo ""
echo "=========================================="
echo "Очистка завершена!"
echo "=========================================="
echo "Удалено файлов: $deleted_count"
echo "Ошибок: $error_count"
echo ""
echo "⚠️  ВАЖНО: Проверьте, что важные файлы не были удалены!"
echo "Сохранены:"
echo "  - README.md"
echo "  - WEBHOOK_SETUP_GUIDE.md"
echo "  - backend/requirements.txt"
echo "  - Основные скрипты запуска (start.sh, stop.sh, restart.sh, install.sh)"
echo "  - Скрипты автосинхронизации (start_auto_sync.sh, stop_auto_sync.sh)"
echo ""

