#!/bin/bash
# Безопасный скрипт для удаления неиспользуемых файлов на сервере
# Использование: 
#   bash cleanup_unused_files_safe.sh --dry-run  # Только показать, что будет удалено
#   bash cleanup_unused_files_safe.sh            # Реальное удаление

set -e  # Остановка при ошибке

# Проверяем режим dry-run
DRY_RUN=false
if [ "$1" == "--dry-run" ] || [ "$1" == "-n" ]; then
    DRY_RUN=true
    echo "⚠️  РЕЖИМ ПРОВЕРКИ (dry-run) - файлы НЕ будут удалены"
    echo ""
fi

echo "=========================================="
echo "Очистка неиспользуемых файлов на сервере"
if [ "$DRY_RUN" = true ]; then
    echo "РЕЖИМ: ПРОВЕРКА (файлы не удаляются)"
else
    echo "РЕЖИМ: УДАЛЕНИЕ"
fi
echo "=========================================="
echo ""

# Переходим в корневую директорию проекта
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Счетчики
deleted_count=0
error_count=0
not_found_count=0

# Функция для безопасного удаления файла (для конкретных файлов)
delete_file() {
    local file="$1"
    if [ -f "$file" ]; then
        if [ "$DRY_RUN" = true ]; then
            echo "🔍 Будет удален: $file"
            ((deleted_count++))
        else
            if rm -f "$file"; then
                echo "✅ Удален: $file"
                ((deleted_count++))
            else
                echo "❌ Ошибка удаления: $file"
                ((error_count++))
            fi
        fi
    else
        ((not_found_count++))
    fi
}

# Функция для удаления файлов по паттерну (оптимизированная - удаляет пакетами)
delete_pattern() {
    local pattern="$1"
    local description="$2"
    echo ""
    echo "--- Удаление $description ---"
    
    # Используем временный файл для подсчета (избегаем проблем с подсчетом в while loop)
    local temp_file=$(mktemp)
    
    if [ "$DRY_RUN" = true ]; then
        # В режиме проверки показываем все файлы
        find . -type f -name "$pattern" -not -path "./.git/*" -not -path "./__pycache__/*" -not -path "./node_modules/*" -not -path "./tmp/*" -not -path "./data/*" -not -path "./database_backups/*" 2>/dev/null | while read -r file; do
            echo "🔍 Будет удален: $file"
            echo "1" >> "$temp_file"
        done
        local count=$(wc -l < "$temp_file" 2>/dev/null || echo "0")
        deleted_count=$((deleted_count + count))
    else
        # Удаляем все файлы одной командой через find -delete (самый быстрый способ)
        local count_before=$(find . -type f -name "$pattern" -not -path "./.git/*" -not -path "./__pycache__/*" -not -path "./node_modules/*" -not -path "./tmp/*" -not -path "./data/*" -not -path "./database_backups/*" 2>/dev/null | wc -l)
        
        if [ "$count_before" -eq 0 ]; then
            echo "ℹ️  Файлы не найдены"
            rm -f "$temp_file"
            return
        fi
        
        echo "Найдено файлов: $count_before"
        
        # Удаляем через find -delete (самый быстрый способ)
        find . -type f -name "$pattern" -not -path "./.git/*" -not -path "./__pycache__/*" -not -path "./node_modules/*" -not -path "./tmp/*" -not -path "./data/*" -not -path "./database_backups/*" -delete 2>/dev/null
        
        echo "✅ Удалено: $count_before файлов"
        deleted_count=$((deleted_count + count_before))
    fi
    
    rm -f "$temp_file"
}

# Функция для пакетного удаления массива файлов
delete_files_array() {
    local files_array_name="$1"
    local description="$2"
    
    # Получаем массив по имени
    eval "local files=(\"\${${files_array_name}[@]}\")"
    
    if [ ${#files[@]} -eq 0 ]; then
        return
    fi
    
    if [ "$DRY_RUN" = true ]; then
        for file in "${files[@]}"; do
            delete_file "$file"
        done
    else
        # Удаляем все файлы одной командой через xargs (параллельно, 4 процесса)
        printf '%s\0' "${files[@]}" | xargs -0 -P 4 -I {} sh -c 'rm -f "{}" 2>/dev/null' || true
        
        # Подсчитываем результаты
        local deleted=0
        local errors=0
        for file in "${files[@]}"; do
            if [ ! -f "$file" ]; then
                ((deleted++))
            else
                ((errors++))
            fi
        done
        
        deleted_count=$((deleted_count + deleted))
        error_count=$((error_count + errors))
        
        echo "✅ Удалено: $deleted файлов"
        if [ "$errors" -gt 0 ]; then
            echo "⚠️  Ошибок: $errors файлов"
        fi
    fi
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
delete_pattern "find_*.py" "временных скриптов (find)"
delete_pattern "load_*.py" "временных скриптов (load)"
delete_pattern "recreate_*.py" "временных скриптов (recreate)"
delete_pattern "setup_*.py" "временных скриптов (setup)"

# Удаляем конкретные файлы из корня (пакетно)
echo ""
echo "--- Удаление конкретных файлов из корня ---"
ROOT_FILES=(
    "apply_correct_keys.py"
    "apply_migration.py"
    "backup_old_database.py"
    "find_correct_key_combinations.py"
    "find_old_database.py"
    "load_all_messages.py"
    "recreate_database.py"
    "remove_old_database.py"
    "remove_old_database_simple.py"
    "setup_avito_keys.py"
    "update_shop_keys.py"
    "diagnose_database_error.py"
    "CHECK_AVITO_API.py"
    "check_all_shops_sync.py"
    "check_avito_sync.py"
    "check_auto_sync.py"
    "check_chat_listing.py"
    "check_chats_detailed.py"
    "check_chats_in_db.py"
    "check_code_updates.py"
    "check_db.py"
    "check_keys_matching.py"
    "check_old_database.py"
    "check_shop1_history.py"
    "check_sync_details.py"
    "test_all.py"
    "test_api_endpoints.py"
    "TEST_APP.py"
    "test_avito_api.py"
    "test_avito_health.py"
    "test_avito_keys.py"
    "test_chats_query.py"
    "test_database_fix.py"
    "test_endpoints.py"
    "test_key_combinations.py"
    "test_v3_endpoint.py"
    "test_webhook.py"
    "TEST_ONE_CHAT.py"
)
delete_files_array "ROOT_FILES" "файлов из корня"

# Удаляем файлы из backend/ (пакетно)
echo ""
echo "--- Удаление файлов из backend/ ---"
BACKEND_FILES=(
    "backend/add_chat_listing_cache_table.py"
    "backend/add_tab_visibility_settings.py"
    "backend/analyze_avito_api_response.py"
    "backend/check_avito_api_compliance.py"
    "backend/check_chats_db.py"
    "backend/check_chats.py"
    "backend/check_db_status.py"
    "backend/check_item_id_extraction.py"
    "backend/check_listing_owner_from_chat.py"
    "backend/check_listing_ownership.py"
    "backend/check_real_chat_count.py"
    "backend/check_user_id_from_token.py"
    "backend/clean_incomplete_listings_cache.py"
    "backend/clear_all_chats_messages_listings.py"
    "backend/clear_listing_cache.py"
    "backend/clear_listings_cache.py"
    "backend/clear_listings_db.py"
    "backend/debug_sync_issue.py"
    "backend/diagnose_listing_loading.py"
    "backend/diagnose_sync_issue.py"
    "backend/fix_missing_product_urls.py"
    "backend/reload_all_listings.py"
    "backend/reset_admin_password.py"
    "backend/update_avito_keys.py"
    "backend/view_logs.py"
    "backend/database_new.py"
    "backend/test_api_chats_response.py"
    "backend/test_api_chats.py"
    "backend/test_avito_api_response.py"
    "backend/test_avito_chats_flow.py"
    "backend/test_chats_api.py"
    "backend/test_listing_with_params.py"
)

delete_files_array "BACKEND_FILES" "файлов из backend/"

# ========== ВРЕМЕННЫЕ .SH СКРИПТЫ ==========
echo ""
echo "--- Удаление временных .sh скриптов ---"
SH_FILES=(
    "CHECK_AFTER_RESTART.sh"
    "CHECK_AND_FIX.sh"
    "CHECK_APP.sh"
    "CHECK_CURRENT_ERRORS.sh"
    "CHECK_ENDPOINTS.sh"
    "CHECK_ERRORS.sh"
    "CHECK_ERRORS_DETAILED.sh"
    "CHECK_EXTRACT_ENDPOINT.sh"
    "CHECK_FILE_VERSION.sh"
    "CHECK_LISTINGS_IN_CHATS.sh"
    "CHECK_LOGIN.sh"
    "CHECK_LOGIN_ERROR.sh"
    "CHECK_LOGS.sh"
    "CHECK_PASSENGER_ERRORS.sh"
    "CHECK_PRODUCT_URLS.sh"
    "CHECK_ROOT_APP_PY.sh"
    "CHECK_SYNC_LOGS.sh"
    "DEBUG_500_ERROR.sh"
    "DEBUG_EXTRACT.sh"
    "DEBUG_REAL_ERROR.sh"
    "DIAGNOSE_LISTINGS_ISSUE.sh"
    "DIAGNOSE_PRODUCT_URL.sh"
    "EXTRACT_ALL_PRODUCT_URLS.sh"
    "EXTRACT_ALL_PRODUCT_URLS_BATCH.sh"
    "EXTRACT_QUICK.sh"
    "EXTRACT_WITHOUT_SESSION.sh"
    "FINAL_FIX.sh"
    "FIX_ALL.sh"
    "FIX_DATABASE_COMPLETE.sh"
    "FIX_LINE_ENDINGS.sh"
    "FIX_LISTINGS_COMPLETE.sh"
    "FIX_LISTINGS_SIMPLE.sh"
    "FIX_LOGIN.sh"
    "FIX_TYPING_CONFLICT.sh"
    "FORCE_RESTART.sh"
    "FORCE_UPDATE_SERVER.sh"
    "INSTALL_FLASK.sh"
    "QUICK_TEST.sh"
    "REINSTALL_FLASK.sh"
    "restart_app.sh"
    "restart_passenger_simple.sh"
    "restart_quick.sh"
    "restart_server.sh"
    "restart_simple.sh"
    "restart_with_cache_clear.sh"
    "fix_restart_script.sh"
    "fix_server_database.sh"
    "fix_server_database_simple.sh"
    "test_server_database.sh"
    "test_webhook.sh"
    "TEST_AUTH.sh"
    "TEST_PASSENGER_IMPORT.sh"
    "TEST_REAL_REQUEST.sh"
    "VERIFY_AND_RESTART.sh"
    "backend/restart.sh"
    "backend/check_logs.sh"
)

delete_files_array "SH_FILES" "временных .sh скриптов"

# ========== ВРЕМЕННЫЕ .MD ФАЙЛЫ ==========
echo ""
echo "--- Удаление временных .md файлов ---"
MD_FILES=(
    "AUTO_UPDATE_SETTINGS.md"
    "AUTO_UPDATE_SUMMARY.md"
    "CHECK_WEBHOOK.md"
    "DATABASE_FIX_README.md"
    "DEBUG_INTERNAL_ERROR.md"
    "EXTRACT_WITHOUT_SESSION.md"
    "FIX_500_404_ERRORS.md"
    "FIX_EXTRACT_ENDPOINT.md"
    "FIX_LISTINGS_FINAL.md"
    "FIX_RESTART_SCRIPT.md"
    "FIX_SCRIPT_ON_SERVER.md"
    "FINAL_DATABASE_FIX.md"
    "GET_SESSION_COOKIE.md"
    "IMPROVEMENTS.md"
    "INSTALL_DATABASE.md"
    "INSTALL_DEPENDENCIES.md"
    "КОПИРОВАТЬ_НА_СЕРВЕР.md"
    "QUICK_FIX.md"
    "register_webhook_osagaming.md"
    "RESTART_INSTRUCTIONS.md"
    "RESTART_PASSENGER.md"
    "SERVER_DATABASE_FIX.md"
    "sync_chats_instructions.md"
    "WEBHOOK_SIMPLE_GUIDE.md"
    "ИСПРАВЛЕНИЕ_ВХОДА.md"
    "ИСПРАВЛЕНИЕ_ДИЗАЙНА.md"
    "ПОЛНОЕ_ИСПРАВЛЕНИЕ_ДИЗАЙНА.md"
    "РЕШЕНИЕ_ПРОБЛЕМЫ.md"
    "COPY_DATABASE_FILES.md"
    "backend/AVITO_API_ANALYSIS.md"
    "backend/AVITO_API_COMPLIANCE_REPORT.md"
    "backend/where_are_logs.md"
    "AUTO_SYNC_AND_WEBHOOKS.md"
)

delete_files_array "MD_FILES" "временных .md файлов"

# ========== ВРЕМЕННЫЕ .BAT/.PS1/.TXT ФАЙЛЫ ==========
echo ""
echo "--- Удаление временных .bat/.ps1/.txt файлов ---"
OTHER_FILES=(
    "restart_with_cache_clear.bat"
    "restart_passenger.bat"
    "backend/restart.bat"
    "backend/start_server.bat"
    "restart.ps1"
    "restart_with_cache_clear.ps1"
    "backend/check_logs.ps1"
    "restart_commands.txt"
    "backend/restart_command.txt"
    "backend/restart.txt"
)

delete_files_array "OTHER_FILES" "временных .bat/.ps1/.txt файлов"

# Итоги
echo ""
echo "=========================================="
if [ "$DRY_RUN" = true ]; then
    echo "ПРОВЕРКА ЗАВЕРШЕНА (файлы НЕ удалены)"
else
    echo "ОЧИСТКА ЗАВЕРШЕНА!"
fi
echo "=========================================="
echo "Найдено/удалено файлов: $deleted_count"
echo "Ошибок: $error_count"
echo "Не найдено: $not_found_count"
echo ""
if [ "$DRY_RUN" = true ]; then
    echo "⚠️  Это был режим проверки. Для реального удаления запустите:"
    echo "   bash cleanup_unused_files_safe.sh"
    echo ""
else
    echo "✅ Файлы успешно удалены!"
    echo ""
fi
echo "📋 Сохранены важные файлы:"
echo "  ✓ README.md"
echo "  ✓ WEBHOOK_SETUP_GUIDE.md"
echo "  ✓ backend/requirements.txt"
echo "  ✓ Основные скрипты запуска (start.sh, stop.sh, restart.sh, install.sh)"
echo "  ✓ Скрипты автосинхронизации (start_auto_sync.sh, stop_auto_sync.sh, start_sync_forever.sh, stop_sync.sh)"
echo "  ✓ passenger_wsgi.py"
echo ""

