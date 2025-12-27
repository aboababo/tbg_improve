/**
 * OsaGaming CRM - Real-time обновления
 * ====================================
 * 
 * WebSocket или polling для обновлений в реальном времени
 */

class RealtimeUpdates {
    constructor() {
        this.pollingInterval = null;
        this.lastUpdate = Date.now();
        this.callbacks = new Map();
        this.shownNotifications = new Set(); // Для дедупликации уведомлений
        this.init();
    }

    init() {
        // Используем polling для обновлений (можно заменить на WebSocket)
        this.startPolling();
        
        // Останавливаем polling когда страница неактивна
        document.addEventListener('visibilitychange', () => {
            if (document.hidden) {
                this.stopPolling();
            } else {
                this.startPolling();
            }
        });
    }

    startPolling() {
        if (this.pollingInterval) return;
        
        this.pollingInterval = setInterval(() => {
            this.checkUpdates();
        }, 5000); // Проверка каждые 5 секунд
    }

    stopPolling() {
        if (this.pollingInterval) {
            clearInterval(this.pollingInterval);
            this.pollingInterval = null;
        }
    }

    async checkUpdates() {
        try {
            // Проверяем новые уведомления
            const response = await fetch('/api/notifications');
            if (response.ok) {
                const notifications = await response.json();
                this.handleNotifications(notifications);
            }

            // Проверяем обновления чатов
            const chatsResponse = await fetch('/api/chats?updated_since=' + this.lastUpdate);
            if (chatsResponse.ok) {
                const chats = await chatsResponse.json();
                if (chats.length > 0) {
                    this.trigger('chats_updated', chats);
                }
            }

            this.lastUpdate = Date.now();
        } catch (error) {
            console.error('Ошибка проверки обновлений:', error);
        }
    }

    handleNotifications(notifications) {
        if (!notifications || !Array.isArray(notifications)) {
            return;
        }
        
        notifications.forEach(notification => {
            // Создаем уникальный ключ для дедупликации
            const notificationKey = `${notification.message}_${notification.type || 'info'}`;
            
            // Пропускаем уже показанные уведомления
            if (this.shownNotifications.has(notificationKey)) {
                return;
            }
            
            // Отмечаем как показанное
            this.shownNotifications.add(notificationKey);
            
            // Очищаем старые уведомления через 5 минут
            setTimeout(() => {
                this.shownNotifications.delete(notificationKey);
            }, 5 * 60 * 1000);
            
            // Проверяем наличие глобального объекта notifications
            if (typeof window.notifications !== 'undefined' && window.notifications && typeof window.notifications.show === 'function') {
                window.notifications.show(notification.message, notification.type || 'info');
            } else {
                // Fallback: просто выводим в консоль
                console.log('Уведомление:', notification.message, notification.type);
            }
        });
    }

    on(event, callback) {
        if (!this.callbacks.has(event)) {
            this.callbacks.set(event, []);
        }
        this.callbacks.get(event).push(callback);
    }

    off(event, callback) {
        if (this.callbacks.has(event)) {
            const callbacks = this.callbacks.get(event);
            const index = callbacks.indexOf(callback);
            if (index > -1) {
                callbacks.splice(index, 1);
            }
        }
    }

    trigger(event, data) {
        if (this.callbacks.has(event)) {
            this.callbacks.get(event).forEach(callback => {
                callback(data);
            });
        }
    }
}

// Инициализация
document.addEventListener('DOMContentLoaded', () => {
    window.realtimeUpdates = new RealtimeUpdates();
});

// Экспорт
window.RealtimeUpdates = RealtimeUpdates;

