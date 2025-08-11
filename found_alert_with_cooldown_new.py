"""
Система обнаружения аномалий в продуктовых метриках
Анализ на одного пользователя для корректной работы с растущими приложениями
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import telegram
from datetime import date
import io
import sys
import os
from io import StringIO
import requests
import pandahouse as ph
from datetime import datetime, timedelta
import json

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# Определяем словари метрик один раз в начале файла
METRICS_LENTA = {
    'users_lenta': 'Активные пользователи в ленте', 
    'views_per_user': 'Просмотры на пользователя', 
    'likes_per_user': 'Лайки на пользователя', 
    'CTR': 'CTR'
}

METRICS_MESSAGE = {
    'users_message': 'Активные пользователи в мессенджере', 
    'sent_message_per_user': 'Отправленные сообщения на пользователя в мессенджере'
}

# Объединяем словари для удобного доступа к подписи
METRICS_FULL = {**METRICS_LENTA, **METRICS_MESSAGE}

# Простая переменная для хранения истории алертов в памяти
alert_history = {}

def cleanup_old_alerts(current_time):
    """Очищает старые записи алертов (старше 1 дня)"""
    global alert_history
    today = current_time.date().isoformat()
    for metric in list(alert_history):
        alert_history[metric] = [a for a in alert_history[metric] if a['date'] == today]
        if not alert_history[metric]:
            del alert_history[metric]

def can_send_alert(metric_name, current_time, cooldown_hours=4, max_alerts_per_day=6):
    """Проверяем, можно ли отправить алерт для данной метрики"""
    global alert_history
    today = current_time.date().isoformat()
    
    # Очищаем старые записи (старше 1 дня)
    cleanup_old_alerts(current_time)
    
    # Проверяем количество алертов за сегодня
    metric_alerts = alert_history.get(metric_name, [])
    today_alerts = [x for x in metric_alerts if x['date'] == today]
    
    if len(today_alerts) >= max_alerts_per_day:
        print(f"Достигнут лимит алертов на день ({max_alerts_per_day}) для метрики {metric_name}")
        return False
    
    # Проверяем кулдаун для конкретной метрики
    if metric_alerts:
        # Берем последний алерт (самый новый)
        last_alert = max(metric_alerts, key=lambda x: x['timestamp'])
        last_alert_time = datetime.fromisoformat(last_alert['timestamp'])
        time_since_last = current_time - last_alert_time
        
        if time_since_last < timedelta(hours=cooldown_hours):
            print(f"Кулдаун для метрики {metric_name}: {time_since_last}")
            return False
    
    return True

def record_alert(metric_name, current_time):
    """Записываем отправленный алерт"""
    global alert_history
    alert_info = {
        'timestamp': current_time.isoformat(),
        'date': current_time.date().isoformat()
    }
    if metric_name not in alert_history:
        alert_history[metric_name] = []
    alert_history[metric_name].append(alert_info)

def check_anomaly(data, metric, threshold=0.3):
    """
    Проверяет аномальность метрики путем сравнения с аналогичным периодом неделю назад
    КЛЮЧЕВОЕ УЛУЧШЕНИЕ: Анализ на одного пользователя
    """
    last_ts = data['ts'].max()
    week_ago_ts = last_ts - pd.DateOffset(days=7)
    
    try:
        last_value = data[data['ts'] == last_ts][metric].iloc[0]
        week_ago_value = data[data['ts'] == week_ago_ts][metric].iloc[0]
    except IndexError:
        return 0, None, None, last_ts, None, week_ago_ts

    # Вычисляем отклонение
    diff = (last_value / week_ago_value - 1)
    
    # Проверяем больше ли отклонение метрики заданного порога threshold
    if abs(diff) > threshold:
        is_alert = 1
    else:
        is_alert = 0 
        
    return is_alert, last_value, diff, last_ts, week_ago_value, week_ago_ts

def send_plot(data, metric, last_ts, week_ago_ts, dataset_name):
    """Создает график сравнения текущих и исторических данных"""
    # Берем данные за два полных дня: последний и неделю назад
    target_dates = [last_ts.date(), week_ago_ts.date()]
    data_filtered = data[data['date'].isin(target_dates)]
    
    # Переименовываем колонки
    if dataset_name == 'lenta':
        data_filtered = data_filtered.rename(columns=METRICS_LENTA)
    else:
        data_filtered = data_filtered.rename(columns=METRICS_MESSAGE)
    
    sns.set(rc={'figure.figsize': (16, 10)})
    plt.tight_layout()

    ax = sns.lineplot(
        data=data_filtered.sort_values(by=['date', 'hm']),
        x="hm", 
        y=METRICS_FULL.get(metric, metric),
        hue="date"
    )

    for ind, label in enumerate(ax.get_xticklabels()):
        if ind % 15 == 0:
            label.set_visible(True)
        else:
            label.set_visible(False)
    ax.set(xlabel='Время')
    ax.set(ylabel=METRICS_FULL.get(metric, metric))

    ax.set_title(f'{METRICS_FULL.get(metric, metric)}')
    ax.set(ylim=(0, None))

    # Формируем файловый объект
    plot_object = io.BytesIO()
    ax.figure.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = '{0}.png'.format(metric)
    plt.close()
    
    return plot_object

def run_alerts_with_cooldown(chat_id):
    """Основная функция запуска системы обнаружения аномалий"""
    bot = telegram.Bot(token=BOT_TOKEN)
    current_time = datetime.now()
    
    # === УЛУЧШЕННЫЕ ЗАПРОСЫ С АНАЛИЗОМ НА ОДНОГО ПОЛЬЗОВАТЕЛЯ ===
    q_1 = '''SELECT 
                ts,
                date,
                hm,
                users_lenta,
                views / users_lenta AS views_per_user,
                likes / users_lenta AS likes_per_user,
                likes / views AS CTR
            FROM (
                SELECT 
                    toStartOfFifteenMinutes(time) AS ts,
                    toDate(time) AS date,
                    formatDateTime(toStartOfFifteenMinutes(time), '%R') AS hm,
                    uniqExact(user_id) AS users_lenta,
                    sum(action = 'view') AS views,
                    sum(action = 'like') AS likes
                FROM simulator_20250620.feed_actions
                WHERE time >= today() - 14
                      AND time < now()
                GROUP BY ts
            )
            ORDER BY ts'''
    
    df_lenta = ph.read_clickhouse(query=q_1, connection=connection)
    
    q_2 = '''SELECT 
                ts,
                date,
                hm,
                users_message,
                sent_message / users_message AS sent_message_per_user
            FROM (
                SELECT 
                    toStartOfFifteenMinutes(time) AS ts,
                    toDate(time) AS date,
                    formatDateTime(toStartOfFifteenMinutes(time), '%R') AS hm,
                    uniqExact(user_id) AS users_message,
                    count(*) AS sent_message
                FROM simulator_20250620.message_actions
                WHERE time >= today() - 14
                      AND time < now()
                GROUP BY ts
            )
            ORDER BY ts'''
    
    df_message = ph.read_clickhouse(query=q_2, connection=connection)
    
    # === ОБНОВЛЕННЫЕ МЕТРИКИ С ФОКУСОМ НА ОДНОГО ПОЛЬЗОВАТЕЛЯ ===
    metrics_lenta = ['views_per_user', 'likes_per_user', 'CTR']  # Убрали users_lenta
    metrics_message = ['sent_message_per_user']  # Убрали users_message
    
    dataframes = {
        'lenta': (df_lenta, metrics_lenta),
        'message': (df_message, metrics_message)
    }
    
    for name, (data, metrics) in dataframes.items():
        for metric in metrics:
            is_alert, last_value, diff, last_ts, _, week_ago_ts = check_anomaly(data, metric, threshold=0.3)
            
            if is_alert:
                # Проверяем, можно ли отправить алерт
                if can_send_alert(metric, current_time, cooldown_hours=4, max_alerts_per_day=6):
                    # Определяем направление изменения
                    if last_value > week_ago_value:
                        direction = "📈 УВЕЛИЧЕНИЕ"
                        emoji = "🔴"
                    else:
                        direction = "📉 УМЕНЬШЕНИЕ"
                        emoji = "🟡"
                    
                    text = f'{emoji} <b>АНОМАЛИЯ ОБНАРУЖЕНА</b>\n\n'
                    text += f'<b>Метрика:</b> {METRICS_FULL.get(metric, metric)}\n'
                    text += f'<b>Направление:</b> {direction}\n'
                    text += f'<b>Текущее значение:</b> {last_value:.2f}\n'
                    text += f'<b>Отклонение:</b> {diff:.1%}\n'
                    text += f'<b>Время обнаружения:</b> {current_time.strftime("%Y-%m-%d %H:%M")}\n\n'
                    text += f'<i>Следующий алерт для этой метрики будет возможен через 4 часа</i>'
                    
                    plot_object = send_plot(data, metric, last_ts, week_ago_ts, name)
                    
                    try:
                        bot.send_message(chat_id=chat_id, text=text, parse_mode='HTML')
                        bot.send_photo(chat_id=chat_id, photo=plot_object)
                        
                        # Записываем отправленный алерт
                        record_alert(metric, current_time)
                        
                        print(f"✅ Алерт отправлен для метрики {metric}")
                        
                    except Exception as e:
                        print(f"❌ Ошибка отправки алерта: {e}")
                else:
                    print(f"⏳ Алерт для метрики {metric} пропущен (кулдаун/лимит)")

# === CONFIG ===
connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'database': 'simulator_20250620',
    'user': 'student',
    'password': 'dpo_python_2020'
}

BOT_TOKEN = '8241718618:AAFW4Y6NE-Uf8ksiPL7zytCx6f8wmlu8MFA'

# Список чатов для отправки отчётов
chat_ids = [
    -969316925,  # основной чат АЛЕРТЫ | KC Симулятор Аналитика
]

default_args = {
    'owner': 'aleksej-polozov-bel8894',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 18),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Интервал запуска DAG
schedule_interval = '*/15 * * * *'  # Каждые 15 минут

@dag(
    dag_id='aleksej_polozov_bel8894_alerts_report_with_cooldown', 
    default_args=default_args, 
    schedule_interval=schedule_interval, 
    catchup=False,
    tags=['anomaly-detection', 'monitoring', 'alerts', 'per-user-analysis']
)
def dag_report():
    
    @task()
    def report_text():
        for chat_id in chat_ids:
            run_alerts_with_cooldown(chat_id)
        
    # Вызываем таски — создаём зависимости
    task1 = report_text()

    # Порядок выполнения
    task1 
    
dag = dag_report()

if __name__ == "__main__":
    # Для локального тестирования
    print("🚀 Запуск системы обнаружения аномалий...")
    print("📊 Анализ метрик на одного пользователя...")
    
    for chat_id in chat_ids:
        run_alerts_with_cooldown(chat_id)
    
    print("✅ Система завершена") 