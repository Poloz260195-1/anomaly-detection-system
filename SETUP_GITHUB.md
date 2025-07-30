# 🚀 Простая настройка проекта для GitHub

## 1. Создание репозитория

1. Перейдите на [GitHub](https://github.com)
2. Нажмите "New repository"
3. Назовите: `anomaly-detection-system`
4. Описание: "Система обнаружения аномалий в продуктовых метриках"
5. Выберите "Public"
6. НЕ ставьте галочки (README, .gitignore, license)
7. Нажмите "Create repository"

## 2. Загрузка файлов

```bash
# Клонируйте репозиторий
git clone https://github.com/your-username/anomaly-detection-system.git
cd anomaly-detection-system

# Скопируйте все файлы проекта сюда
# (README.md, requirements.txt, found_alert_with_cooldown_new.py, etc.)

# Добавьте файлы в git
git add .

# Создайте первый коммит
git commit -m "Initial commit: Anomaly Detection System

- Система обнаружения аномалий в продуктовых метриках
- Анализ на одного пользователя для корректной работы с растущими приложениями
- Умный кулдаун для предотвращения спама алертов
- Интеграция с Telegram для мгновенных уведомлений
- Airflow оркестрация для автоматического мониторинга"

# Отправьте в репозиторий
git push -u origin main
```

## 3. Настройка секретов (опционально)

Если хотите использовать переменные окружения:

1. Перейдите в Settings → Secrets and variables → Actions
2. Добавьте секреты:
   - `BOT_TOKEN` - токен Telegram бота
   - `CHAT_ID` - ID чата

## 4. Обновление README

Замените в `README.md`:
- `your-username` на ваше имя пользователя GitHub
- Обновите ссылки на репозиторий

## 5. Создание релиза

1. Перейдите в Releases
2. Нажмите "Create a new release"
3. Тег: `v1.0.0`
4. Название: `Initial Release`
5. Описание:
```markdown
## 🎉 Первый релиз системы обнаружения аномалий

### Что нового
- ✅ Система обнаружения аномалий в продуктовых метриках
- ✅ Анализ на одного пользователя
- ✅ Умный кулдаун для алертов
- ✅ Интеграция с Telegram
- ✅ Airflow оркестрация

### Установка
```bash
git clone https://github.com/your-username/anomaly-detection-system.git
cd anomaly-detection-system
pip install -r requirements.txt
```

### Использование
```bash
# Запуск системы
python found_alert_with_cooldown_new.py
```
```

## 6. Добавление бейджей в README

Добавьте в начало README.md:

```markdown
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
```

## 7. Финальный коммит

```bash
git add .
git commit -m "Update README with badges and links"
git push
```

## 🎉 Готово!

Теперь у вас есть простой, но профессиональный проект на GitHub с:
- ✅ Четким описанием
- ✅ Инструкциями по установке
- ✅ Лицензией
- ✅ Первым релизом
- ✅ Красивыми бейджами

Проект готов для демонстрации! 🚀 