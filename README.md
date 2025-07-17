# 🏢 Auto Update Companies

## 📝 Описание проекта

Сервис для автоматического обновления данных о компаниях из базы данных ClickHouse с использованием API DaData. Приложение извлекает записи компаний, сортирует их по времени последнего обновления, обогащает актуальной информацией через DaData API и сохраняет результаты в JSON файлы.

## ⚡ Основная функциональность

- **📊 Извлечение данных**: Подключается к ClickHouse и извлекает записи компаний из таблицы `reference_compass`
- **🔄 Обогащение данных**: Получает актуальную информацию о компаниях через сервис DaData (название, адрес, статус, ОКВЭД и др.)
- **💾 Сохранение результатов**: Записывает обновленные данные в JSON файлы в структурированном формате
- **⏰ Автоматизация**: Запускается по расписанию через cron в выходные дни
- **📋 Логирование**: Ведет подробные логи работы с записью в файлы и вывод в консоль

## 🏗️ Архитектура

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   ClickHouse    │    │  Auto Update     │    │   DaData API    │
│   Database      │───▶│   Companies      │───▶│   Service       │
│                 │    │                  │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌─────────────────┐
                       │   JSON Files    │
                       │   Output        │
                       └─────────────────┘
```

## 🔧 Технические требования

### 💻 Системные требования
- Python 3.8+
- Docker (для контейнеризации)
- Доступ к ClickHouse базе данных
- Доступ к сервису DaData

### 🐍 Зависимости Python
- `clickhouse-connect==0.5.14` - для подключения к ClickHouse
- `python-dotenv==1.0.0` - для работы с переменными окружения
- `requests==2.31.0` - для HTTP запросов к API
- `numpy==1.24.4` - для обработки данных

## 🚀 Установка и настройка

### 1️⃣ Клонирование репозитория
```bash
git clone <repository-url>
cd auto_update_companies
```

### 2️⃣ Настройка переменных окружения
Создайте файл `.env` в корне проекта:
```env
# ClickHouse настройки
HOST=your_clickhouse_host
DATABASE=your_database_name
USERNAME_DB=your_username
PASSWORD=your_password

# Сервис DaData
SERVICE_INN=your_service_inn_host

# Пути для сохранения файлов
XL_IDP_PATH_REFERENCE=json
XL_IDP_ROOT_AUTO_UPDATE_SCRIPTS=.
```

### 3️⃣ Установка зависимостей

#### 🏠 Локальная установка
```bash
# Создание виртуального окружения
python3 -m venv venv
source venv/bin/activate  # Linux/Mac
# или
venv\Scripts\activate     # Windows

# Установка зависимостей
pip install -r requirements.txt
```

#### 🐳 Docker установка
```bash
# Сборка Docker образа
docker build --build-arg XL_IDP_PATH_DOCKER=/app -t auto-update-companies .

# Запуск контейнера
docker run -d \
  --name auto-update-companies \
  --env-file .env \
  -v $(pwd)/json:/app/json \
  -v $(pwd)/logging:/app/logging \
  auto-update-companies
```

## 📁 Структура проекта

```
auto_update_companies/
├── main.py                 # Основной модуль приложения
├── __init__.py            # Инициализация и вспомогательные функции
├── requirements.txt       # Python зависимости
├── Dockerfile            # Docker конфигурация
├── crontab              # Расписание cron задач
├── .env                 # Переменные окружения (не в git)
├── scripts/             # Вспомогательные скрипты
│   └── logging/         # Директория для логов
├── tests/               # Тесты (в разработке)
├── venv/               # Виртуальное окружение Python
└── json/               # Выходные JSON файлы
    └── reference_compass/
        └── update/      # Обновленные данные компаний
```

## ⚙️ Механика работы

### 🔄 Алгоритм обработки
1. **🔌 Подключение к БД**: Устанавливается соединение с ClickHouse
2. **📤 Извлечение данных**: Выполняется SQL запрос для получения компаний с сортировкой по дате последнего обновления (до 19000 записей за раз)
3. **📋 Приоритизация**: Первыми обрабатываются записи с NULL в поле last_updated, затем по алфавиту original_file_name
4. **🔍 Обработка записей**: Для каждой компании:
   - Отправляется запрос к сервису DaData по ИНН
   - Обрабатываются данные головного офиса и филиалов
   - Извлекается информация о статусе, адресе, названии, ОКВЭД
5. **💾 Сохранение**: Каждая обработанная запись сохраняется в отдельный JSON файл

### 🛠️ Ключевые компоненты

#### 🏭 Класс `UpdatingCompanies`
- `connect_to_db()` - подключение к ClickHouse и извлечение данных с приоритизацией по дате обновления
- `get_data_from_service_inn()` - взаимодействие с сервисом DaData
- `get_data_from_dadata()` - обработка ответа от DaData API
- `add_dadata_columns()` - добавление полученных данных в структуру
- `write_to_json()` - сохранение данных в JSON файлы

#### 📝 Система логирования
- Логи записываются в файлы в директории `logging/`
- Одновременный вывод в stdout и stderr
- Ротация логов по датам
- Уровни логирования: INFO для обычных операций, ERROR для ошибок

## ▶️ Запуск приложения

### 🏠 Локальный запуск
```bash
# Активация виртуального окружения
source venv/bin/activate

# Запуск основного скрипта
python main.py
```

### 🐳 Запуск в Docker
```yaml
version: "3.9"
services:
  auto_update_companies:
    container_name: auto_update_companies
    ports:
      - "8021:8021"
    volumes:
      - ${XL_IDP_PATH_AUTO_UPDATE_SCRIPTS}:${XL_IDP_PATH_DOCKER}
      - ${XL_IDP_ROOT_REFERENCE}:${XL_IDP_PATH_REFERENCE}
    environment:
      XL_IDP_ROOT_AUTO_UPDATE_SCRIPTS: ${XL_IDP_PATH_DOCKER}
      XL_IDP_PATH_REFERENCE: ${XL_IDP_PATH_REFERENCE}
      TOKEN_TELEGRAM: ${TOKEN_TELEGRAM}
    build:
      context: auto_update_companies
      dockerfile: ./Dockerfile
      args:
        XL_IDP_PATH_DOCKER: ${XL_IDP_PATH_DOCKER}
    networks:
      - postgres
```

```bash
docker-compose up --build
```

### ⏰ Автоматический запуск по расписанию
Приложение настроено на автоматический запуск:
- **Время**: каждую субботу и воскресенье в 01:00
- **Cron выражение**: `0 1 * * 6-7`
- **Команда**: `python3 main.py`

## ⚙️ Конфигурация

### 🌐 Переменные окружения

| Переменная | Описание | Обязательная | Пример |
|------------|----------|--------------|---------|
| `HOST` | Хост ClickHouse сервера | Да | `localhost` |
| `DATABASE` | Имя базы данных | Да | `main_db` |
| `USERNAME_DB` | Пользователь БД | Да | `user` |
| `PASSWORD` | Пароль БД | Да | `password123` |
| `SERVICE_INN` | Хост сервиса DaData | Да | `dadata-service` |
| `XL_IDP_PATH_REFERENCE` | Путь для JSON файлов | Нет | `json` |
| `XL_IDP_ROOT_AUTO_UPDATE_SCRIPTS` | Корневая директория | Нет | `.` |

### 📋 Настройка логирования
Логи автоматически создаются в директории `logging/` с именем файла в формате:
```
main_YYYY-MM-DD.log
```

## 👨‍💻 Разработка

### 📊 Структура данных

#### 📥 Входные данные (ClickHouse)
```sql
SELECT * 
FROM reference_compass 
ORDER BY last_updated NULLS FIRST, original_file_name 
LIMIT 19000
```

#### 📤 Выходные данные (JSON)
```json
{
    "uuid": "company-uuid",
    "inn": "1234567890",
    "dadata_company_name": "ООО Компания",
    "dadata_address": "г. Москва, ул. Примерная, д. 1",
    "dadata_status": "ACTIVE",
    "dadata_geo_lat": "55.7558",
    "dadata_geo_lon": "37.6176",
    "last_updated": "2024-01-15 01:00:00",
    "from_cache": false
}
```

## 📊 Мониторинг и диагностика

### 🔍 Проверка статуса
```bash
# Проверка логов
tail -f logging/main_$(date +%Y-%m-%d).log

# Проверка выходных файлов
ls -la json/reference_compass/update/

# Проверка работы контейнера
docker logs auto-update-companies
```

### ❗ Типичные проблемы и решения

| Проблема | Причина | Решение |
|----------|---------|---------|
| Ошибка подключения к БД | Неверные credentials | Проверить переменные окружения |
| Таймаут DaData API | Перегрузка сервиса | Добавить retry механизм |
| Нет прав на запись JSON | Права доступа | `chmod 777` на директорию |
| Контейнер не запускается | Ошибка в crontab | Проверить синтаксис crontab |