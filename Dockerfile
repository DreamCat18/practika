FROM python:3.9-slim

WORKDIR /app

# Устанавливаем системные зависимости для psycopg2 и tkinter
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    python3-tk \
    tk \
    && rm -rf /var/lib/apt/lists/*

# Копируем и устанавливаем Python зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем исходный код и данные
COPY client_management_system.py .
COPY clients_100.csv .
COPY book_orders.csv .

# Создаем директории для данных
RUN mkdir -p /app/data /app/config

# Устанавливаем переменные окружения
ENV DATA_DIR=/app/data
ENV CONFIG_DIR=/app/config

VOLUME /app/data
VOLUME /app/config

# Проверяем установку зависимостей
RUN python -c "import pandas; import psycopg2; import tkinter; print('Все зависимости загружены успешно')"

CMD ["python", "client_management_system.py"]