
import os
import sys
import time
import json
import logging
from datetime import datetime
import psycopg2
from metabase_integration import MetabaseIntegration

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('metabase_sync.log'),
        logging.StreamHandler()
    ]
)

class MetabaseSyncService:
    """Служба синхронизации с Metabase"""
    
    def __init__(self, config_file='metabase_config.json'):
        self.config_file = config_file
        self.config = self.load_config()
        self.metabase = None
        self.db_connection = None
    
    def load_config(self):
        """Загрузка конфигурации"""
        try:
            with open(self.config_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Ошибка загрузки конфигурации: {e}")
            return None
    
    def connect_to_database(self):
        """Подключение к PostgreSQL"""
        try:
            self.db_connection = psycopg2.connect(
                host=os.getenv('DATABASE_HOST', 'db'),
                database=os.getenv('DATABASE_NAME', 'cms_db'),
                user=os.getenv('DATABASE_USER', 'postgres'),
                password=os.getenv('DATABASE_PASSWORD', 'password'),
                port=os.getenv('DATABASE_PORT', '5432')
            )
            logging.info("Подключение к БД установлено")
            return True
        except Exception as e:
            logging.error(f"Ошибка подключения к БД: {e}")
            return False
    
    def get_customers_data(self):
        """Получение данных клиентов"""
        try:
            query = "SELECT * FROM customers ORDER BY id"
            cursor = self.db_connection.cursor()
            cursor.execute(query)
            
            columns = [desc[0] for desc in cursor.description]
            customers = []
            
            for row in cursor.fetchall():
                customers.append(dict(zip(columns, row)))
            
            cursor.close()
            logging.info(f"Получено {len(customers)} клиентов")
            return customers
            
        except Exception as e:
            logging.error(f"Ошибка получения данных клиентов: {e}")
            return []
    
    def get_orders_data(self):
        """Получение данных заказов"""
        try:
            query = "SELECT * FROM orders ORDER BY order_date"
            cursor = self.db_connection.cursor()
            cursor.execute(query)
            
            columns = [desc[0] for desc in cursor.description]
            orders = []
            
            for row in cursor.fetchall():
                orders.append(dict(zip(columns, row)))
            
            cursor.close()
            logging.info(f"Получено {len(orders)} заказов")
            return orders
            
        except Exception as e:
            logging.error(f"Ошибка получения данных заказов: {e}")
            return []
    
    def run_sync(self):
        """Запуск синхронизации"""
        try:
            # Подключение к Metabase
            self.metabase = MetabaseIntegration(self.config)
            if not self.metabase.session_id:
                logging.error("Не удалось подключиться к Metabase")
                return False
            
            # Подключение к БД
            if not self.connect_to_database():
                return False
            
            # Получение данных
            customers = self.get_customers_data()
            orders = self.get_orders_data()
            
            if not customers and not orders:
                logging.warning("Нет данных для синхронизации")
                return True
            
            # Синхронизация с Metabase
            result = self.metabase.sync_cms_data(customers, orders)
            
            if result:
                logging.info("Синхронизация завершена успешно")
            else:
                logging.warning("Синхронизация завершена с ошибками")
            
            return result
            
        except Exception as e:
            logging.error(f"Ошибка выполнения синхронизации: {e}")
            return False
    
    def run_continuous(self, interval_minutes=5):
        """Непрерывная синхронизация с заданным интервалом"""
        logging.info(f"Запуск непрерывной синхронизации с интервалом {interval_minutes} минут")
        
        while True:
            try:
                logging.info(f"Начало синхронизации в {datetime.now()}")
                self.run_sync()
                logging.info(f"Следующая синхронизация через {interval_minutes} минут")
                time.sleep(interval_minutes * 60)
                
            except KeyboardInterrupt:
                logging.info("Остановка службы синхронизации")
                break
            except Exception as e:
                logging.error(f"Ошибка в цикле синхронизации: {e}")
                time.sleep(60)  # Пауза при ошибке

if __name__ == "__main__":
    # Парсинг аргументов командной строки
    import argparse
    
    parser = argparse.ArgumentParser(description='Синхронизация данных с Metabase')
    parser.add_argument('--once', action='store_true', help='Однократная синхронизация')
    parser.add_argument('--continuous', action='store_true', help='Непрерывная синхронизация')
    parser.add_argument('--interval', type=int, default=5, help='Интервал в минутах (по умолчанию: 5)')
    parser.add_argument('--config', type=str, default='metabase_config.json', help='Путь к файлу конфигурации')
    
    args = parser.parse_args()
    
    # Запуск службы
    service = MetabaseSyncService(args.config)
    
    if args.once:
        # Однократная синхронизация
        service.run_sync()
    elif args.continuous:
        # Непрерывная синхронизация
        service.run_continuous(args.interval)
    else:
        # По умолчанию - однократная синхронизация
        service.run_sync()