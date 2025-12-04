import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import pandas as pd
from datetime import datetime, date, timedelta
import logging
import os
import psycopg2
from psycopg2.extras import DictCursor
from typing import List, Dict, Any, Optional
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import seaborn as sns
import numpy as np
import json
import requests
import webbrowser


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# ========== ВСПОМОГАТЕЛЬНЫЕ КЛАССЫ ==========

class MetabaseIntegration:
    """Интеграция с Metabase для автоматической синхронизации данных"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.base_url = config.get('url', 'http://localhost:3000')
        self.session_id = None
        self.database_id = config.get('database_id')
        self.logger = logging.getLogger(__name__)
        
    def connect(self) -> bool:
        """Подключение к Metabase API"""
        try:
            # Проверяем, включена ли интеграция
            if not self.config.get('enabled', False):
                self.logger.info("Интеграция с Metabase отключена")
                return False
            
            # Получение сессии
            auth_data = {
                "username": self.config['username'],
                "password": self.config['password']
            }
            
            response = requests.post(
                f"{self.base_url}/api/session",
                json=auth_data,
                headers={"Content-Type": "application/json"},
                timeout=10
            )
            
            if response.status_code == 200:
                self.session_id = response.json()['id']
                self.logger.info("Успешное подключение к Metabase API")
                return True
            else:
                self.logger.error(f"Ошибка подключения: {response.status_code} - {response.text}")
                return False
                
        except requests.exceptions.ConnectionError:
            self.logger.warning("Metabase недоступен, проверьте запущен ли контейнер")
            return False
        except Exception as e:
            self.logger.error(f"Ошибка подключения к Metabase: {e}")
            return False
    
    def sync_schema(self) -> bool:
        """Синхронизация схемы базы данных"""
        try:
            if not self.session_id:
                if not self.connect():
                    return False
            
            if not self.database_id:
                self.logger.warning("ID базы данных не указан")
                return False
            
            response = requests.post(
                f"{self.base_url}/api/database/{self.database_id}/sync_schema",
                headers=self._get_headers(),
                timeout=30
            )
            
            if response.status_code == 200:
                self.logger.info("Схема базы данных синхронизирована с Metabase")
                return True
            else:
                self.logger.warning(f"Ошибка синхронизации схемы: {response.status_code}")
                return False
                
        except Exception as e:
            self.logger.error(f"Ошибка синхронизации схемы: {e}")
            return False
    
    def create_dashboard(self, name: str, description: str = "") -> Optional[int]:
        """Создание дашборда в Metabase"""
        try:
            if not self.session_id:
                if not self.connect():
                    return None
            
            dashboard_data = {
                "name": name,
                "description": description,
                "parameters": [],
                "collection_id": self.config.get('collection_id')
            }
            
            response = requests.post(
                f"{self.base_url}/api/dashboard",
                json=dashboard_data,
                headers=self._get_headers(),
                timeout=10
            )
            
            if response.status_code == 200:
                dashboard_id = response.json()['id']
                self.logger.info(f"Создан дашборд '{name}' с ID: {dashboard_id}")
                return dashboard_id
            else:
                self.logger.error(f"Ошибка создания дашборда: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            self.logger.error(f"Ошибка создания дашборда: {e}")
            return None
    
    def get_dashboard_url(self, dashboard_id: int) -> str:
        """Получение URL дашборда"""
        return f"{self.base_url}/dashboard/{dashboard_id}"
    
    def _get_headers(self) -> Dict[str, str]:
        """Получение заголовков для API запросов"""
        return {
            "Content-Type": "application/json",
            "X-Metabase-Session": self.session_id
        }


class DatabaseManager:
    """Менеджер для работы с PostgreSQL базой данных"""
    
    def __init__(self):
        self.connection = None
        self.cursor = None
        self.connect()
    
    def connect(self):
        """Подключение к PostgreSQL"""
        try:
            # Проверяем переменные окружения для Docker
            db_host = os.getenv('DATABASE_HOST', 'localhost')
            db_port = os.getenv('DATABASE_PORT', '5432')
            db_name = os.getenv('DATABASE_NAME', 'cms_db')
            db_user = os.getenv('DATABASE_USER', 'postgres')
            db_password = os.getenv('DATABASE_PASSWORD', 'password')
            
            self.connection = psycopg2.connect(
                host=db_host,
                port=db_port,
                database=db_name,
                user=db_user,
                password=db_password,
                cursor_factory=DictCursor
            )
            self.cursor = self.connection.cursor()
            logging.info(f"Успешное подключение к PostgreSQL: {db_host}:{db_port}/{db_name}")
            
        except Exception as e:
            logging.error(f"Ошибка подключения к PostgreSQL: {e}")
            # Возвращаемся к CSV-файлам
            self.connection = None
    
    def save_to_database(self, customers, orders):
        """Сохранение данных в PostgreSQL"""
        if not self.connection:
            logging.warning("Нет подключения к БД, используется CSV")
            return False
        
        try:
            # Сначала создаем таблицы если они не существуют
            self._create_tables_if_not_exists()
            
            # Сохраняем клиентов
            for customer in customers:
                self.cursor.execute("""
                    INSERT INTO customers (id, full_name, email, phone, registration_date, notes, total_orders, total_spent)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        full_name = EXCLUDED.full_name,
                        email = EXCLUDED.email,
                        phone = EXCLUDED.phone,
                        notes = EXCLUDED.notes,
                        total_orders = EXCLUDED.total_orders,
                        total_spent = EXCLUDED.total_spent,
                        updated_at = CURRENT_TIMESTAMP
                """, (
                    customer['id'],
                    customer.get('full_name', ''),
                    customer.get('email', ''),
                    customer.get('phone', ''),
                    customer.get('registration_date', date.today()),
                    customer.get('notes', ''),
                    customer.get('total_orders', 0),
                    customer.get('total_spent', 0.0)
                ))
            
            # Сохраняем заказы
            for order in orders:
                self.cursor.execute("""
                    INSERT INTO orders (id, customer_id, customer_name, order_date, book_title, 
                                      author, genre, quantity, price, discount, final_price, 
                                      total_amount, status, delivery_method, order_notes)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        customer_id = EXCLUDED.customer_id,
                        customer_name = EXCLUDED.customer_name,
                        order_date = EXCLUDED.order_date,
                        book_title = EXCLUDED.book_title,
                        author = EXCLUDED.author,
                        genre = EXCLUDED.genre,
                        quantity = EXCLUDED.quantity,
                        price = EXCLUDED.price,
                        discount = EXCLUDED.discount,
                        final_price = EXCLUDED.final_price,
                        total_amount = EXCLUDED.total_amount,
                        status = EXCLUDED.status,
                        delivery_method = EXCLUDED.delivery_method,
                        order_notes = EXCLUDED.order_notes,
                        updated_at = CURRENT_TIMESTAMP
                """, (
                    order['id'],
                    order.get('customer_id', 0),
                    order.get('customer_name', ''),
                    order.get('date', date.today()),
                    order.get('book_title', ''),
                    order.get('author', ''),
                    order.get('genre', ''),
                    order.get('quantity', 1),
                    order.get('price', 0.0),
                    order.get('discount', 0.0),
                    order.get('final_price', 0.0),
                    order.get('total_amount', 0.0),
                    order.get('status', 'Ожидает оплаты'),
                    order.get('delivery_method', ''),
                    order.get('order_notes', '')
                ))
            
            self.connection.commit()
            logging.info(f"Данные сохранены в PostgreSQL: {len(customers)} клиентов, {len(orders)} заказов")
            return True
            
        except Exception as e:
            self.connection.rollback()
            logging.error(f"Ошибка сохранения в PostgreSQL: {e}")
            return False
    
    def load_from_database(self):
        """Загрузка данных из PostgreSQL"""
        if not self.connection:
            logging.warning("Нет подключения к БД, загружаем из CSV")
            return None, None
        
        try:
            # Проверяем существование таблиц
            self._create_tables_if_not_exists()
            
            # Загружаем клиентов
            self.cursor.execute("SELECT * FROM customers ORDER BY id")
            customers = []
            for row in self.cursor.fetchall():
                customers.append(dict(row))
            
            # Загружаем заказы
            self.cursor.execute("SELECT * FROM orders ORDER BY order_date")
            orders = []
            for row in self.cursor.fetchall():
                orders.append(dict(row))
            
            logging.info(f"Загружено из PostgreSQL: {len(customers)} клиентов, {len(orders)} заказов")
            return customers, orders
            
        except Exception as e:
            logging.error(f"Ошибка загрузки из PostgreSQL: {e}")
            return None, None
    
    def _create_tables_if_not_exists(self):
        """Создание таблиц если они не существуют"""
        try:
            # Таблица клиентов
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS customers (
                    id SERIAL PRIMARY KEY,
                    full_name VARCHAR(255) NOT NULL,
                    email VARCHAR(255),
                    phone VARCHAR(50),
                    registration_date DATE NOT NULL,
                    notes TEXT,
                    total_orders INTEGER DEFAULT 0,
                    total_spent DECIMAL(10,2) DEFAULT 0.0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Таблица заказов
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS orders (
                    id VARCHAR(50) PRIMARY KEY,
                    customer_id INTEGER REFERENCES customers(id) ON DELETE CASCADE,
                    customer_name VARCHAR(255) NOT NULL,
                    order_date DATE NOT NULL,
                    book_title VARCHAR(255) NOT NULL,
                    author VARCHAR(255),
                    genre VARCHAR(100),
                    quantity INTEGER DEFAULT 1,
                    price DECIMAL(10,2) NOT NULL,
                    discount DECIMAL(5,2) DEFAULT 0.0,
                    final_price DECIMAL(10,2) NOT NULL,
                    total_amount DECIMAL(10,2) NOT NULL,
                    status VARCHAR(50) DEFAULT 'Ожидает оплаты',
                    delivery_method VARCHAR(100),
                    order_notes TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Индексы
            self.cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_customers_name ON customers(full_name)
            """)
            self.cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders(customer_id)
            """)
            
            self.connection.commit()
            
        except Exception as e:
            self.connection.rollback()
            logging.error(f"Ошибка создания таблиц: {e}")
            raise
    
    def close(self):
        """Закрытие соединения"""
        if self.connection:
            if self.cursor:
                self.cursor.close()
            self.connection.close()


class OrderManager:
    """Менеджер для работы с заказами"""
    
    def get_customer_orders(self, customer_id, orders):
        """Получение заказов клиента"""
        return [order for order in orders if order['customer_id'] == customer_id]
    
    def get_customer_total_spent(self, customer_id, orders):
        """Получение общей суммы потраченной клиентом"""
        customer_orders = self.get_customer_orders(customer_id, orders)
        return sum(order.get('total_amount', 0) for order in customer_orders)
    
    def get_customer_order_statistics(self, customer_id, orders):
        """Получение статистики заказов клиента"""
        customer_orders = self.get_customer_orders(customer_id, orders)
        
        if not customer_orders:
            return {
                'total_orders': 0,
                'total_amount': 0,
                'average_order': 0,
                'last_order': None
            }
        
        total_amount = sum(order.get('total_amount', 0) for order in customer_orders)
        order_dates = [order.get('date') for order in customer_orders if order.get('date')]
        
        return {
            'total_orders': len(customer_orders),
            'total_amount': total_amount,
            'average_order': total_amount / len(customer_orders) if customer_orders else 0,
            'last_order': max(order_dates) if order_dates else None
        }


class ReportGenerator:
    """Генератор отчетов"""
    
    def generate_report(self, report_type, customers, orders, date_from=None, date_to=None):
        """Генерация отчета по типу"""
        if report_type == "customer_summary":
            return self.generate_customer_summary(customers, orders)
        elif report_type == "registration_analysis":
            return self.generate_registration_analysis(customers, date_from, date_to)
        elif report_type == "order_statistics":
            return self.generate_order_statistics(orders, date_from, date_to)
        elif report_type == "customer_activity":
            return self.generate_customer_activity(customers, orders, date_from, date_to)
        else:
            return "Неизвестный тип отчета"
    
    def generate_customer_summary(self, customers, orders):
        """Генерация сводки по клиентам"""
        order_manager = OrderManager()
        
        report = "ОТЧЕТ ПО КЛИЕНТАМ - СВОДКА\n"
        report += "=" * 50 + "\n\n"
        
        report += f"Всего клиентов: {len(customers)}\n"
        
        # Статистика по заказам
        customers_with_orders = 0
        total_revenue = 0
        
        for customer in customers:
            customer_orders = order_manager.get_customer_orders(customer['id'], orders)
            if customer_orders:
                customers_with_orders += 1
                total_revenue += order_manager.get_customer_total_spent(customer['id'], orders)
        
        report += f"Клиентов с заказами: {customers_with_orders}\n"
        report += f"Общая выручка: {total_revenue:.2f} руб.\n\n"
        
        # Детали по клиентам
        report += "ДЕТАЛИ ПО КЛИЕНТАМ:\n"
        report += "-" * 40 + "\n"
        
        for customer in customers:
            customer_orders = order_manager.get_customer_orders(customer['id'], orders)
            total_spent = order_manager.get_customer_total_spent(customer['id'], orders)
            
            report += f"ID: {customer['id']} | ФИО: {customer['full_name']}\n"
            report += f"    Заказов: {len(customer_orders)} | Потрачено: {total_spent:.2f} руб.\n"
            report += f"    Зарегистрирован: {customer['registration_date']}\n\n"
        
        return report
    
    def generate_registration_analysis(self, customers, date_from=None, date_to=None):
        """Анализ регистраций"""
        report = "АНАЛИЗ РЕГИСТРАЦИЙ КЛИЕНТОВ\n"
        report += "=" * 50 + "\n\n"
        
        # Группировка по месяцам
        from collections import defaultdict
        monthly_registrations = defaultdict(int)
        
        for customer in customers:
            reg_date = customer.get('registration_date')
            if reg_date:
                # Извлекаем год и месяц
                year_month = reg_date[:7]  # YYYY-MM
                monthly_registrations[year_month] += 1
        
        report += "РЕГИСТРАЦИИ ПО МЕСЯЦАМ:\n"
        report += "-" * 25 + "\n"
        
        for month in sorted(monthly_registrations.keys()):
            report += f"{month}: {monthly_registrations[month]} клиентов\n"
        
        report += f"\nВсего регистраций: {len(customers)}\n"
        
        return report
    
    def generate_order_statistics(self, orders, date_from=None, date_to=None):
        """Статистика заказов"""
        report = "СТАТИСТИКА ЗАКАЗОВ\n"
        report += "=" * 40 + "\n\n"
        
        if not orders:
            return report + "Заказы не найдены.\n"
        
        total_orders = len(orders)
        total_revenue = sum(order.get('total_amount', 0) for order in orders)
        avg_order = total_revenue / total_orders if total_orders > 0 else 0
        
        report += f"Всего заказов: {total_orders}\n"
        report += f"Общая выручка: {total_revenue:.2f} руб.\n"
        report += f"Средний чек: {avg_order:.2f} руб.\n\n"
        
        # Статистика по статусам
        status_counts = {}
        for order in orders:
            status = order.get('status', 'Неизвестно')
            status_counts[status] = status_counts.get(status, 0) + 1
        
        report += "ЗАКАЗЫ ПО СТАТУСАМ:\n"
        report += "-" * 20 + "\n"
        for status, count in status_counts.items():
            report += f"{status}: {count}\n"
        
        return report
    
    def generate_customer_activity(self, customers, orders, date_from=None, date_to=None):
        """Активность клиентов"""
        order_manager = OrderManager()
        
        report = "АКТИВНОСТЬ КЛИЕНТОВ\n"
        report += "=" * 40 + "\n\n"
        
        # Сортировка клиентов по количеству заказов
        customers_with_orders = []
        for customer in customers:
            customer_orders = order_manager.get_customer_orders(customer['id'], orders)
            if customer_orders:
                customers_with_orders.append((customer, len(customer_orders)))
        
        # Сортировка по убыванию количества заказов
        customers_with_orders.sort(key=lambda x: x[1], reverse=True)
        
        report += "ТОП КЛИЕНТОВ ПО КОЛИЧЕСТВУ ЗАКАЗОВ:\n"
        report += "-" * 45 + "\n"
        
        for customer, order_count in customers_with_orders[:10]:  # Топ 10
            total_spent = order_manager.get_customer_total_spent(customer['id'], orders)
            report += f"{customer['full_name']}: {order_count} заказов, {total_spent:.2f} руб. потрачено\n"
        
        return report


class ExcelDataImporter:
    """Модуль импорта данных из Excel файлов"""
    
    def __init__(self, main_app):
        self.main_app = main_app
        self.logger = logging.getLogger(__name__)
    
    def import_customers_from_excel(self, file_path=None):
        """
        Импорт клиентов из Excel файла
        
        Args:
            file_path (str): Путь к файлу Excel
        
        Returns:
            list: Список клиентов
        """
        if not file_path:
            file_path = filedialog.askopenfilename(
                title="Выберите Excel файл с клиентами",
                filetypes=[
                    ("Excel files", "*.xlsx *.xls"),
                    ("Все файлы", "*.*")
                ]
            )
        
        if not file_path:
            return None
        
        try:
            self.logger.info(f"Начинаю импорт клиентов из {file_path}")
            
            # Чтение Excel файла
            df = pd.read_excel(file_path)
            self.logger.info(f"Загружено {len(df)} строк из файла")
            
            # Преобразование данных
            customers = []
            customer_id_start = self.main_app.next_customer_id
            
            for index, row in df.iterrows():
                customer = self._process_customer_row(row, customer_id_start + index)
                if customer:
                    customers.append(customer)
            
            self.logger.info(f"Успешно обработано {len(customers)} клиентов")
            return customers
            
        except Exception as e:
            self.logger.error(f"Ошибка импорта клиентов: {e}")
            messagebox.showerror("Ошибка импорта", f"Не удалось импортировать клиентов:\n{str(e)}")
            return None
    
    def import_orders_from_excel(self, file_path=None):
        """
        Импорт заказов из Excel файла
        
        Args:
            file_path (str): Путь к файлу Excel
        
        Returns:
            list: Список заказов
        """
        if not file_path:
            file_path = filedialog.askopenfilename(
                title="Выберите Excel файл с заказами",
                filetypes=[
                    ("Excel files", "*.xlsx *.xls"),
                    ("Все файлы", "*.*")
                ]
            )
        
        if not file_path:
            return None
        
        try:
            self.logger.info(f"Начинаю импорт заказов из {file_path}")
            
            # Чтение Excel файла
            df = pd.read_excel(file_path)
            self.logger.info(f"Загружено {len(df)} строк из файла")
            
            # Преобразование данных
            orders = []
            order_id_start = self.main_app.next_order_id
            
            for index, row in df.iterrows():
                order = self._process_order_row(row, order_id_start + index)
                if order:
                    orders.append(order)
            
            self.logger.info(f"Успешно обработано {len(orders)} заказов")
            return orders
            
        except Exception as e:
            self.logger.error(f"Ошибка импорта заказов: {e}")
            messagebox.showerror("Ошибка импорта", f"Не удалось импортировать заказы:\n{str(e)}")
            return None
    
    def _process_customer_row(self, row, customer_id):
        """
        Обработка строки с данными клиента
        
        Args:
            row: Строка DataFrame
            customer_id: ID клиента
        
        Returns:
            dict: Данные клиента
        """
        try:
            # Маппинг возможных названий колонок
            column_mapping = {
                'ФИО': ['ФИО', 'full_name', 'Имя', 'Клиент', 'ФИО_клиента'],
                'Email': ['Email', 'email', 'Почта'],
                'Телефон': ['Телефон', 'phone', 'Мобильный'],
                'Дата регистрации': ['Дата регистрации', 'registration_date', 'Дата'],
                'Примечания': ['Примечания', 'notes', 'Комментарий']
            }
            
            # Поиск значений по различным названиям колонок
            customer_data = {
                'id': customer_id,
                'full_name': self._get_value_from_row(row, column_mapping['ФИО'], ''),
                'email': self._get_value_from_row(row, column_mapping['Email'], ''),
                'phone': self._get_value_from_row(row, column_mapping['Телефон'], ''),
                'registration_date': self._parse_date(
                    self._get_value_from_row(row, column_mapping['Дата регистрации'], datetime.now())
                ),
                'notes': self._get_value_from_row(row, column_mapping['Примечания'], ''),
                'total_orders': 0,
                'total_spent': 0.0
            }
            
            # Если имя пустое, пропускаем запись
            if not customer_data['full_name']:
                return None
            
            return customer_data
            
        except Exception as e:
            self.logger.warning(f"Ошибка обработки строки клиента: {e}")
            return None
    
    def _process_order_row(self, row, order_index):
        """
        Обработка строки с данными заказа
        
        Args:
            row: Строка DataFrame
            order_index: Индекс заказа
        
        Returns:
            dict: Данные заказа
        """
        try:
            # Маппинг возможных названий колонок
            column_mapping = {
                'ID_заказа': ['ID_заказа', 'order_id', 'ID заказа'],
                'ФИО_клиента': ['ФИО_клиента', 'client_name', 'Клиент', 'ФИО'],
                'Дата_заказа': ['Дата_заказа', 'order_date', 'Дата'],
                'Название_книги': ['Название_книги', 'book_title', 'Книга'],
                'Автор': ['Автор', 'author'],
                'Жанр': ['Жанр', 'genre', 'Категория'],
                'Количество': ['Количество', 'quantity', 'Кол-во'],
                'Цена_за_шт': ['Цена_за_шт', 'price', 'Цена'],
                'Скидка_%': ['Скидка_%', 'discount', 'Скидка'],
                'Статус_заказа': ['Статус_заказа', 'status', 'Статус'],
                'Способ_доставки': ['Способ_доставки', 'delivery_method', 'Доставка'],
                'Примечание_к_заказу': ['Примечание_к_заказу', 'notes', 'Комментарий']
            }
            
            # Получение значений
            customer_name = self._get_value_from_row(row, column_mapping['ФИО_клиента'], '')
            if not customer_name:
                self.logger.warning(f"Пропускаю заказ {order_index}: не указан клиент")
                return None
            
            # Поиск клиента
            customer = self._find_customer_by_name(customer_name)
            if not customer:
                self.logger.warning(f"Клиент не найден: {customer_name}")
                # Создаем нового клиента
                customer = self._create_new_customer(customer_name)
                if customer:
                    self.main_app.customers.append(customer)
                    self.main_app.next_customer_id += 1
            
            if not customer:
                return None
            
            # Расчет цен
            quantity = self._parse_int(self._get_value_from_row(row, column_mapping['Количество'], 1))
            price = self._parse_float(self._get_value_from_row(row, column_mapping['Цена_за_шт'], 0))
            discount = self._parse_float(self._get_value_from_row(row, column_mapping['Скидка_%'], 0))
            
            final_price = price * (1 - discount / 100)
            total_amount = final_price * quantity
            
            # Формирование данных заказа
            order_data = {
                'id': self._get_value_from_row(row, column_mapping['ID_заказа'], f"ORD{order_index:05d}"),
                'customer_id': customer['id'],
                'customer_name': customer_name,
                'date': self._parse_date(
                    self._get_value_from_row(row, column_mapping['Дата_заказа'], datetime.now())
                ),
                'book_title': self._get_value_from_row(row, column_mapping['Название_книги'], ''),
                'author': self._get_value_from_row(row, column_mapping['Автор'], ''),
                'genre': self._get_value_from_row(row, column_mapping['Жанр'], ''),
                'quantity': quantity,
                'price': price,
                'discount': discount,
                'final_price': final_price,
                'total_amount': total_amount,
                'status': self._get_value_from_row(row, column_mapping['Статус_заказа'], 'Ожидает оплаты'),
                'delivery_method': self._get_value_from_row(row, column_mapping['Способ_доставки'], 'Самовывоз'),
                'order_notes': self._get_value_from_row(row, column_mapping['Примечание_к_заказу'], '')
            }
            
            return order_data
            
        except Exception as e:
            self.logger.warning(f"Ошибка обработки строки заказа: {e}")
            return None
    
    def _create_new_customer(self, customer_name):
        """Создание нового клиента при импорте"""
        customer = {
            'id': self.main_app.next_customer_id,
            'full_name': customer_name,
            'email': '',
            'phone': '',
            'registration_date': datetime.now().strftime("%Y-%m-%d"),
            'notes': 'Создан автоматически при импорте заказов',
            'total_orders': 0,
            'total_spent': 0.0
        }
        return customer
    
    def _get_value_from_row(self, row, possible_columns, default=None):
        """
        Получение значения из строки по различным возможным названиям колонок
        
        Args:
            row: Строка DataFrame
            possible_columns: Список возможных названий колонок
            default: Значение по умолчанию
        
        Returns:
            Любое: Значение из строки
        """
        for col in possible_columns:
            if col in row.index:
                value = row[col]
                if pd.isna(value):
                    return default
                return value
        return default
    
    def _find_customer_by_name(self, name):
        """
        Поиск клиента по имени
        
        Args:
            name (str): Имя клиента
        
        Returns:
            dict: Данные клиента или None
        """
        clean_name = str(name).strip().lower()
        for customer in self.main_app.customers:
            if customer['full_name'].lower() == clean_name:
                return customer
        
        # Попробуем найти частичное совпадение
        for customer in self.main_app.customers:
            if clean_name in customer['full_name'].lower():
                return customer
        
        return None
    
    def _parse_date(self, value):
        """
        Парсинг даты из различных форматов
        
        Args:
            value: Значение даты
        
        Returns:
            str: Дата в формате YYYY-MM-DD
        """
        if pd.isna(value):
            return datetime.now().strftime("%Y-%m-%d")
        
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d")
        
        if isinstance(value, str):
            # Попробуем разные форматы
            date_formats = [
                "%Y-%m-%d",
                "%d.%m.%Y",
                "%d/%m/%Y",
                "%m/%d/%Y",
                "%Y.%m.%d",
                "%d-%m-%Y",
                "%Y/%m/%d"
            ]
            
            for fmt in date_formats:
                try:
                    date_obj = datetime.strptime(value.strip(), fmt)
                    return date_obj.strftime("%Y-%m-%d")
                except ValueError:
                    continue
        
        # Если не удалось распарсить, возвращаем сегодняшнюю дату
        return datetime.now().strftime("%Y-%m-%d")
    
    def _parse_int(self, value):
        """
        Парсинг целого числа
        
        Args:
            value: Значение
        
        Returns:
            int: Целое число
        """
        if pd.isna(value):
            return 1
        
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return 1
    
    def _parse_float(self, value):
        """
        Парсинг дробного числа
        
        Args:
            value: Значение
        
        Returns:
            float: Дробное число
        """
        if pd.isna(value):
            return 0.0
        
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0.0
    
    def import_all_data(self):
        """
        Импорт всех данных из Excel файлов
        """
        try:
            customers_added = 0
            orders_added = 0
            
            # Импорт клиентов
            customers_file = filedialog.askopenfilename(
                title="Выберите Excel файл с клиентами",
                filetypes=[("Excel files", "*.xlsx *.xls")]
            )
            
            if customers_file:
                customers = self.import_customers_from_excel(customers_file)
                if customers:
                    self.main_app.customers.extend(customers)
                    self.main_app.next_customer_id += len(customers)
                    customers_added = len(customers)
                    self.logger.info(f"Добавлено {customers_added} клиентов")
            
            # Импорт заказов
            orders_file = filedialog.askopenfilename(
                title="Выберите Excel файл с заказами",
                filetypes=[("Excel files", "*.xlsx *.xls")]
            )
            
            if orders_file:
                orders = self.import_orders_from_excel(orders_file)
                if orders:
                    self.main_app.orders.extend(orders)
                    self.main_app.next_order_id += len(orders)
                    orders_added = len(orders)
                    self.logger.info(f"Добавлено {orders_added} заказов")
            
            # Обновление интерфейса
            if customers_added > 0 or orders_added > 0:
                self.main_app.load_customers()
                self.main_app.update_customer_listbox()
                self.main_app.update_data_info()
                
                messagebox.showinfo(
                    "Импорт завершен",
                    f"Успешно импортировано:\n"
                    f"- Клиентов: {customers_added}\n"
                    f"- Заказов: {orders_added}"
                )
            else:
                messagebox.showinfo("Импорт", "Данные не были импортированы")
                
        except Exception as e:
            self.logger.error(f"Ошибка при импорте всех данных: {e}")
            messagebox.showerror("Ошибка", f"Не удалось импортировать данные:\n{str(e)}")


class DataVisualization:
    """Модуль визуализации данных и отчетности"""
    
    def __init__(self, main_app):
        self.main_app = main_app
        self.logger = logging.getLogger(__name__)
        self.metabase_config = None
        self.setup_metabase()
        
        # Цветовая схема для графиков
        self.color_palette = sns.color_palette("husl", 8)
        plt.style.use('seaborn-v0_8-darkgrid')
    
    def setup_metabase(self):
        """Настройка подключения к Metabase"""
        try:
            # Пытаемся загрузить конфигурацию из файла
            config_file = "metabase_config.json"
            if os.path.exists(config_file):
                with open(config_file, 'r') as f:
                    self.metabase_config = json.load(f)
                self.logger.info("Конфигурация Metabase загружена")
            else:
                # Создаем шаблон конфигурации
                self.metabase_config = {
                    "enabled": False,
                    "url": "http://localhost:3000",
                    "username": "admin@example.com",
                    "password": "password123",
                    "database_id": 1,
                    "collection_id": None
                }
                self.logger.info("Используется конфигурация Metabase по умолчанию")
                
        except Exception as e:
            self.logger.error(f"Ошибка загрузки конфигурации Metabase: {e}")
            self.metabase_config = {"enabled": False}
    
    def export_to_excel(self):
        """Экспорт данных визуализации в Excel"""
        try:
            # Временно используем экспорт из основного приложения
            self.main_app.export_orders_to_excel()
        except Exception as e:
            self.logger.error(f"Ошибка экспорта в Excel: {e}")
            messagebox.showerror("Ошибка", f"Не удалось экспортировать данные:\n{str(e)}")
    
    def save_visualization(self):
        """Сохранение графика как изображения"""
        try:
            if not hasattr(self, 'current_figure') or self.current_figure is None:
                messagebox.showwarning("Внимание", "Сначала создайте график")
                return
            
            # Временно сообщаем, что функция в разработке
            messagebox.showinfo("Сохранение", 
                "Функция сохранения графиков как изображения будет реализована в следующей версии.")
                
        except Exception as e:
            self.logger.error(f"Ошибка сохранения графика: {e}")
            messagebox.showerror("Ошибка", f"Не удалось сохранить график:\n{str(e)}")

    def open_metabase(self):
        """Открыть Metabase в браузере"""
        try:
            if self.metabase_config and self.metabase_config.get("enabled"):
                import webbrowser
                url = self.metabase_config.get("url", "http://localhost:3000")
                webbrowser.open(url)
                self.logger.info(f"Открываю Metabase: {url}")
            else:
                messagebox.showwarning("Metabase", 
                    "Интеграция с Metabase отключена. Настройте в конфигурации.")
        except Exception as e:
            self.logger.error(f"Ошибка открытия Metabase: {e}")
            messagebox.showerror("Ошибка", f"Не удалось открыть Metabase:\n{str(e)}")
    def generate_visualization(self):
        """Генерация визуализации"""
        try:
            viz_type = self.viz_type.get()
            period = self.period_var.get()
            
            # Создание графика в зависимости от типа
            if viz_type == "revenue_trend":
                self.create_revenue_trend_chart()
            elif viz_type == "genre_distribution":
                self.create_genre_distribution_chart()
            elif viz_type == "top_customers":
                self.create_top_customers_chart()
            elif viz_type == "order_status":
                self.create_order_status_chart()
            elif viz_type == "seasonality":
                self.create_seasonality_chart()
            elif viz_type == "discount_analysis":
                self.create_discount_analysis_chart()
            else:
                messagebox.showwarning("Внимание", "Выберите тип графика")
            
        except Exception as e:
            self.logger.error(f"Ошибка генерации визуализации: {e}")
            messagebox.showerror("Ошибка", f"Не удалось создать график:\n{str(e)}")

    def create_revenue_trend_chart(self):
        """Создание графика динамики выручки"""
        try:
            # Создание простого графика для демонстрации
            fig, ax = plt.subplots(figsize=(10, 6))
            ax.plot([1, 2, 3, 4, 5], [100, 200, 150, 300, 250], marker='o')
            ax.set_xlabel('Месяцы')
            ax.set_ylabel('Выручка, руб.')
            ax.set_title('Динамика выручки')
            
            # Отображение графика
            self.display_chart(fig)
            
            # Обновление статистики
            self.stats_text.delete(1.0, tk.END)
            self.stats_text.insert(1.0, 
                "Статистика по выручке:\n"
                "Средняя выручка: 200 руб.\n"
                "Максимальная: 300 руб.\n"
                "Минимальная: 100 руб.")
            
        except Exception as e:
            self.logger.error(f"Ошибка создания графика: {e}")
            raise
    
    def display_chart(self, figure):
        """Отображение графика в интерфейсе"""
        if self.chart_canvas:
            self.chart_canvas.get_tk_widget().destroy()
        
        self.current_figure = figure
        self.chart_canvas = FigureCanvasTkAgg(figure, master=self.chart_frame)
        self.chart_canvas.draw()
        self.chart_canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)

    def create_visualization_tab(self, parent):
        """
        Создание вкладки визуализации
        
        Args:
            parent: Родительский виджет
        """
        # Основной фрейм
        main_frame = ttk.Frame(parent)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Панель управления
        control_frame = ttk.LabelFrame(main_frame, text="Управление визуализацией", padding="10")
        control_frame.pack(fill=tk.X, pady=(0, 10))
        
        # Выбор типа визуализации
        ttk.Label(control_frame, text="Тип графика:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)
        self.viz_type = tk.StringVar(value="revenue_trend")
        
        viz_types = [
            ("Динамика выручки", "revenue_trend"),
            ("Распределение по жанрам", "genre_distribution"),
            ("Топ клиентов", "top_customers"),
            ("Статусы заказов", "order_status"),
            ("Сезонность", "seasonality"),
            ("Анализ скидок", "discount_analysis")
        ]
        
        for i, (text, value) in enumerate(viz_types):
            ttk.Radiobutton(control_frame, text=text, variable=self.viz_type, 
                           value=value).grid(row=0, column=i+1, sticky=tk.W, padx=5, pady=5)
        
        # Параметры периода
        period_frame = ttk.Frame(control_frame)
        period_frame.grid(row=1, column=0, columnspan=len(viz_types)+1, sticky=tk.W, pady=5)
        
        self.chart_frame = ttk.LabelFrame(main_frame, text="Визуализация", padding="10")
        self.chart_frame.pack(fill=tk.BOTH, expand=True)
        
        self.chart_canvas = None
        self.current_figure = None

        ttk.Label(period_frame, text="Период:").pack(side=tk.LEFT, padx=5)
        
        self.period_var = tk.StringVar(value="month")
        periods = [("Месяц", "month"), ("Квартал", "quarter"), ("Год", "year"), ("Все время", "all")]
        
        for text, value in periods:
            ttk.Radiobutton(period_frame, text=text, variable=self.period_var, 
                           value=value).pack(side=tk.LEFT, padx=5)
        
        # Кнопки управления
        button_frame = ttk.Frame(control_frame)
        button_frame.grid(row=2, column=0, columnspan=len(viz_types)+1, pady=10)
        
        ttk.Button(button_frame, text="Сгенерировать график", 
                  command=self.generate_visualization).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Экспорт в Excel",
                  command=self.export_to_excel).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Сохранить как изображение", 
                  command=self.save_visualization).pack(side=tk.LEFT, padx=5)
        
        if self.metabase_config.get("enabled"):
            ttk.Button(button_frame, text="Открыть в Metabase", 
                      command=self.open_metabase).pack(side=tk.LEFT, padx=5)
        
        # Область для графика
        chart_frame = ttk.LabelFrame(main_frame, text="Визуализация", padding="10")
        chart_frame.pack(fill=tk.BOTH, expand=True)
        
        self.chart_canvas = None
        self.current_figure = None
        
        # Область для статистики
        stats_frame = ttk.LabelFrame(main_frame, text="Статистика", padding="10")
        stats_frame.pack(fill=tk.X, pady=(10, 0))
        
        self.stats_text = tk.Text(stats_frame, height=6, wrap=tk.WORD)
        stats_scrollbar = ttk.Scrollbar(stats_frame, orient=tk.VERTICAL, command=self.stats_text.yview)
        self.stats_text.configure(yscrollcommand=stats_scrollbar.set)
        
        self.stats_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        stats_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Кнопка настройки Metabase
        if self.metabase_config.get("enabled"):
            ttk.Button(button_frame, text="Открыть в Metabase", 
                      command=self.open_metabase).pack(side=tk.LEFT, padx=5)
            

class CustomerDialog:
    """Диалог для работы с клиентами"""
    
    def __init__(self, parent, title, customer_data=None):
        self.parent = parent
        self.customer_data = customer_data or {}
        self.result = None
        
        self.dialog = tk.Toplevel(parent)
        self.dialog.title(title)
        self.dialog.geometry("500x400")
        self.dialog.transient(parent)
        self.dialog.grab_set()
        
        self.create_widgets()
        self.fill_form()
    
    def create_widgets(self):
        """Создание элементов формы"""
        main_frame = ttk.Frame(self.dialog, padding="20")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Поля формы
        ttk.Label(main_frame, text="ФИО:*").grid(row=0, column=0, sticky=tk.W, pady=5)
        self.name_entry = ttk.Entry(main_frame, width=40)
        self.name_entry.grid(row=0, column=1, sticky=tk.W, pady=5, padx=10)
        
        ttk.Label(main_frame, text="Email:").grid(row=2, column=0, sticky=tk.W, pady=5)
        self.email_entry = ttk.Entry(main_frame, width=40)
        self.email_entry.grid(row=2, column=1, sticky=tk.W, pady=5, padx=10)
        
        ttk.Label(main_frame, text="Телефон:").grid(row=3, column=0, sticky=tk.W, pady=5)
        self.phone_entry = ttk.Entry(main_frame, width=40)
        self.phone_entry.grid(row=3, column=1, sticky=tk.W, pady=5, padx=10)
        
        ttk.Label(main_frame, text="Дата регистрации:").grid(row=4, column=0, sticky=tk.W, pady=5)
        self.reg_date_entry = ttk.Entry(main_frame, width=40)
        self.reg_date_entry.insert(0, date.today().strftime("%Y-%m-%d"))
        self.reg_date_entry.grid(row=4, column=1, sticky=tk.W, pady=5, padx=10)
        
        ttk.Label(main_frame, text="Примечания:").grid(row=5, column=0, sticky=tk.NW, pady=5)
        self.notes_text = tk.Text(main_frame, width=40, height=6)
        self.notes_text.grid(row=5, column=1, sticky=tk.W, pady=5, padx=10)
        
        # Кнопки
        button_frame = ttk.Frame(main_frame)
        button_frame.grid(row=6, column=0, columnspan=2, pady=20)
        
        ttk.Button(button_frame, text="Сохранить", 
                  command=self.save).pack(side=tk.LEFT, padx=10)
        ttk.Button(button_frame, text="Отмена", 
                  command=self.cancel).pack(side=tk.LEFT, padx=10)
    
    def fill_form(self):
        """Заполнение формы данными"""
        if self.customer_data:
            self.name_entry.insert(0, self.customer_data.get('full_name', ''))
            self.email_entry.insert(0, self.customer_data.get('email', ''))
            self.phone_entry.insert(0, self.customer_data.get('phone', ''))
            self.reg_date_entry.delete(0, tk.END)
            self.reg_date_entry.insert(0, self.customer_data.get('registration_date', date.today().strftime("%Y-%m-%d")))
            self.notes_text.insert('1.0', self.customer_data.get('notes', ''))
    
    def validate_form(self):
        """Валидация формы"""
        name = self.name_entry.get().strip()
        if not name:
            messagebox.showerror("Ошибка", "ФИО обязательно для заполнения!")
            return False
        
        reg_date = self.reg_date_entry.get().strip()
        try:
            datetime.strptime(reg_date, "%Y-%m-%d")
        except ValueError:
            messagebox.showerror("Ошибка", "Неверный формат даты. Используйте ГГГГ-ММ-ДД")
            return False
        
        return True
    
    def save(self):
        """Сохранение данных"""
        if self.validate_form():
            self.result = {
                'full_name': self.name_entry.get().strip(),
                'email': self.email_entry.get().strip(),
                'phone': self.phone_entry.get().strip(),
                'registration_date': self.reg_date_entry.get().strip(),
                'notes': self.notes_text.get('1.0', tk.END).strip()
            }
            self.dialog.destroy()
    
    def cancel(self):
        """Отмена"""
        self.dialog.destroy()


class OrderDialog:
    """Диалог для работы с заказами"""
    
    def __init__(self, parent, title, customer, order_data=None):
        self.parent = parent
        self.customer = customer
        self.order_data = order_data or {}
        self.result = None
        
        self.dialog = tk.Toplevel(parent)
        self.dialog.title(f"{title} - {customer['full_name']}")
        self.dialog.geometry("500x450")
        self.dialog.transient(parent)
        self.dialog.grab_set()
        
        self.create_widgets()
        self.fill_form()
    
    def create_widgets(self):
        """Создание элементов формы"""
        main_frame = ttk.Frame(self.dialog, padding="20")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        ttk.Label(main_frame, text=f"Клиент: {self.customer['full_name']}", 
                 font=('Arial', 10, 'bold')).grid(row=0, column=0, columnspan=2, sticky=tk.W, pady=(0, 10))
        
        # Поля формы
        ttk.Label(main_frame, text="Дата заказа:*").grid(row=1, column=0, sticky=tk.W, pady=5)
        self.date_entry = ttk.Entry(main_frame, width=25)
        self.date_entry.insert(0, date.today().strftime("%Y-%m-%d"))
        self.date_entry.grid(row=1, column=1, sticky=tk.W, pady=5, padx=10)
        
        ttk.Label(main_frame, text="Название книги:*").grid(row=2, column=0, sticky=tk.W, pady=5)
        self.book_title_entry = ttk.Entry(main_frame, width=25)
        self.book_title_entry.grid(row=2, column=1, sticky=tk.W, pady=5, padx=10)
        
        ttk.Label(main_frame, text="Автор:").grid(row=3, column=0, sticky=tk.W, pady=5)
        self.author_entry = ttk.Entry(main_frame, width=25)
        self.author_entry.grid(row=3, column=1, sticky=tk.W, pady=5, padx=10)
        
        ttk.Label(main_frame, text="Жанр:").grid(row=4, column=0, sticky=tk.W, pady=5)
        self.genre_entry = ttk.Entry(main_frame, width=25)
        self.genre_entry.grid(row=4, column=1, sticky=tk.W, pady=5, padx=10)
        
        ttk.Label(main_frame, text="Количество:*").grid(row=5, column=0, sticky=tk.W, pady=5)
        self.quantity_entry = ttk.Entry(main_frame, width=25)
        self.quantity_entry.insert(0, "1")
        self.quantity_entry.grid(row=5, column=1, sticky=tk.W, pady=5, padx=10)
        
        ttk.Label(main_frame, text="Цена за шт:*").grid(row=6, column=0, sticky=tk.W, pady=5)
        self.price_entry = ttk.Entry(main_frame, width=25)
        self.price_entry.grid(row=6, column=1, sticky=tk.W, pady=5, padx=10)
        
        ttk.Label(main_frame, text="Скидка %:").grid(row=7, column=0, sticky=tk.W, pady=5)
        self.discount_entry = ttk.Entry(main_frame, width=25)
        self.discount_entry.insert(0, "0")
        self.discount_entry.grid(row=7, column=1, sticky=tk.W, pady=5, padx=10)
        
        ttk.Label(main_frame, text="Статус:").grid(row=8, column=0, sticky=tk.W, pady=5)
        self.status_combo = ttk.Combobox(main_frame, width=22, 
                                       values=["Ожидает оплаты", "Оплачен", "В обработке", "Отправлен", "Завершен", "Отменен"])
        self.status_combo.set("Ожидает оплаты")
        self.status_combo.grid(row=8, column=1, sticky=tk.W, pady=5, padx=10)
        
        ttk.Label(main_frame, text="Способ доставки:").grid(row=9, column=0, sticky=tk.W, pady=5)
        self.delivery_combo = ttk.Combobox(main_frame, width=22,
                                         values=["Самовывоз", "Курьер", "Почта России", "СДЭК"])
        self.delivery_combo.set("Самовывоз")
        self.delivery_combo.grid(row=9, column=1, sticky=tk.W, pady=5, padx=10)
        
        ttk.Label(main_frame, text="Примечание к заказу:").grid(row=10, column=0, sticky=tk.NW, pady=5)
        self.notes_text = tk.Text(main_frame, width=25, height=3)
        self.notes_text.grid(row=10, column=1, sticky=tk.W, pady=5, padx=10)
        
        # Кнопки
        button_frame = ttk.Frame(main_frame)
        button_frame.grid(row=11, column=0, columnspan=2, pady=20)
        
        ttk.Button(button_frame, text="Сохранить", 
                  command=self.save).pack(side=tk.LEFT, padx=10)
        ttk.Button(button_frame, text="Отмена", 
                  command=self.cancel).pack(side=tk.LEFT, padx=10)
    
    def fill_form(self):
        """Заполнение формы данными"""
        if self.order_data:
            self.date_entry.delete(0, tk.END)
            self.date_entry.insert(0, self.order_data.get('date', date.today().strftime("%Y-%m-%d")))
            self.book_title_entry.insert(0, self.order_data.get('book_title', ''))
            self.author_entry.insert(0, self.order_data.get('author', ''))
            self.genre_entry.insert(0, self.order_data.get('genre', ''))
            self.quantity_entry.delete(0, tk.END)
            self.quantity_entry.insert(0, str(self.order_data.get('quantity', 1)))
            self.price_entry.delete(0, tk.END)
            self.price_entry.insert(0, str(self.order_data.get('price', 0)))
            self.discount_entry.delete(0, tk.END)
            self.discount_entry.insert(0, str(self.order_data.get('discount', 0)))
            self.status_combo.set(self.order_data.get('status', 'Ожидает оплаты'))
            self.delivery_combo.set(self.order_data.get('delivery_method', 'Самовывоз'))
            self.notes_text.insert('1.0', self.order_data.get('order_notes', ''))
    
    def validate_form(self):
        """Валидация формы"""
        # Проверка обязательных полей
        required_fields = {
            'Дата заказа': self.date_entry.get().strip(),
            'Название книги': self.book_title_entry.get().strip(),
            'Количество': self.quantity_entry.get().strip(),
            'Цена за шт': self.price_entry.get().strip()
        }
        
        for field_name, value in required_fields.items():
            if not value:
                messagebox.showerror("Ошибка", f"{field_name} обязательно для заполнения!")
                return False
        
        # Валидация даты
        try:
            datetime.strptime(self.date_entry.get().strip(), "%Y-%m-%d")
        except ValueError:
            messagebox.showerror("Ошибка", "Неверный формат даты. Используйте ГГГГ-ММ-ДД")
            return False
        
        # Валидация числовых полей
        try:
            quantity = int(self.quantity_entry.get().strip())
            price = float(self.price_entry.get().strip())
            discount = float(self.discount_entry.get().strip())
            
            if quantity <= 0:
                messagebox.showerror("Ошибка", "Количество должно быть положительным")
                return False
            if price < 0:
                messagebox.showerror("Ошибка", "Цена не может быть отрицательной")
                return False
            if discount < 0 or discount > 100:
                messagebox.showerror("Ошибка", "Скидка должна быть от 0 до 100%")
                return False
                
        except ValueError:
            messagebox.showerror("Ошибка", "Количество, цена и скидка должны быть числами")
            return False
        
        return True
    
    def save(self):
        """Сохранение данных"""
        if self.validate_form():
            self.result = {
                'date': self.date_entry.get().strip(),
                'book_title': self.book_title_entry.get().strip(),
                'author': self.author_entry.get().strip(),
                'genre': self.genre_entry.get().strip(),
                'quantity': int(self.quantity_entry.get().strip()),
                'price': float(self.price_entry.get().strip()),
                'discount': float(self.discount_entry.get().strip()),
                'status': self.status_combo.get(),
                'delivery_method': self.delivery_combo.get(),
                'order_notes': self.notes_text.get('1.0', tk.END).strip()
            }
            self.dialog.destroy()
    
    def cancel(self):
        """Отмена"""
        self.dialog.destroy()


# ========== ОСНОВНОЙ КЛАСС ==========

class CustomerManagementSystem:
    def __init__(self, root):
        self.root = root
        self.root.title("Система управления клиентами и заказами")
        self.root.geometry("1400x800")
        
        # Сначала инициализируем менеджеры
        self.order_manager = OrderManager()
        self.report_generator = ReportGenerator()
        
        # Инициализация новых модулей
        self.excel_importer = ExcelDataImporter(self)
        self.data_viz = DataVisualization(self)
        
        # Инициализация Metabase
        self.metabase_config = self.load_metabase_config()
        self.metabase_integration = None
        self.setup_metabase_integration()
        
        self.setup_database()
        self.create_widgets()
        
        # Автоматическая загрузка данных из CSV
        self.load_data_from_csv()
    
    def load_metabase_config(self) -> Dict[str, Any]:
        """Загрузка конфигурации Metabase"""
        try:
            config_file = "metabase_config.json"
            if os.path.exists(config_file):
                with open(config_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                logging.info("Конфигурация Metabase загружена из файла")
                return config
            else:
                # Конфигурация по умолчанию
                default_config = {
                    "enabled": False,
                    "url": "http://localhost:3000",
                    "username": "admin@example.com",
                    "password": "password123",
                    "database_id": 1,
                    "collection_id": None,
                    "auto_sync": False,
                    "auto_sync_on_save": True
                }
                logging.info("Используется конфигурация Metabase по умолчанию")
                return default_config
        except Exception as e:
            logging.error(f"Ошибка загрузки конфигурации Metabase: {e}")
            return {"enabled": False}
    
    def setup_metabase_integration(self):
        """Настройка интеграции с Metabase"""
        try:
            if self.metabase_config.get("enabled", False):
                self.metabase_integration = MetabaseIntegration(self.metabase_config)
                
                # Пробуем подключиться
                if self.metabase_integration.connect():
                    logging.info("Интеграция с Metabase успешно настроена")
                else:
                    logging.warning("Не удалось подключиться к Metabase")
                    self.metabase_integration = None
        except Exception as e:
            logging.error(f"Ошибка настройки интеграции с Metabase: {e}")
            self.metabase_integration = None
    
    def setup_database(self):
        """Настройка базы данных"""
        self.customers = []
        self.orders = []
        self.next_customer_id = 1
        self.next_order_id = 1
        
        # Инициализация менеджера БД
        self.db_manager = DatabaseManager()
    
    def create_widgets(self):
        """Создание интерфейса"""
        # Создание вкладок
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Вкладка клиентов
        self.customer_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.customer_frame, text="Клиенты")
        
        # Вкладка заказов
        self.orders_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.orders_frame, text="Заказы")
        
        # Вкладка отчетов
        self.reports_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.reports_frame, text="Отчеты")
        
        # Вкладка импорта данных
        self.import_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.import_frame, text="Импорт данных")
        
        # Вкладка визуализации
        self.viz_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.viz_frame, text="Визуализация")
        
        # Вкладка экспорта
        self.export_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.export_frame, text="Экспорт данных")
        
        # Инициализация вкладок
        self.setup_customer_tab()
        self.setup_orders_tab()
        self.setup_reports_tab()
        self.setup_import_tab()
        self.setup_visualization_tab()
        self.setup_export_tab()
    
    def setup_customer_tab(self):
        """Настройка вкладки клиентов"""
        # Панель управления
        control_frame = ttk.Frame(self.customer_frame)
        control_frame.pack(fill=tk.X, pady=(0, 10))
        
        # Кнопки действий
        btn_frame = ttk.Frame(control_frame)
        btn_frame.pack(side=tk.LEFT)
        
        ttk.Button(btn_frame, text="Добавить клиента", 
                  command=self.add_customer).pack(side=tk.LEFT, padx=2)
        ttk.Button(btn_frame, text="Экспорт в CSV", 
                  command=self.export_to_csv).pack(side=tk.LEFT, padx=2)
        ttk.Button(btn_frame, text="Обновить", 
                  command=self.load_customers).pack(side=tk.LEFT, padx=2)
        
        # Поиск
        search_frame = ttk.Frame(control_frame)
        search_frame.pack(side=tk.RIGHT)
        
        ttk.Label(search_frame, text="Поиск:").pack(side=tk.LEFT, padx=(0, 5))
        self.search_entry = ttk.Entry(search_frame, width=20)
        self.search_entry.pack(side=tk.LEFT, padx=5)
        self.search_entry.bind('<KeyRelease>', lambda e: self.search_customers())
        
        # Кнопка синхронизации с БД
        if self.db_manager.connection:
            ttk.Button(btn_frame, text="💾 Сохранить в БД", 
                      command=self.save_to_database).pack(side=tk.LEFT, padx=2)
            ttk.Button(btn_frame, text="📥 Загрузить из БД", 
                      command=self.load_from_database).pack(side=tk.LEFT, padx=2)
        
        # Таблица клиентов
        self.create_customer_table()
    
    def save_to_database(self):
        """Сохранение данных в PostgreSQL"""
        if not self.db_manager.connection:
            messagebox.showwarning("База данных", "Нет подключения к PostgreSQL")
            return
        
        try:
            if self.db_manager.save_to_database(self.customers, self.orders):
                messagebox.showinfo("База данных", "Данные успешно сохранены в PostgreSQL")
                
                # Автоматическая синхронизация с Metabase
                if (self.metabase_integration and 
                    self.metabase_config.get("enabled", False) and
                    self.metabase_config.get("auto_sync_on_save", True)):
                    
                    def sync_after_save():
                        if self.metabase_integration.sync_schema():
                            self.update_status("Данные синхронизированы с Metabase")
                        else:
                            self.update_status("Ошибка синхронизации с Metabase")
                    
                    import threading
                    thread = threading.Thread(target=sync_after_save, daemon=True)
                    thread.start()
                    
            else:
                messagebox.showwarning("База данных", "Не удалось сохранить данные в PostgreSQL")
        except Exception as e:
            messagebox.showerror("База данных", f"Ошибка сохранения: {str(e)}")
    
    def load_from_database(self):
        """Загрузка данных из PostgreSQL"""
        if not self.db_manager.connection:
            messagebox.showwarning("База данных", "Нет подключения к PostgreSQL")
            return
        
        try:
            customers, orders = self.db_manager.load_from_database()
            if customers is not None and orders is not None:
                self.customers = customers
                self.orders = orders
                
                # Обновляем ID счетчики
                if self.customers:
                    self.next_customer_id = max(c['id'] for c in self.customers) + 1
                if self.orders:
                    # Находим максимальный числовой ID
                    numeric_ids = []
                    for order in self.orders:
                        try:
                            if order['id'].startswith('ORD'):
                                numeric_id = int(order['id'][3:])
                                numeric_ids.append(numeric_id)
                        except:
                            pass
                    if numeric_ids:
                        self.next_order_id = max(numeric_ids) + 1
                
                self.load_customers()
                self.update_customer_listbox()
                self.load_orders_for_customer()
                self.update_data_info()
                
                messagebox.showinfo("База данных", 
                                  f"Загружено {len(self.customers)} клиентов и {len(self.orders)} заказов")
            else:
                messagebox.showwarning("База данных", "Не удалось загрузить данные из PostgreSQL")
        except Exception as e:
            messagebox.showerror("База данных", f"Ошибка загрузки: {str(e)}")
    
    def create_customer_table(self):
        """Создание таблицы клиентов"""
        table_frame = ttk.Frame(self.customer_frame)
        table_frame.pack(fill=tk.BOTH, expand=True)
        
        # Создание таблицы
        columns = ('ID', 'ФИО', 'Email', 'Телефон', 'Дата регистрации', 'Всего заказов', 'Общая сумма', 'Примечания')
        self.customer_tree = ttk.Treeview(table_frame, columns=columns, show='headings', height=20)
        
        # Настройка колонок
        column_widths = {
            'ID': 50,
            'ФИО': 180,
            'Email': 150,
            'Телефон': 120,
            'Дата регистрации': 120,
            'Всего заказов': 100,
            'Общая сумма': 120,
            'Примечания': 200
        }
        
        for col in columns:
            self.customer_tree.heading(col, text=col)
            self.customer_tree.column(col, width=column_widths.get(col, 100))
        
        # Полоса прокрутки
        scrollbar = ttk.Scrollbar(table_frame, orient=tk.VERTICAL, command=self.customer_tree.yview)
        self.customer_tree.configure(yscrollcommand=scrollbar.set)
        
        self.customer_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Контекстное меню
        self.create_customer_context_menu()
        
        # Привязка событий
        self.customer_tree.bind('<Double-1>', self.on_customer_double_click)
        self.customer_tree.bind('<Button-3>', self.show_customer_context_menu)
    
    def create_customer_context_menu(self):
        """Создание контекстного меню для клиентов"""
        self.customer_context_menu = tk.Menu(self.root, tearoff=0)
        self.customer_context_menu.add_command(label="Редактировать", command=self.edit_selected_customer)
        self.customer_context_menu.add_command(label="Удалить", command=self.delete_selected_customer)
        self.customer_context_menu.add_separator()
        self.customer_context_menu.add_command(label="Просмотреть заказы", command=self.view_customer_orders)
        self.customer_context_menu.add_command(label="Добавить заказ", command=self.add_order_for_selected_customer)
    
    def show_customer_context_menu(self, event):
        """Показать контекстное меню для клиентов"""
        selection = self.customer_tree.identify_row(event.y)
        if selection:
            self.customer_tree.selection_set(selection)
            self.customer_context_menu.post(event.x_root, event.y_root)
    
    def edit_selected_customer(self):
        """Редактирование выбранного клиента"""
        selection = self.customer_tree.selection()
        if selection:
            customer_id = self.customer_tree.item(selection[0])['values'][0]
            self.edit_customer(customer_id)
    
    def delete_selected_customer(self):
        """Удаление выбранного клиента"""
        selection = self.customer_tree.selection()
        if selection:
            customer_id = self.customer_tree.item(selection[0])['values'][0]
            self.delete_customer(customer_id)
    
    def view_customer_orders(self):
        """Просмотр заказов клиента"""
        selection = self.customer_tree.selection()
        if selection:
            customer_id = self.customer_tree.item(selection[0])['values'][0]
            # Переключение на вкладку заказов
            self.notebook.select(1)
            # Выбор клиента в списке
            customer = self.find_customer_by_id(customer_id)
            if customer:
                customer_index = self.customers.index(customer)
                self.customer_listbox.selection_clear(0, tk.END)
                self.customer_listbox.selection_set(customer_index)
                self.customer_listbox.see(customer_index)
                self.load_orders_for_customer()
    
    def add_order_for_selected_customer(self):
        """Добавление заказа для выбранного клиента"""
        selection = self.customer_tree.selection()
        if selection:
            customer_id = self.customer_tree.item(selection[0])['values'][0]
            # Переключение на вкладку заказов
            self.notebook.select(1)
            # Выбор клиента в списке
            customer = self.find_customer_by_id(customer_id)
            if customer:
                customer_index = self.customers.index(customer)
                self.customer_listbox.selection_clear(0, tk.END)
                self.customer_listbox.selection_set(customer_index)
                self.customer_listbox.see(customer_index)
                self.add_order()
    
    def setup_orders_tab(self):
        """Настройка вкладки заказов"""
        # Основной фрейм
        main_frame = ttk.Frame(self.orders_frame)
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Левая панель - список клиентов
        left_frame = ttk.Frame(main_frame, width=300)
        left_frame.pack(side=tk.LEFT, fill=tk.Y, padx=(0, 10))
        left_frame.pack_propagate(False)
        
        ttk.Label(left_frame, text="Клиенты:", font=('Arial', 10, 'bold')).pack(anchor=tk.W, pady=(0, 5))
        
        # Список клиентов для заказов
        self.customer_listbox = tk.Listbox(left_frame)
        self.customer_listbox.pack(fill=tk.BOTH, expand=True)
        self.customer_listbox.bind('<<ListboxSelect>>', self.on_customer_select)
        
        # Правая панель - заказы выбранного клиента
        right_frame = ttk.Frame(main_frame)
        right_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True)
        
        # Панель управления заказами
        order_control_frame = ttk.Frame(right_frame)
        order_control_frame.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Button(order_control_frame, text="Добавить заказ", 
                  command=self.add_order).pack(side=tk.LEFT, padx=2)
        ttk.Button(order_control_frame, text="Редактировать заказ", 
                  command=self.edit_selected_order).pack(side=tk.LEFT, padx=2)
        ttk.Button(order_control_frame, text="Удалить заказ", 
                  command=self.delete_selected_order).pack(side=tk.LEFT, padx=2)
        
        # Таблица заказов
        self.create_orders_table(right_frame)
        
        # Статистика заказов
        self.setup_order_statistics(right_frame)
    
    def create_orders_table(self, parent):
        """Создание таблицы заказов"""
        table_frame = ttk.Frame(parent)
        table_frame.pack(fill=tk.BOTH, expand=True)
        
        columns = ('ID заказа', 'Дата', 'Название книги', 'Автор', 'Жанр', 'Кол-во', 'Цена', 'Скидка', 'Итог', 'Статус')
        self.orders_tree = ttk.Treeview(table_frame, columns=columns, show='headings', height=15)
        
        column_widths = {
            'ID заказа': 80,
            'Дата': 100,
            'Название книги': 150,
            'Автор': 120,
            'Жанр': 100,
            'Кол-во': 60,
            'Цена': 80,
            'Скидка': 70,
            'Итог': 90,
            'Статус': 120
        }
        
        for col in columns:
            self.orders_tree.heading(col, text=col)
            self.orders_tree.column(col, width=column_widths.get(col, 100))
        
        scrollbar = ttk.Scrollbar(table_frame, orient=tk.VERTICAL, command=self.orders_tree.yview)
        self.orders_tree.configure(yscrollcommand=scrollbar.set)
        
        self.orders_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Контекстное меню для заказов
        self.orders_context_menu = tk.Menu(self.root, tearoff=0)
        self.orders_context_menu.add_command(label="Редактировать заказ", command=self.edit_selected_order)
        self.orders_context_menu.add_command(label="Удалить заказ", command=self.delete_selected_order)
        self.orders_tree.bind('<Button-3>', self.show_orders_context_menu)
    
    def show_orders_context_menu(self, event):
        """Показать контекстное меню для заказов"""
        selection = self.orders_tree.identify_row(event.y)
        if selection:
            self.orders_tree.selection_set(selection)
            self.orders_context_menu.post(event.x_root, event.y_root)
    
    def setup_order_statistics(self, parent):
        """Настройка статистики заказов"""
        stats_frame = ttk.LabelFrame(parent, text="Статистика заказов", padding="10")
        stats_frame.pack(fill=tk.X, pady=(10, 0))
        
        self.stats_vars = {}
        stats_grid = ttk.Frame(stats_frame)
        stats_grid.pack(fill=tk.X)
        
        stats = [
            ("Всего заказов:", "total_orders"),
            ("Общая сумма:", "total_amount"),
            ("Средний заказ:", "avg_order"),
            ("Последний заказ:", "last_order")
        ]
        
        for i, (label, key) in enumerate(stats):
            ttk.Label(stats_grid, text=label).grid(row=i//2, column=(i%2)*2, sticky=tk.W, padx=5, pady=2)
            self.stats_vars[key] = tk.StringVar(value="0")
            ttk.Label(stats_grid, textvariable=self.stats_vars[key]).grid(
                row=i//2, column=(i%2)*2+1, sticky=tk.W, padx=5, pady=2)
    
    def setup_reports_tab(self):
        """Настройка вкладки отчетов"""
        main_frame = ttk.Frame(self.reports_frame)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)
        
        # Выбор типа отчета
        report_type_frame = ttk.LabelFrame(main_frame, text="Тип отчета", padding="10")
        report_type_frame.pack(fill=tk.X, pady=(0, 10))
        
        self.report_type = tk.StringVar(value="customer_summary")
        
        reports = [
            ("Сводка по клиентам", "customer_summary"),
            ("Анализ регистраций", "registration_analysis"),
            ("Статистика заказов", "order_statistics"),
            ("Активность клиентов", "customer_activity")
        ]
        
        for i, (text, value) in enumerate(reports):
            ttk.Radiobutton(report_type_frame, text=text, variable=self.report_type, 
                           value=value).grid(row=i//2, column=i%2, sticky=tk.W, padx=5, pady=2)
        
        # Параметры отчета
        params_frame = ttk.LabelFrame(main_frame, text="Параметры отчета", padding="10")
        params_frame.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Label(params_frame, text="Дата с:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=2)
        self.date_from = ttk.Entry(params_frame, width=12)
        self.date_from.insert(0, (date.today() - timedelta(days=30)).strftime("%Y-%m-%d"))
        self.date_from.grid(row=0, column=1, sticky=tk.W, padx=5, pady=2)
        
        ttk.Label(params_frame, text="Дата по:").grid(row=0, column=2, sticky=tk.W, padx=5, pady=2)
        self.date_to = ttk.Entry(params_frame, width=12)
        self.date_to.insert(0, date.today().strftime("%Y-%m-%d"))
        self.date_to.grid(row=0, column=3, sticky=tk.W, padx=5, pady=2)
        
        # Кнопки генерации
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Button(button_frame, text="Сгенерировать отчет", 
                  command=self.generate_report).pack(side=tk.LEFT, padx=2)
        ttk.Button(button_frame, text="Экспорт в Excel", 
                  command=self.export_report_excel).pack(side=tk.LEFT, padx=2)
        
        # Область отображения отчета
        report_frame = ttk.LabelFrame(main_frame, text="Результат отчета", padding="10")
        report_frame.pack(fill=tk.BOTH, expand=True)
        
        self.report_text = tk.Text(report_frame, wrap=tk.WORD, height=20)
        scrollbar = ttk.Scrollbar(report_frame, orient=tk.VERTICAL, command=self.report_text.yview)
        self.report_text.configure(yscrollcommand=scrollbar.set)
        
        self.report_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
    
    def add_customer(self):
        """Добавление нового клиента"""
        dialog = CustomerDialog(self.root, "Добавить клиента")
        self.root.wait_window(dialog.dialog)
        
        if dialog.result:
            customer_data = dialog.result
            customer_data['id'] = self.next_customer_id
            self.next_customer_id += 1
            customer_data['total_orders'] = 0
            customer_data['total_spent'] = 0.0
            
            self.customers.append(customer_data)
            self.load_customers()
            self.update_customer_listbox()
            self.update_data_info()
            
            messagebox.showinfo("Успех", "Клиент успешно добавлен!")
    
    def edit_customer(self, customer_id):
        """Редактирование клиента"""
        customer = self.find_customer_by_id(customer_id)
        if customer:
            dialog = CustomerDialog(self.root, "Редактировать клиента", customer)
            self.root.wait_window(dialog.dialog)
            
            if dialog.result:
                customer.update(dialog.result)
                self.load_customers()
                self.update_customer_listbox()
                messagebox.showinfo("Успех", "Данные клиента обновлены!")
        else:
            messagebox.showerror("Ошибка", "Клиент не найден!")
    
    def delete_customer(self, customer_id):
        """Удаление клиента"""
        customer = self.find_customer_by_id(customer_id)
        if customer:
            customer_name = customer.get('full_name', 'Неизвестно')
            
            # Проверка наличия заказов
            customer_orders = self.order_manager.get_customer_orders(customer_id, self.orders)
            if customer_orders:
                confirm = messagebox.askyesno(
                    "Удаление клиента", 
                    f"У клиента {customer_name} есть {len(customer_orders)} заказов.\n"
                    f"Все заказы также будут удалены.\n\n"
                    f"Вы уверены, что хотите удалить этого клиента?",
                    icon='warning'
                )
            else:
                confirm = messagebox.askyesno(
                    "Удаление клиента", 
                    f"Вы уверены, что хотите удалить {customer_name}?",
                    icon='warning'
                )
            
            if confirm:
                # Удаление клиента и его заказов
                self.customers = [c for c in self.customers if c['id'] != customer_id]
                self.orders = [o for o in self.orders if o['customer_id'] != customer_id]
                
                self.load_customers()
                self.update_customer_listbox()
                self.load_orders_for_customer()
                self.update_data_info()
                
                messagebox.showinfo("Успех", "Клиент успешно удален!")
        else:
            messagebox.showerror("Ошибка", "Клиент не найден!")
    
    def search_customers(self):
        """Поиск клиентов"""
        query = self.search_entry.get().strip().lower()
        
        if not query:
            self.load_customers()
            return
        
        filtered_customers = []
        for customer in self.customers:
            # Поиск по различным полям
            search_fields = [
                customer.get('full_name', ''),
                customer.get('email', ''),
                customer.get('phone', ''),
                customer.get('notes', '')
            ]
            
            if any(query in str(field).lower() for field in search_fields):
                filtered_customers.append(customer)
        
        self.display_customers(filtered_customers)
    
    def load_customers(self):
        """Загрузка всех клиентов"""
        self.display_customers(self.customers)
        self.update_customer_listbox()
    
    def display_customers(self, customers):
        """Отображение клиентов в таблице"""
        # Очистка таблицы
        for item in self.customer_tree.get_children():
            self.customer_tree.delete(item)
        
        # Заполнение данными
        for customer in customers:
            customer_orders = self.order_manager.get_customer_orders(customer['id'], self.orders)
            total_spent = self.order_manager.get_customer_total_spent(customer['id'], self.orders)
            
            self.customer_tree.insert('', tk.END, values=(
                customer['id'],
                customer.get('full_name', ''),
                customer.get('email', ''),
                customer.get('phone', ''),
                customer.get('registration_date', ''),
                len(customer_orders),
                f"{total_spent:.2f} руб.",
                customer.get('notes', '')[:50] + '...' if customer.get('notes', '') and len(customer.get('notes', '')) > 50 else customer.get('notes', '')
            ))
    
    def export_to_csv(self):
        """Экспорт данных в CSV"""
        if not self.customers:
            messagebox.showwarning("Предупреждение", "Нет клиентов для экспорта!")
            return
        
        try:
            file_path = filedialog.asksaveasfilename(
                defaultextension=".csv",
                filetypes=[("CSV files", "*.csv"), ("Все файлы", "*.*")],
                title="Экспорт клиентов в CSV"
            )
            
            if file_path:
                df = pd.DataFrame([{
                    'ID': c['id'],
                    'ФИО': c.get('full_name', ''),
                    'Email': c.get('email', ''),
                    'Телефон': c.get('phone', ''),
                    'Дата регистрации': c.get('registration_date', ''),
                    'Всего заказов': len(self.order_manager.get_customer_orders(c['id'], self.orders)),
                    'Общая сумма': self.order_manager.get_customer_total_spent(c['id'], self.orders),
                    'Примечания': c.get('notes', '')
                } for c in self.customers])
                
                df.to_csv(file_path, index=False, encoding='utf-8')
                messagebox.showinfo("Успех", f"Клиенты экспортированы в:\n{file_path}")
        
        except Exception as e:
            logging.error(f"Ошибка экспорта: {e}")
            messagebox.showerror("Ошибка", f"Не удалось экспортировать: {e}")
    
    def add_order(self):
        """Добавление нового заказа"""
        selected_customer = self.get_selected_customer_from_listbox()
        if not selected_customer:
            messagebox.showwarning("Предупреждение", "Сначала выберите клиента!")
            return
        
        dialog = OrderDialog(self.root, "Добавить заказ", selected_customer)
        self.root.wait_window(dialog.dialog)
        
        if dialog.result:
            order_data = dialog.result
            order_data['id'] = f"ORD{self.next_order_id:03d}"
            order_data['customer_id'] = selected_customer['id']
            order_data['customer_name'] = selected_customer['full_name']
            order_data['total_amount'] = order_data['quantity'] * order_data['price'] * (1 - order_data['discount'] / 100)
            order_data['final_price'] = order_data['price'] * (1 - order_data['discount'] / 100)
            self.next_order_id += 1
            
            self.orders.append(order_data)
            self.load_orders_for_customer()
            self.load_customers()  # Обновляем статистику в таблице клиентов
            self.update_data_info()
            
            messagebox.showinfo("Успех", "Заказ успешно добавлен!")
    
    def edit_selected_order(self):
        """Редактирование выбранного заказа"""
        selected_order = self.get_selected_order()
        if not selected_order:
            messagebox.showwarning("Предупреждение", "Сначала выберите заказ!")
            return
        
        customer = self.find_customer_by_id(selected_order['customer_id'])
        dialog = OrderDialog(self.root, "Редактировать заказ", customer, selected_order)
        self.root.wait_window(dialog.dialog)
        
        if dialog.result:
            selected_order.update(dialog.result)
            selected_order['total_amount'] = selected_order['quantity'] * selected_order['price'] * (1 - selected_order['discount'] / 100)
            selected_order['final_price'] = selected_order['price'] * (1 - selected_order['discount'] / 100)
            self.load_orders_for_customer()
            self.load_customers()
            
            messagebox.showinfo("Успех", "Заказ успешно обновлен!")
    
    def delete_selected_order(self):
        """Удаление выбранного заказа"""
        selected_order = self.get_selected_order()
        if not selected_order:
            messagebox.showwarning("Предупреждение", "Сначала выберите заказ!")
            return
        
        confirm = messagebox.askyesno(
            "Удаление заказа", 
            f"Вы уверены, что хотите удалить этот заказ?",
            icon='warning'
        )
        
        if confirm:
            self.orders = [o for o in self.orders if o['id'] != selected_order['id']]
            self.load_orders_for_customer()
            self.load_customers()
            self.update_data_info()
            
            messagebox.showinfo("Успех", "Заказ успешно удален!")
    
    def load_orders_for_customer(self):
        """Загрузка заказов для выбранного клиента"""
        selected_customer = self.get_selected_customer_from_listbox()
        if not selected_customer:
            return
        
        customer_orders = self.order_manager.get_customer_orders(selected_customer['id'], self.orders)
        
        # Очистка таблицы
        for item in self.orders_tree.get_children():
            self.orders_tree.delete(item)
        
        # Заполнение данными
        for order in customer_orders:
            self.orders_tree.insert('', tk.END, values=(
                order['id'],
                order.get('date', ''),
                order.get('book_title', ''),
                order.get('author', ''),
                order.get('genre', ''),
                order.get('quantity', 0),
                f"{order.get('price', 0):.2f} руб.",
                f"{order.get('discount', 0):.1f}%",
                f"{order.get('total_amount', 0):.2f} руб.",
                order.get('status', 'Ожидает оплаты')
            ))
        
        # Обновление статистики
        self.update_order_statistics(selected_customer['id'])
    
    def update_order_statistics(self, customer_id):
        """Обновление статистики заказов"""
        stats = self.order_manager.get_customer_order_statistics(customer_id, self.orders)
        
        self.stats_vars['total_orders'].set(str(stats['total_orders']))
        self.stats_vars['total_amount'].set(f"{stats['total_amount']:.2f} руб.")
        self.stats_vars['avg_order'].set(f"{stats['average_order']:.2f} руб.")
        self.stats_vars['last_order'].set(stats['last_order'] or "Нет заказов")
    
    def generate_report(self):
        """Генерация отчета"""
        try:
            report_type = self.report_type.get()
            date_from = self.date_from.get()
            date_to = self.date_to.get()
            
            report_data = self.report_generator.generate_report(
                report_type, self.customers, self.orders, date_from, date_to
            )
            
            self.report_text.delete(1.0, tk.END)
            self.report_text.insert(1.0, report_data)
            
        except Exception as e:
            messagebox.showerror("Ошибка", f"Не удалось сгенерировать отчет: {e}")
    
    def export_report_excel(self):
        """Экспорт отчета в Excel"""
        try:
            file_path = filedialog.asksaveasfilename(
                defaultextension=".xlsx",
                filetypes=[("Excel files", "*.xlsx"), ("Все файлы", "*.*")],
                title="Экспорт отчета в Excel"
            )
            
            if file_path:
                # Создание DataFrame с данными отчета
                df = pd.DataFrame([{
                    'ID': c['id'],
                    'ФИО': c.get('full_name', ''),
                    'Email': c.get('email', ''),
                    'Телефон': c.get('phone', ''),
                    'Дата регистрации': c.get('registration_date', ''),
                    'Всего заказов': len(self.order_manager.get_customer_orders(c['id'], self.orders)),
                    'Общая сумма': self.order_manager.get_customer_total_spent(c['id'], self.orders)
                } for c in self.customers])
                
                df.to_excel(file_path, index=False)
                messagebox.showinfo("Успех", f"Отчет экспортирован в:\n{file_path}")
        
        except Exception as e:
            messagebox.showerror("Ошибка", f"Не удалось экспортировать: {e}")
    
    def find_customer_by_id(self, customer_id):
        """Поиск клиента по ID"""
        for customer in self.customers:
            if customer['id'] == customer_id:
                return customer
        return None
    
    def get_selected_customer_from_listbox(self):
        """Получение выбранного клиента из списка"""
        selection = self.customer_listbox.curselection()
        if selection:
            customer_name = self.customer_listbox.get(selection[0])
            customer_id = int(customer_name.split(" - ")[0])
            return self.find_customer_by_id(customer_id)
        return None
    
    def get_selected_order(self):
        """Получение выбранного заказа"""
        selection = self.orders_tree.selection()
        if selection:
            order_id = self.orders_tree.item(selection[0])['values'][0]
            for order in self.orders:
                if order['id'] == order_id:
                    return order
        return None
    
    def update_customer_listbox(self):
        """Обновление списка клиентов"""
        self.customer_listbox.delete(0, tk.END)
        for customer in self.customers:
            self.customer_listbox.insert(tk.END, f"{customer['id']} - {customer['full_name']}")
    
    def on_customer_select(self, event):
        """Обработка выбора клиента в списке"""
        self.load_orders_for_customer()
    
    def on_customer_double_click(self, event):
        """Обработка двойного клика по клиенту"""
        selection = self.customer_tree.selection()
        if selection:
            customer_id = self.customer_tree.item(selection[0])['values'][0]
            self.edit_customer(customer_id)
    
    def load_data_from_csv(self):
        """Загрузка данных из CSV файлов"""
        try:
            # Загрузка клиентов из clients_100.csv
            clients_file = "clients_100.csv"
            if os.path.exists(clients_file):
                self.load_customers_from_csv(clients_file)
            else:
                logging.warning(f"Файл {clients_file} не найден. Используются тестовые данные.")
                self.load_sample_customers()
            
            # Загрузка заказов из book_orders.csv
            orders_file = "book_orders.csv"
            if os.path.exists(orders_file):
                self.load_orders_from_csv(orders_file)
            else:
                logging.warning(f"Файл {orders_file} не найден. Используются тестовые данные.")
                self.load_sample_orders()
            
            self.load_customers()
            
        except Exception as e:
            logging.error(f"Ошибка загрузки CSV данных: {e}")
            messagebox.showerror("Ошибка", f"Не удалось загрузить данные из CSV: {e}")
            # Загрузка тестовых данных в случае ошибки
            self.load_sample_data()
    
    def load_customers_from_csv(self, file_path):
        """Загрузка клиентов из CSV файла"""
        try:
            df = pd.read_csv(file_path, encoding='utf-8')
            logging.info(f"Загрузка клиентов из {file_path}, найдено {len(df)} записей")
            
            self.customers = []
            self.next_customer_id = 1
            
            for index, row in df.iterrows():
                customer = {
                    'id': self.next_customer_id,
                    'full_name': self.clean_string(row.get('ФИО', row.get('full_name', ''))),
                    'email': self.clean_string(row.get('Email', row.get('email', ''))),
                    'phone': self.clean_string(row.get('Телефон', row.get('phone', ''))),
                    'registration_date': self.parse_date(row.get('Дата регистрации', row.get('registration_date', date.today()))),
                    'notes': self.clean_string(row.get('Примечания', row.get('notes', ''))),
                    'total_orders': 0,
                    'total_spent': 0.0
                }
                
                # Пропускаем пустые записи
                if customer['full_name']:
                    self.customers.append(customer)
                    self.next_customer_id += 1
            
            logging.info(f"Успешно загружено {len(self.customers)} клиентов")
            
        except Exception as e:
            logging.error(f"Ошибка загрузки клиентов из CSV: {e}")
            raise
    
    def load_orders_from_csv(self, file_path):
        """Загрузка заказов из CSV файла"""
        try:
            df = pd.read_csv(file_path, encoding='utf-8')
            logging.info(f"Загрузка заказов из {file_path}, найдено {len(df)} записей")
            
            self.orders = []
            self.next_order_id = 1
            
            for index, row in df.iterrows():
                # Ищем клиента по имени
                customer_name = self.clean_string(row.get('ФИО_клиента', row.get('client_name', '')))
                customer = self.find_customer_by_name(customer_name)
                
                if customer:
                    order = {
                        'id': self.clean_string(row.get('ID_заказа', row.get('order_id', f'ORD{self.next_order_id:03d}'))),
                        'customer_id': customer['id'],
                        'customer_name': customer_name,
                        'date': self.parse_date(row.get('Дата_заказа', row.get('order_date', date.today()))),
                        'book_title': self.clean_string(row.get('Название_книги', row.get('product_name', ''))),
                        'author': self.clean_string(row.get('Автор', '')),
                        'genre': self.clean_string(row.get('Жанр', '')),
                        'quantity': self.parse_int(row.get('Количество', row.get('quantity', 1))),
                        'price': self.parse_float(row.get('Цена_за_шт', row.get('price', 0))),
                        'discount': self.parse_float(row.get('Скидка_%', row.get('discount', 0))),
                        'final_price': self.parse_float(row.get('Итоговая_цена', row.get('final_price', 0))),
                        'total_amount': self.parse_float(row.get('Общая_сумма', row.get('total_amount', 0))),
                        'status': self.clean_string(row.get('Статус_заказа', row.get('status', 'Ожидает оплаты'))),
                        'delivery_method': self.clean_string(row.get('Способ_доставки', row.get('delivery_method', ''))),
                        'order_notes': self.clean_string(row.get('Примечание_к_заказу', row.get('notes', '')))
                    }
                    
                    self.orders.append(order)
                    self.next_order_id += 1
                else:
                    logging.warning(f"Клиент не найден для заказа: {customer_name}")
            
            logging.info(f"Успешно загружено {len(self.orders)} заказов")
            
        except Exception as e:
            logging.error(f"Ошибка загрузки заказов из CSV: {e}")
            raise
    
    def clean_string(self, value):
        """Очистка строковых значений"""
        if pd.isna(value):
            return ""
        return str(value).strip()
    
    def parse_date(self, value):
        """Парсинг даты из различных форматов"""
        if pd.isna(value):
            return date.today().strftime("%Y-%m-%d")
        
        value_str = str(value).strip()
        
        # Пробуем разные форматы дат
        date_formats = [
            "%Y-%m-%d",
            "%d.%m.%Y",
            "%m/%d/%Y",
            "%d-%m-%Y",
            "%Y/%m/%d"
        ]
        
        for fmt in date_formats:
            try:
                date_obj = datetime.strptime(value_str, fmt)
                return date_obj.strftime("%Y-%m-%d")
            except ValueError:
                continue
        
        # Если не удалось распарсить, используем сегодняшнюю дату
        logging.warning(f"Не удалось распарсить дату: {value_str}, используется сегодняшняя дата")
        return date.today().strftime("%Y-%m-%d")
    
    def parse_int(self, value):
        """Парсинг целых чисел"""
        if pd.isna(value):
            return 1
        
        try:
            return int(float(value))
        except (ValueError, TypeError):
            logging.warning(f"Не удалось распарсить целое число: {value}, используется 1")
            return 1
    
    def parse_float(self, value):
        """Парсинг дробных чисел"""
        if pd.isna(value):
            return 0.0
        
        try:
            return float(value)
        except (ValueError, TypeError):
            logging.warning(f"Не удалось распарсить дробное число: {value}, используется 0.0")
            return 0.0
    
    def find_customer_by_name(self, name):
        """Поиск клиента по имени"""
        clean_name = self.clean_string(name).lower()
        for customer in self.customers:
            if customer['full_name'].lower() == clean_name:
                return customer
        return None
    
    def load_sample_customers(self):
        """Загрузка тестовых клиентов"""
        sample_customers = [
            {
                'id': 1,
                'full_name': 'Киселев Любомир Адамович',
                'email': 'киселевл@mail.ru',
                'phone': '+79876143194',
                'registration_date': '2024-09-26',
                'notes': 'Работает по предоплате',
                'total_orders': 0,
                'total_spent': 0.0
            },
            {
                'id': 2,
                'full_name': 'Фокин Гостомысл Ильясович',
                'email': 'foking@mail.ru',
                'phone': '+79876143195',
                'registration_date': '2024-09-25',
                'notes': 'Постоянный клиент',
                'total_orders': 0,
                'total_spent': 0.0
            }
        ]
        self.customers = sample_customers
        self.next_customer_id = 3
    
    def load_sample_orders(self):
        """Загрузка тестовых заказов"""
        sample_orders = [
            {
                'id': 'ORD001',
                'customer_id': 1,
                'customer_name': 'Киселев Любомир Адамович',
                'date': '2024-09-27',
                'book_title': 'Мастер и Маргарита',
                'author': 'Михаил Булгаков',
                'genre': 'Роман',
                'quantity': 1,
                'price': 450.0,
                'discount': 10.0,
                'final_price': 405.0,
                'total_amount': 405.0,
                'status': 'Завершен',
                'delivery_method': 'Самовывоз',
                'order_notes': 'Предоплата 100%'
            },
            {
                'id': 'ORD002',
                'customer_id': 2,
                'customer_name': 'Фокин Гостомысл Ильясович',
                'date': '2024-09-26',
                'book_title': 'Маленький принц',
                'author': 'Антуан де Сент-Экзюпери',
                'genre': 'Философия',
                'quantity': 3,
                'price': 320.0,
                'discount': 15.0,
                'final_price': 272.0,
                'total_amount': 816.0,
                'status': 'Ожидает оплаты',
                'delivery_method': 'Самовывоз',
                'order_notes': 'Со скидочной картой'
            }
        ]
        self.orders = sample_orders
        self.next_order_id = 3
    
    def load_sample_data(self):
        """Загрузка тестовых данных (для обратной совместимости)"""
        self.load_sample_customers()
        self.load_sample_orders()
    
    def setup_import_tab(self):
        """Настройка вкладки импорта данных"""
        main_frame = ttk.Frame(self.import_frame, padding="20")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Информация о текущих данных
        info_frame = ttk.LabelFrame(main_frame, text="Текущие данные", padding="10")
        info_frame.pack(fill=tk.X, pady=(0, 20))
        
        self.data_info_var = tk.StringVar()
        self.data_info_var.set("Данные не загружены")
        ttk.Label(info_frame, textvariable=self.data_info_var).pack(anchor=tk.W)
        
        # Кнопки импорта
        import_frame = ttk.LabelFrame(main_frame, text="Импорт данных", padding="10")
        import_frame.pack(fill=tk.X, pady=(0, 20))
        
        ttk.Button(import_frame, text="Импорт из Excel (клиенты)", 
                  command=self.excel_importer.import_customers_from_excel).pack(fill=tk.X, pady=5)
        ttk.Button(import_frame, text="Импорт из Excel (заказы)", 
                  command=self.excel_importer.import_orders_from_excel).pack(fill=tk.X, pady=5)
        ttk.Button(import_frame, text="Импорт всех данных из Excel", 
                  command=self.excel_importer.import_all_data).pack(fill=tk.X, pady=5)
        ttk.Button(import_frame, text="Загрузить из CSV (клиенты)", 
                  command=self.manual_load_customers).pack(fill=tk.X, pady=5)
        ttk.Button(import_frame, text="Загрузить из CSV (заказы)", 
                  command=self.manual_load_orders).pack(fill=tk.X, pady=5)
        ttk.Button(import_frame, text="Перезагрузить все данные", 
                  command=self.reload_all_data).pack(fill=tk.X, pady=5)
        
        # Интеграция с Metabase
        if self.metabase_config.get("enabled", False):
            metabase_frame = ttk.LabelFrame(main_frame, text="Интеграция с Metabase", padding="10")
            metabase_frame.pack(fill=tk.X, pady=(0, 20))
            
            # Кнопки управления Metabase
            metabase_btn_frame = ttk.Frame(metabase_frame)
            metabase_btn_frame.pack(fill=tk.X, pady=(10, 0))
            
            ttk.Button(metabase_btn_frame, text="🔄 Синхронизировать с Metabase", 
                      command=self.sync_with_metabase).pack(side=tk.LEFT, padx=2)
            
            ttk.Button(metabase_btn_frame, text="📊 Создать дашборд", 
                      command=self.create_dashboard_in_metabase).pack(side=tk.LEFT, padx=2)
        
        # Обновляем информацию о данных
        self.update_data_info()
    
    def sync_with_metabase(self):
        """Синхронизация данных с Metabase (ручной запуск)"""
        if not self.metabase_integration:
            messagebox.showwarning("Metabase", "Интеграция с Metabase не настроена")
            return
        
        try:
            # Сохраняем данные в БД перед синхронизацией
            if self.db_manager.connection:
                self.db_manager.save_to_database(self.customers, self.orders)
            
            # Синхронизируем схему
            if self.metabase_integration.sync_schema():
                messagebox.showinfo("Metabase", "Данные успешно синхронизированы с Metabase")
                
                # Предлагаем открыть Metabase
                if messagebox.askyesno("Metabase", "Хотите открыть Metabase в браузере?"):
                    webbrowser.open(self.metabase_config.get("url", "http://localhost:3000"))
            else:
                messagebox.showwarning("Metabase", "Не удалось синхронизировать данные с Metabase")
                
        except Exception as e:
            logging.error(f"Ошибка синхронизации с Metabase: {e}")
            messagebox.showerror("Metabase", f"Ошибка синхронизации: {str(e)}")
    
    def create_dashboard_in_metabase(self):
        """Создание дашборда в Metabase"""
        if not self.metabase_integration:
            messagebox.showwarning("Metabase", "Интеграция с Metabase не настроена")
            return
        
        try:
            dashboard_id = self.metabase_integration.create_dashboard(
                name="Система управления клиентами",
                description="Дашборд для анализа клиентов и заказов"
            )
            
            if dashboard_id:
                dashboard_url = self.metabase_integration.get_dashboard_url(dashboard_id)
                messagebox.showinfo("Metabase", f"Дашборд создан успешно!\n\nURL: {dashboard_url}")
                
                # Открываем дашборд в браузере
                if messagebox.askyesno("Metabase", "Открыть дашборд в браузере?"):
                    webbrowser.open(dashboard_url)
            else:
                messagebox.showwarning("Metabase", "Не удалось создать дашборд")
                
        except Exception as e:
            logging.error(f"Ошибка создания дашборда: {e}")
            messagebox.showerror("Metabase", f"Ошибка создания дашборда: {str(e)}")
    
    def manual_load_customers(self):
        """Ручная загрузка клиентов из CSV"""
        file_path = filedialog.askopenfilename(
            title="Выберите CSV файл с клиентами",
            filetypes=[("CSV files", "*.csv"), ("Все файлы", "*.*")]
        )
        
        if file_path:
            try:
                self.load_customers_from_csv(file_path)
                self.load_customers()
                self.update_data_info()
                messagebox.showinfo("Успех", f"Загружено {len(self.customers)} клиентов из {file_path}")
            except Exception as e:
                messagebox.showerror("Ошибка", f"Не удалось загрузить клиентов: {e}")
    
    def manual_load_orders(self):
        """Ручная загрузка заказов из CSV"""
        file_path = filedialog.askopenfilename(
            title="Выберите CSV файл с заказами",
            filetypes=[("CSV files", "*.csv"), ("Все файлы", "*.*")]
        )
        
        if file_path:
            try:
                self.load_orders_from_csv(file_path)
                self.load_orders_for_customer()
                self.update_data_info()
                messagebox.showinfo("Успех", f"Загружено {len(self.orders)} заказов из {file_path}")
            except Exception as e:
                messagebox.showerror("Ошибка", f"Не удалось загрузить заказы: {e}")
    
    def reload_all_data(self):
        """Перезагрузка всех данных"""
        try:
            self.setup_database()  # Сброс данных
            self.load_data_from_csv()
            self.update_data_info()
            messagebox.showinfo("Успех", "Все данные перезагружены успешно")
        except Exception as e:
            messagebox.showerror("Ошибка", f"Не удалось перезагрузить данные: {e}")
    
    def update_data_info(self):
        """Обновление информации о данных"""
        info = f"Клиентов: {len(self.customers)} | Заказов: {len(self.orders)}"
        
        # Проверяем существование файлов
        clients_exists = os.path.exists("clients_100.csv")
        orders_exists = os.path.exists("book_orders.csv")
        
        files_info = []
        if clients_exists:
            files_info.append("clients_100.csv ✓")
        else:
            files_info.append("clients_100.csv ✗")
        
        if orders_exists:
            files_info.append("book_orders.csv ✓")
        else:
            files_info.append("book_orders.csv ✗")
        
        info += f" | Файлы: {', '.join(files_info)}"
        self.data_info_var.set(info)
    
    def setup_visualization_tab(self):
        """Настройка вкладки визуализации"""
        self.data_viz.create_visualization_tab(self.viz_frame)
    
    def setup_export_tab(self):
        """Настройка вкладки экспорта"""
        main_frame = ttk.Frame(self.export_frame, padding="20")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Разделы экспорта
        sections = [
            ("Экспорт клиентов", "Экспортировать данные клиентов в Excel", 
             self.export_to_csv),
            ("Экспорт заказов", "Экспортировать все заказы в Excel", 
             self.export_orders_to_excel),
            ("Отчет по статистике", "Экспорт статистики в Excel", 
             self.export_statistics_report)
        ]
        
        for i, (title, description, command) in enumerate(sections):
            section_frame = ttk.LabelFrame(main_frame, text=title, padding="10")
            section_frame.pack(fill=tk.X, pady=(0, 10))
            
            ttk.Label(section_frame, text=description).pack(anchor=tk.W, pady=(0, 10))
            ttk.Button(section_frame, text="Экспортировать", 
                      command=command).pack(anchor=tk.E)
        
        # Настройки экспорта
        settings_frame = ttk.LabelFrame(main_frame, text="Настройки экспорта", padding="10")
        settings_frame.pack(fill=tk.X)
        
        self.export_with_charts = tk.BooleanVar(value=True)
        ttk.Checkbutton(settings_frame, text="Включать графики в отчет", 
                       variable=self.export_with_charts).pack(anchor=tk.W, pady=5)
        
        self.export_format = tk.StringVar(value="xlsx")
        ttk.Label(settings_frame, text="Формат файла:").pack(anchor=tk.W, pady=5)
        
        format_frame = ttk.Frame(settings_frame)
        format_frame.pack(fill=tk.X, pady=5)
        
        formats = [("Excel (.xlsx)", "xlsx"), ("CSV", "csv"), ("PDF", "pdf")]
        for text, value in formats:
            ttk.Radiobutton(format_frame, text=text, variable=self.export_format, 
                           value=value).pack(side=tk.LEFT, padx=10)
    
    def export_orders_to_excel(self):
        """Экспорт заказов в Excel"""
        if not self.orders:
            messagebox.showwarning("Предупреждение", "Нет заказов для экспорта.")
            return
        
        try:
            file_path = filedialog.asksaveasfilename(
                defaultextension=".xlsx",
                filetypes=[("Excel files", "*.xlsx"), ("Все файлы", "*.*")],
                title="Экспорт заказов"
            )
            
            if file_path:
                # Подготовка данных заказов
                orders_data = []
                for order in self.orders:
                    orders_data.append({
                        'ID заказа': order.get('id', ''),
                        'ID клиента': order.get('customer_id', ''),
                        'Клиент': order.get('customer_name', ''),
                        'Дата заказа': order.get('date', ''),
                        'Название книги': order.get('book_title', ''),
                        'Автор': order.get('author', ''),
                        'Жанр': order.get('genre', ''),
                        'Количество': order.get('quantity', 0),
                        'Цена за шт': order.get('price', 0),
                        'Скидка %': order.get('discount', 0),
                        'Итоговая цена': order.get('final_price', 0),
                        'Общая сумма': order.get('total_amount', 0),
                        'Статус': order.get('status', ''),
                        'Способ доставки': order.get('delivery_method', ''),
                        'Примечание': order.get('order_notes', '')
                    })
                
                df = pd.DataFrame(orders_data)
                df.to_excel(file_path, index=False)
                
                messagebox.showinfo("Успех", f"Заказы экспортированы в:\n{file_path}")
        
        except Exception as e:
            logging.error(f"Ошибка экспорта заказов: {e}")
            messagebox.showerror("Ошибка", f"Не удалось экспортировать заказы:\n{str(e)}")
    
    def export_statistics_report(self):
        """Экспорт статистического отчета"""
        try:
            file_path = filedialog.asksaveasfilename(
                defaultextension=".xlsx",
                filetypes=[("Excel files", "*.xlsx"), ("Все файлы", "*.*")],
                title="Экспорт статистического отчета"
            )
            
            if file_path:
                with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                    # 1. Общая статистика
                    general_stats = pd.DataFrame([{
                        'Показатель': 'Всего клиентов',
                        'Значение': len(self.customers)
                    }, {
                        'Показатель': 'Клиентов с заказами',
                        'Значение': len(set(o['customer_id'] for o in self.orders))
                    }, {
                        'Показатель': 'Всего заказов',
                        'Значение': len(self.orders)
                    }, {
                        'Показатель': 'Общая выручка',
                        'Значение': sum(o.get('total_amount', 0) for o in self.orders)
                    }, {
                        'Показатель': 'Средний чек',
                        'Значение': sum(o.get('total_amount', 0) for o in self.orders) / len(self.orders) if self.orders else 0
                    }, {
                        'Показатель': 'Среднее количество товаров в заказе',
                        'Значение': sum(o.get('quantity', 0) for o in self.orders) / len(self.orders) if self.orders else 0
                    }])
                    
                    general_stats.to_excel(writer, sheet_name='Общая статистика', index=False)
                    
                    # 2. Статистика по статусам заказов
                    if self.orders:
                        status_stats = pd.DataFrame(self.orders)
                        if 'status' in status_stats.columns:
                            status_summary = status_stats['status'].value_counts().reset_index()
                            status_summary.columns = ['Статус', 'Количество']
                            status_summary.to_excel(writer, sheet_name='Статусы заказов', index=False)
                    
                    # 3. Топ-10 клиентов
                    if self.orders:
                        customer_stats = []
                        for customer in self.customers:
                            customer_orders = self.order_manager.get_customer_orders(customer['id'], self.orders)
                            if customer_orders:
                                total_spent = self.order_manager.get_customer_total_spent(customer['id'], self.orders)
                                customer_stats.append({
                                    'ID': customer['id'],
                                    'ФИО': customer.get('full_name', ''),
                                    'Количество заказов': len(customer_orders),
                                    'Общая сумма': total_spent,
                                    'Средний чек': total_spent / len(customer_orders),
                                    'Последний заказ': max([o['date'] for o in customer_orders]) if customer_orders else ''
                                })
                        
                        if customer_stats:
                            top_customers = pd.DataFrame(customer_stats)
                            top_customers = top_customers.sort_values('Общая сумма', ascending=False).head(10)
                            top_customers.to_excel(writer, sheet_name='Топ-10 клиентов', index=False)
                    
                    # 4. Статистика по жанрам
                    if self.orders:
                        genre_data = []
                        for order in self.orders:
                            genre = order.get('genre', 'Не указан')
                            genre_data.append({
                                'Жанр': genre,
                                'Сумма': order.get('total_amount', 0),
                                'Количество': order.get('quantity', 1)
                            })
                        
                        if genre_data:
                            genre_df = pd.DataFrame(genre_data)
                            genre_summary = genre_df.groupby('Жанр').agg({
                                'Сумма': ['sum', 'count', 'mean'],
                                'Количество': 'sum'
                            }).round(2)
                            genre_summary.to_excel(writer, sheet_name='Статистика по жанрам')
                
                messagebox.showinfo("Успех", f"Статистический отчет экспортирован в:\n{file_path}")
        
        except Exception as e:
            logging.error(f"Ошибка экспорта статистического отчета: {e}")
            messagebox.showerror("Ошибка", f"Не удалось экспортировать отчет:\n{str(e)}")


if __name__ == "__main__":
    root = tk.Tk()
    app = CustomerManagementSystem(root)
    root.mainloop()