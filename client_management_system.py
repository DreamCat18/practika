import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import pandas as pd
from datetime import datetime, date, timedelta
import logging
import os
from typing import List, Dict, Any

class CustomerManagementSystem:
    def __init__(self, root):
        self.root = root
        self.root.title("Система управления клиентами и заказами")
        self.root.geometry("1400x800")
        
        # Сначала инициализируем менеджеры
        self.order_manager = OrderManager()
        self.report_generator = ReportGenerator()
        
        self.setup_database()
        self.create_widgets()
        
        # Автоматическая загрузка данных из CSV
        self.load_data_from_csv()
    
    def setup_database(self):
        """Настройка базы данных"""
        self.customers = []
        self.orders = []
        self.next_customer_id = 1
        self.next_order_id = 1
    
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
            df = pd.read_csv(file_path)
            logging.info(f"Загрузка клиентов из {file_path}, найдено {len(df)} записей")
            
            self.customers = []
            self.next_customer_id = 1
            
            for index, row in df.iterrows():
                customer = {
                    'id': self.next_customer_id,
                    'full_name': self.clean_string(row.get('ФИО', row.get('full_name', ''))),
                    'contact_info': self.clean_string(row.get('Контактная информация', '')),
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
            df = pd.read_csv(file_path)
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
        
        # Инициализация вкладок
        self.setup_customer_tab()
        self.setup_orders_tab()
        self.setup_reports_tab()
        self.setup_import_tab()
    
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
        
        ttk.Button(import_frame, text="Загрузить клиентов из CSV", 
                  command=self.manual_load_customers).pack(fill=tk.X, pady=5)
        ttk.Button(import_frame, text="Загрузить заказы из CSV", 
                  command=self.manual_load_orders).pack(fill=tk.X, pady=5)
        ttk.Button(import_frame, text="Перезагрузить все данные", 
                  command=self.reload_all_data).pack(fill=tk.X, pady=5)
        
        # Информация о файлах
        files_frame = ttk.LabelFrame(main_frame, text="Ожидаемые файлы", padding="10")
        files_frame.pack(fill=tk.X)
        
        files_info = """
Ожидаемые CSV файлы:

1. clients_100.csv - Данные клиентов с колонками:
   - ФИО (обязательно)
   - Контактная информация
   - Email
   - Телефон
   - Дата регистрации
   - Примечания

2. book_orders.csv - Данные заказов с колонками:
   - ID_заказа
   - ФИО_клиента (должно совпадать с ФИО клиента)
   - Название_книги
   - Автор
   - Жанр
   - Количество
   - Цена_за_шт
   - Скидка_%
   - Итоговая_цена
   - Общая_сумма
   - Дата_заказа
   - Статус_заказа
   - Способ_доставки
   - Примечание_к_заказу

Система автоматически загрузит эти файлы если они существуют в той же директории.
        """
        
        files_text = tk.Text(files_frame, height=15, wrap=tk.WORD)
        files_text.insert("1.0", files_info)
        files_text.config(state=tk.DISABLED)
        files_text.pack(fill=tk.BOTH, expand=True)
        
        # Обновляем информацию о данных
        self.update_data_info()
    
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
        
        # Таблица клиентов
        self.create_customer_table()
    
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
        ttk.Button(button_frame, text="Экспорт в PDF", 
                  command=self.export_report_pdf).pack(side=tk.LEFT, padx=2)
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
    
    # Основные методы управления клиентами
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
    
    # Методы управления заказами
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
    
    # Методы генерации отчетов
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
    
    def export_report_pdf(self):
        """Экспорт отчета в PDF"""
        messagebox.showinfo("Информация", "Функция экспорта в PDF будет реализована здесь")
    
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
    
    # Вспомогательные методы
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
    
    def load_sample_customers(self):
        """Загрузка тестовых клиентов"""
        sample_customers = [
            {
                'id': 1,
                'full_name': 'Киселев Любомир Адамович',
                'contact_info': 'киселевл@mail.ru; +79876143194',
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
                'contact_info': 'foking@mail.ru; +79876143195',
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


# Класс для управления заказами
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


# Класс для генерации отчетов
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


# Диалоговые окна
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
        
        ttk.Label(main_frame, text="Контактная информация:").grid(row=1, column=0, sticky=tk.W, pady=5)
        self.contact_entry = ttk.Entry(main_frame, width=40)
        self.contact_entry.grid(row=1, column=1, sticky=tk.W, pady=5, padx=10)
        
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
            self.contact_entry.insert(0, self.customer_data.get('contact_info', ''))
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
        
        # Валидация даты
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
                'contact_info': self.contact_entry.get().strip(),
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


# Запуск приложения
if __name__ == "__main__":
    # Настройка логирования
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    root = tk.Tk()
    app = CustomerManagementSystem(root)
    root.mainloop()