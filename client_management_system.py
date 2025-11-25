import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import sqlite3
from datetime import datetime, date
import re
import os

class CustomerManagementSystem:
    def __init__(self, root):
        self.root = root
        self.root.title("Система управления клиентами")
        self.root.geometry("1200x700")
        
        # Инициализация базы данных
        self.init_database()
        
        # Создание интерфейса
        self.create_widgets()
        
        # Загрузка данных
        self.load_customers()
        
    def init_database(self):
        """Инициализация базы данных"""
        self.conn = sqlite3.connect('customers.db')
        self.cursor = self.conn.cursor()
        
        # Создание таблицы клиентов
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS customers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                full_name TEXT NOT NULL,
                phone TEXT,
                email TEXT,
                registration_date DATE NOT NULL,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Создание таблицы заказов
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                customer_id INTEGER,
                order_date DATE NOT NULL,
                product_name TEXT NOT NULL,
                quantity INTEGER NOT NULL,
                total_amount REAL NOT NULL,
                status TEXT DEFAULT 'Новый',
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (customer_id) REFERENCES customers (id)
            )
        ''')
        
        self.conn.commit()
    
    def create_widgets(self):
        """Создание элементов интерфейса"""
        # Основной фрейм
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Конфигурация веса строк и столбцов
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)
        main_frame.rowconfigure(5, weight=1)
        
        # Заголовок
        title_label = ttk.Label(main_frame, text="Система управления клиентами", 
                               font=('Arial', 16, 'bold'))
        title_label.grid(row=0, column=0, columnspan=4, pady=(0, 20))
        
        # Форма ввода данных клиента
        input_frame = ttk.LabelFrame(main_frame, text="Данные клиента", padding="10")
        input_frame.grid(row=1, column=0, columnspan=4, sticky=(tk.W, tk.E), pady=(0, 10))
        input_frame.columnconfigure(1, weight=1)
        input_frame.columnconfigure(3, weight=1)
        
        # Поля ввода
        ttk.Label(input_frame, text="ФИО*:").grid(row=0, column=0, sticky=tk.W, padx=(0, 5))
        self.full_name_entry = ttk.Entry(input_frame, width=30)
        self.full_name_entry.grid(row=0, column=1, sticky=(tk.W, tk.E), padx=(0, 10))
        
        ttk.Label(input_frame, text="Телефон:").grid(row=0, column=2, sticky=tk.W, padx=(0, 5))
        self.phone_entry = ttk.Entry(input_frame, width=20)
        self.phone_entry.grid(row=0, column=3, sticky=(tk.W, tk.E))
        
        ttk.Label(input_frame, text="Email:").grid(row=1, column=0, sticky=tk.W, padx=(0, 5))
        self.email_entry = ttk.Entry(input_frame, width=30)
        self.email_entry.grid(row=1, column=1, sticky=(tk.W, tk.E), padx=(0, 10))
        
        ttk.Label(input_frame, text="Дата регистрации*:").grid(row=1, column=2, sticky=tk.W, padx=(0, 5))
        self.reg_date_entry = ttk.Entry(input_frame, width=20)
        self.reg_date_entry.insert(0, date.today().strftime("%Y-%m-%d"))
        self.reg_date_entry.grid(row=1, column=3, sticky=(tk.W, tk.E))
        
        ttk.Label(input_frame, text="Примечания:").grid(row=2, column=0, sticky=tk.W, padx=(0, 5))
        self.notes_entry = scrolledtext.ScrolledText(input_frame, width=50, height=3)
        self.notes_entry.grid(row=2, column=1, columnspan=3, sticky=(tk.W, tk.E), pady=(5, 0))
        
        # Кнопки управления
        button_frame = ttk.Frame(main_frame)
        button_frame.grid(row=2, column=0, columnspan=4, pady=10)
        
        self.add_btn = ttk.Button(button_frame, text="Добавить клиента", command=self.add_customer)
        self.add_btn.pack(side=tk.LEFT, padx=(0, 5))
        
        self.update_btn = ttk.Button(button_frame, text="Обновить данные", command=self.update_customer)
        self.update_btn.pack(side=tk.LEFT, padx=5)
        
        self.delete_btn = ttk.Button(button_frame, text="Удалить клиента", command=self.delete_customer)
        self.delete_btn.pack(side=tk.LEFT, padx=5)
        
        self.clear_btn = ttk.Button(button_frame, text="Очистить форму", command=self.clear_form)
        self.clear_btn.pack(side=tk.LEFT, padx=5)
        
        # Поиск
        search_frame = ttk.LabelFrame(main_frame, text="Поиск клиентов", padding="10")
        search_frame.grid(row=3, column=0, columnspan=4, sticky=(tk.W, tk.E), pady=(0, 10))
        search_frame.columnconfigure(1, weight=1)
        
        ttk.Label(search_frame, text="Параметр поиска:").grid(row=0, column=0, sticky=tk.W, padx=(0, 5))
        self.search_var = tk.StringVar()
        search_combo = ttk.Combobox(search_frame, textvariable=self.search_var, 
                                   values=["ФИО", "Телефон", "Email", "Дата регистрации"])
        search_combo.grid(row=0, column=1, sticky=(tk.W, tk.E), padx=(0, 10))
        search_combo.set("ФИО")
        
        ttk.Label(search_frame, text="Значение:").grid(row=0, column=2, sticky=tk.W, padx=(0, 5))
        self.search_entry = ttk.Entry(search_frame, width=20)
        self.search_entry.grid(row=0, column=3, sticky=(tk.W, tk.E), padx=(0, 10))
        
        ttk.Button(search_frame, text="Найти", command=self.search_customers).grid(row=0, column=4, padx=(0, 5))
        ttk.Button(search_frame, text="Показать всех", command=self.load_customers).grid(row=0, column=5)
        
        # Таблица клиентов
        tree_frame = ttk.Frame(main_frame)
        tree_frame.grid(row=4, column=0, columnspan=4, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(0, 10))
        tree_frame.columnconfigure(0, weight=1)
        tree_frame.rowconfigure(0, weight=1)
        
        columns = ("ID", "ФИО", "Телефон", "Email", "Дата регистрации", "Примечания")
        self.tree = ttk.Treeview(tree_frame, columns=columns, show="headings", height=12)
        
        # Настройка колонок
        for col in columns:
            self.tree.heading(col, text=col)
            self.tree.column(col, width=100)
        
        self.tree.column("ФИО", width=150)
        self.tree.column("Примечания", width=200)
        
        # Scrollbar для таблицы
        scrollbar = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)
        
        self.tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))
        
        # Привязка события выбора
        self.tree.bind('<<TreeviewSelect>>', self.on_tree_select)
        
        # Управление заказами
        orders_frame = ttk.LabelFrame(main_frame, text="Управление заказами", padding="10")
        orders_frame.grid(row=5, column=0, columnspan=4, sticky=(tk.W, tk.E, tk.N, tk.S))
        orders_frame.columnconfigure(1, weight=1)
        
        ttk.Button(orders_frame, text="Добавить заказ", command=self.add_order_dialog).grid(row=0, column=0, padx=(0, 5))
        ttk.Button(orders_frame, text="Просмотреть заказы", command=self.view_orders).grid(row=0, column=1, padx=5)
        ttk.Button(orders_frame, text="Сгенерировать отчет", command=self.generate_report).grid(row=0, column=2, padx=5)
        
        # Статус бар
        self.status_var = tk.StringVar()
        self.status_var.set("Готов к работе")
        status_bar = ttk.Label(main_frame, textvariable=self.status_var, relief=tk.SUNKEN)
        status_bar.grid(row=6, column=0, columnspan=4, sticky=(tk.W, tk.E))
        
        # Переменная для хранения ID выбранного клиента
        self.selected_customer_id = None
    
    def validate_email(self, email):
        """Валидация email"""
        if not email:
            return True
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return re.match(pattern, email) is not None
    
    def validate_phone(self, phone):
        """Валидация телефона"""
        if not phone:
            return True
        # Разрешаем форматы: +7XXX..., 8XXX..., и цифры без пробелов
        pattern = r'^(\+7|8)?\d{10}$'
        return re.match(pattern, phone.replace(' ', '').replace('-', '')) is not None
    
    def validate_date(self, date_str):
        """Валидация даты"""
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
            return True
        except ValueError:
            return False
    
    def add_customer(self):
        """Добавление нового клиента"""
        try:
            # Получение данных из формы
            full_name = self.full_name_entry.get().strip()
            phone = self.phone_entry.get().strip()
            email = self.email_entry.get().strip()
            reg_date = self.reg_date_entry.get().strip()
            notes = self.notes_entry.get("1.0", tk.END).strip()
            
            # Валидация обязательных полей
            if not full_name:
                messagebox.showerror("Ошибка", "Поле 'ФИО' обязательно для заполнения")
                return
            
            if not reg_date:
                messagebox.showerror("Ошибка", "Поле 'Дата регистрации' обязательно для заполнения")
                return
            
            # Валидация форматов
            if not self.validate_date(reg_date):
                messagebox.showerror("Ошибка", "Неверный формат даты. Используйте ГГГГ-ММ-ДД")
                return
            
            if email and not self.validate_email(email):
                messagebox.showerror("Ошибка", "Неверный формат email")
                return
            
            if phone and not self.validate_phone(phone):
                messagebox.showerror("Ошибка", "Неверный формат телефона")
                return
            
            # Добавление в базу данных
            self.cursor.execute('''
                INSERT INTO customers (full_name, phone, email, registration_date, notes)
                VALUES (?, ?, ?, ?, ?)
            ''', (full_name, phone, email, reg_date, notes))
            
            self.conn.commit()
            self.load_customers()
            self.clear_form()
            self.status_var.set("Клиент успешно добавлен")
            
        except Exception as e:
            messagebox.showerror("Ошибка", f"Ошибка при добавлении клиента: {str(e)}")
    
    def update_customer(self):
        """Обновление данных клиента"""
        if not self.selected_customer_id:
            messagebox.showwarning("Предупреждение", "Выберите клиента для редактирования")
            return
        
        try:
            # Получение данных из формы
            full_name = self.full_name_entry.get().strip()
            phone = self.phone_entry.get().strip()
            email = self.email_entry.get().strip()
            reg_date = self.reg_date_entry.get().strip()
            notes = self.notes_entry.get("1.0", tk.END).strip()
            
            # Валидация
            if not full_name or not reg_date:
                messagebox.showerror("Ошибка", "Поля 'ФИО' и 'Дата регистрации' обязательны")
                return
            
            if not self.validate_date(reg_date):
                messagebox.showerror("Ошибка", "Неверный формат даты")
                return
            
            if email and not self.validate_email(email):
                messagebox.showerror("Ошибка", "Неверный формат email")
                return
            
            if phone and not self.validate_phone(phone):
                messagebox.showerror("Ошибка", "Неверный формат телефона")
                return
            
            # Обновление в базе данных
            self.cursor.execute('''
                UPDATE customers 
                SET full_name=?, phone=?, email=?, registration_date=?, notes=?
                WHERE id=?
            ''', (full_name, phone, email, reg_date, notes, self.selected_customer_id))
            
            self.conn.commit()
            self.load_customers()
            self.status_var.set("Данные клиента обновлены")
            
        except Exception as e:
            messagebox.showerror("Ошибка", f"Ошибка при обновлении клиента: {str(e)}")
    
    def delete_customer(self):
        """Удаление клиента"""
        if not self.selected_customer_id:
            messagebox.showwarning("Предупреждение", "Выберите клиента для удаления")
            return
        
        if messagebox.askyesno("Подтверждение", "Вы уверены, что хотите удалить этого клиента?"):
            try:
                # Сначала удаляем связанные заказы
                self.cursor.execute('DELETE FROM orders WHERE customer_id=?', (self.selected_customer_id,))
                # Затем удаляем клиента
                self.cursor.execute('DELETE FROM customers WHERE id=?', (self.selected_customer_id,))
                self.conn.commit()
                self.load_customers()
                self.clear_form()
                self.status_var.set("Клиент удален")
            except Exception as e:
                messagebox.showerror("Ошибка", f"Ошибка при удалении клиента: {str(e)}")
    
    def search_customers(self):
        """Поиск клиентов по параметрам"""
        search_param = self.search_var.get()
        search_value = self.search_entry.get().strip()
        
        if not search_value:
            self.load_customers()
            return
        
        try:
            query = "SELECT id, full_name, phone, email, registration_date, notes FROM customers"
            
            if search_param == "ФИО":
                self.cursor.execute(query + " WHERE full_name LIKE ?", (f'%{search_value}%',))
            elif search_param == "Телефон":
                self.cursor.execute(query + " WHERE phone LIKE ?", (f'%{search_value}%',))
            elif search_param == "Email":
                self.cursor.execute(query + " WHERE email LIKE ?", (f'%{search_value}%',))
            elif search_param == "Дата регистрации":
                self.cursor.execute(query + " WHERE registration_date = ?", (search_value,))
            
            customers = self.cursor.fetchall()
            self.display_customers(customers)
            self.status_var.set(f"Найдено клиентов: {len(customers)}")
            
        except Exception as e:
            messagebox.showerror("Ошибка", f"Ошибка при поиске: {str(e)}")
    
    def load_customers(self):
        """Загрузка всех клиентов"""
        try:
            self.cursor.execute('''
                SELECT id, full_name, phone, email, registration_date, notes 
                FROM customers 
                ORDER BY full_name
            ''')
            customers = self.cursor.fetchall()
            self.display_customers(customers)
            self.status_var.set(f"Загружено клиентов: {len(customers)}")
        except Exception as e:
            messagebox.showerror("Ошибка", f"Ошибка при загрузке клиентов: {str(e)}")
    
    def display_customers(self, customers):
        """Отображение клиентов в таблице"""
        # Очистка таблицы
        for item in self.tree.get_children():
            self.tree.delete(item)
        
        # Заполнение данными
        for customer in customers:
            self.tree.insert('', tk.END, values=customer)
    
    def on_tree_select(self, event):
        """Обработка выбора клиента в таблице"""
        selection = self.tree.selection()
        if selection:
            item = selection[0]
            customer_data = self.tree.item(item)['values']
            self.selected_customer_id = customer_data[0]
            self.populate_form(customer_data)
    
    def populate_form(self, customer_data):
        """Заполнение формы данными выбранного клиента"""
        self.full_name_entry.delete(0, tk.END)
        self.full_name_entry.insert(0, customer_data[1])
        
        self.phone_entry.delete(0, tk.END)
        self.phone_entry.insert(0, customer_data[2] if customer_data[2] else "")
        
        self.email_entry.delete(0, tk.END)
        self.email_entry.insert(0, customer_data[3] if customer_data[3] else "")
        
        self.reg_date_entry.delete(0, tk.END)
        self.reg_date_entry.insert(0, customer_data[4])
        
        self.notes_entry.delete("1.0", tk.END)
        self.notes_entry.insert("1.0", customer_data[5] if customer_data[5] else "")
    
    def clear_form(self):
        """Очистка формы"""
        self.full_name_entry.delete(0, tk.END)
        self.phone_entry.delete(0, tk.END)
        self.email_entry.delete(0, tk.END)
        self.reg_date_entry.delete(0, tk.END)
        self.reg_date_entry.insert(0, date.today().strftime("%Y-%m-%d"))
        self.notes_entry.delete("1.0", tk.END)
        self.selected_customer_id = None
    
    def add_order_dialog(self):
        """Диалог добавления заказа"""
        if not self.selected_customer_id:
            messagebox.showwarning("Предупреждение", "Выберите клиента для добавления заказа")
            return
        
        dialog = tk.Toplevel(self.root)
        dialog.title("Добавить заказ")
        dialog.geometry("400x300")
        dialog.transient(self.root)
        dialog.grab_set()
        
        # Форма заказа
        ttk.Label(dialog, text="Товар/Услуга*:").grid(row=0, column=0, sticky=tk.W, padx=10, pady=5)
        product_entry = ttk.Entry(dialog, width=30)
        product_entry.grid(row=0, column=1, padx=10, pady=5)
        
        ttk.Label(dialog, text="Количество*:").grid(row=1, column=0, sticky=tk.W, padx=10, pady=5)
        quantity_entry = ttk.Entry(dialog, width=30)
        quantity_entry.grid(row=1, column=1, padx=10, pady=5)
        
        ttk.Label(dialog, text="Сумма*:").grid(row=2, column=0, sticky=tk.W, padx=10, pady=5)
        amount_entry = ttk.Entry(dialog, width=30)
        amount_entry.grid(row=2, column=1, padx=10, pady=5)
        
        ttk.Label(dialog, text="Статус:").grid(row=3, column=0, sticky=tk.W, padx=10, pady=5)
        status_var = tk.StringVar(value="Новый")
        status_combo = ttk.Combobox(dialog, textvariable=status_var, 
                                   values=["Новый", "В обработке", "Выполнен", "Отменен"])
        status_combo.grid(row=3, column=1, padx=10, pady=5)
        
        ttk.Label(dialog, text="Примечания:").grid(row=4, column=0, sticky=tk.W, padx=10, pady=5)
        notes_text = scrolledtext.ScrolledText(dialog, width=30, height=4)
        notes_text.grid(row=4, column=1, padx=10, pady=5)
        
        def save_order():
            try:
                product = product_entry.get().strip()
                quantity = quantity_entry.get().strip()
                amount = amount_entry.get().strip()
                status = status_var.get()
                notes = notes_text.get("1.0", tk.END).strip()
                
                if not product or not quantity or not amount:
                    messagebox.showerror("Ошибка", "Заполните обязательные поля")
                    return
                
                if not quantity.isdigit() or int(quantity) <= 0:
                    messagebox.showerror("Ошибка", "Количество должно быть положительным числом")
                    return
                
                try:
                    float(amount)
                except ValueError:
                    messagebox.showerror("Ошибка", "Сумма должна быть числом")
                    return
                
                self.cursor.execute('''
                    INSERT INTO orders (customer_id, order_date, product_name, quantity, total_amount, status, notes)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (self.selected_customer_id, date.today(), product, int(quantity), float(amount), status, notes))
                
                self.conn.commit()
                dialog.destroy()
                self.status_var.set("Заказ успешно добавлен")
                
            except Exception as e:
                messagebox.showerror("Ошибка", f"Ошибка при добавлении заказа: {str(e)}")
        
        ttk.Button(dialog, text="Сохранить", command=save_order).grid(row=5, column=0, columnspan=2, pady=10)
    
    def view_orders(self):
        """Просмотр заказов выбранного клиента"""
        if not self.selected_customer_id:
            messagebox.showwarning("Предупреждение", "Выберите клиента для просмотра заказов")
            return
        
        try:
            self.cursor.execute('''
                SELECT o.id, o.order_date, o.product_name, o.quantity, o.total_amount, o.status, o.notes
                FROM orders o
                WHERE o.customer_id = ?
                ORDER BY o.order_date DESC
            ''', (self.selected_customer_id,))
            
            orders = self.cursor.fetchall()
            
            # Диалог просмотра заказов
            dialog = tk.Toplevel(self.root)
            dialog.title("Заказы клиента")
            dialog.geometry("800x400")
            
            # Таблица заказов
            columns = ("ID", "Дата", "Товар", "Кол-во", "Сумма", "Статус", "Примечания")
            tree = ttk.Treeview(dialog, columns=columns, show="headings", height=15)
            
            for col in columns:
                tree.heading(col, text=col)
                tree.column(col, width=100)
            
            tree.column("Товар", width=150)
            tree.column("Примечания", width=200)
            
            for order in orders:
                tree.insert('', tk.END, values=order)
            
            scrollbar = ttk.Scrollbar(dialog, orient=tk.VERTICAL, command=tree.yview)
            tree.configure(yscrollcommand=scrollbar.set)
            
            tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
            scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))
            
            # Кнопки управления статусом
            def update_status():
                selection = tree.selection()
                if not selection:
                    messagebox.showwarning("Предупреждение", "Выберите заказ")
                    return
                
                item = selection[0]
                order_id = tree.item(item)['values'][0]
                
                status_dialog = tk.Toplevel(dialog)
                status_dialog.title("Изменение статуса")
                status_dialog.geometry("300x150")
                
                ttk.Label(status_dialog, text="Новый статус:").pack(pady=10)
                new_status_var = tk.StringVar()
                status_combo = ttk.Combobox(status_dialog, textvariable=new_status_var,
                                           values=["Новый", "В обработке", "Выполнен", "Отменен"])
                status_combo.pack(pady=10)
                status_combo.set(tree.item(item)['values'][5])
                
                def save_status():
                    self.cursor.execute('UPDATE orders SET status=? WHERE id=?', 
                                      (new_status_var.get(), order_id))
                    self.conn.commit()
                    status_dialog.destroy()
                    view_orders()  # Обновляем список
                
                ttk.Button(status_dialog, text="Сохранить", command=save_status).pack(pady=10)
            
            button_frame = ttk.Frame(dialog)
            button_frame.grid(row=1, column=0, columnspan=2, pady=10)
            
            ttk.Button(button_frame, text="Изменить статус", command=update_status).pack(side=tk.LEFT, padx=5)
            ttk.Button(button_frame, text="Закрыть", command=dialog.destroy).pack(side=tk.LEFT, padx=5)
            
            dialog.columnconfigure(0, weight=1)
            dialog.rowconfigure(0, weight=1)
            
        except Exception as e:
            messagebox.showerror("Ошибка", f"Ошибка при загрузке заказов: {str(e)}")
    
    def generate_report(self):
        """Генерация отчетов"""
        try:
            # Диалог выбора параметров отчета
            dialog = tk.Toplevel(self.root)
            dialog.title("Генерация отчета")
            dialog.geometry("400x200")
            
            ttk.Label(dialog, text="Период отчета:").grid(row=0, column=0, sticky=tk.W, padx=10, pady=10)
            
            ttk.Label(dialog, text="С:").grid(row=1, column=0, sticky=tk.W, padx=10, pady=5)
            start_date_entry = ttk.Entry(dialog, width=15)
            start_date_entry.grid(row=1, column=1, sticky=tk.W, padx=10, pady=5)
            start_date_entry.insert(0, date.today().replace(day=1).strftime("%Y-%m-%d"))
            
            ttk.Label(dialog, text="По:").grid(row=2, column=0, sticky=tk.W, padx=10, pady=5)
            end_date_entry = ttk.Entry(dialog, width=15)
            end_date_entry.grid(row=2, column=1, sticky=tk.W, padx=10, pady=5)
            end_date_entry.insert(0, date.today().strftime("%Y-%m-%d"))
            
            report_type = tk.StringVar(value="customers")
            ttk.Radiobutton(dialog, text="Отчет по клиентам", variable=report_type, value="customers").grid(row=3, column=0, sticky=tk.W, padx=10, pady=5)
            ttk.Radiobutton(dialog, text="Отчет по заказам", variable=report_type, value="orders").grid(row=3, column=1, sticky=tk.W, padx=10, pady=5)
            
            def generate():
                start_date = start_date_entry.get().strip()
                end_date = end_date_entry.get().strip()
                
                if not self.validate_date(start_date) or not self.validate_date(end_date):
                    messagebox.showerror("Ошибка", "Неверный формат даты")
                    return
                
                if report_type.get() == "customers":
                    self.generate_customer_report(start_date, end_date)
                else:
                    self.generate_order_report(start_date, end_date)
                
                dialog.destroy()
            
            ttk.Button(dialog, text="Сгенерировать", command=generate).grid(row=4, column=0, columnspan=2, pady=20)
            
        except Exception as e:
            messagebox.showerror("Ошибка", f"Ошибка при генерации отчета: {str(e)}")
    
    def generate_customer_report(self, start_date, end_date):
        """Генерация отчета по клиентам"""
        try:
            self.cursor.execute('''
                SELECT COUNT(*) as total_customers,
                       COUNT(DISTINCT o.customer_id) as customers_with_orders,
                       SUM(o.total_amount) as total_revenue
                FROM customers c
                LEFT JOIN orders o ON c.id = o.customer_id AND o.order_date BETWEEN ? AND ?
                WHERE c.registration_date BETWEEN ? AND ?
            ''', (start_date, end_date, start_date, end_date))
            
            stats = self.cursor.fetchone()
            
            self.cursor.execute('''
                SELECT full_name, phone, email, registration_date,
                       (SELECT COUNT(*) FROM orders WHERE customer_id = c.id AND order_date BETWEEN ? AND ?) as order_count,
                       (SELECT SUM(total_amount) FROM orders WHERE customer_id = c.id AND order_date BETWEEN ? AND ?) as total_spent
                FROM customers c
                WHERE registration_date BETWEEN ? AND ?
                ORDER BY total_spent DESC
            ''', (start_date, end_date, start_date, end_date, start_date, end_date))
            
            customers = self.cursor.fetchall()
            
            # Создание отчета
            report = f"ОТЧЕТ ПО КЛИЕНТАМ\n"
            report += f"Период: с {start_date} по {end_date}\n"
            report += f"Всего клиентов: {stats[0]}\n"
            report += f"Клиентов с заказами: {stats[1]}\n"
            report += f"Общая выручка: {stats[2] or 0:.2f}\n\n"
            report += "ДЕТАЛЬНАЯ ИНФОРМАЦИЯ:\n"
            report += "-" * 80 + "\n"
            
            for customer in customers:
                report += f"ФИО: {customer[0]}\n"
                report += f"Телефон: {customer[1] or 'Не указан'}\n"
                report += f"Email: {customer[2] or 'Не указан'}\n"
                report += f"Дата регистрации: {customer[3]}\n"
                report += f"Количество заказов: {customer[4]}\n"
                report += f"Общая сумма заказов: {customer[5] or 0:.2f}\n"
                report += "-" * 80 + "\n"
            
            self.show_report_dialog("Отчет по клиентам", report)
            
        except Exception as e:
            messagebox.showerror("Ошибка", f"Ошибка при генерации отчета: {str(e)}")
    

    def generate_order_report(self, start_date, end_date):
        """Генерация отчета по заказам"""
        try:
            self.cursor.execute('''
                SELECT COUNT(*) as total_orders,
                        SUM(total_amount) as total_revenue,
                        AVG(total_amount) as avg_order,
                        COUNT(DISTINCT customer_id) as unique_customers
                FROM orders
                WHERE order_date BETWEEN ? AND ?
            ''', (start_date, end_date))
        
            stats = self.cursor.fetchone()
        
            self.cursor.execute('''
                SELECT o.order_date, c.full_name, o.product_name, o.quantity, 
                    o.total_amount, o.status
                FROM orders o
                JOIN customers c ON o.customer_id = c.id
                WHERE o.order_date BETWEEN ? AND ?
                ORDER BY o.order_date DESC
            ''', (start_date, end_date))
        
            orders = self.cursor.fetchall()
        
        # Статистика по статусам
            self.cursor.execute('''
                SELECT status, COUNT(*), SUM(total_amount)
                FROM orders
                WHERE order_date BETWEEN ? AND ?
                GROUP BY status
            ''', (start_date, end_date))
        
            status_stats = self.cursor.fetchall()
        
        # Создание отчета
            report = f"ОТЧЕТ ПО ЗАКАЗАМ\n"
            report += f"Период: с {start_date} по {end_date}\n"
            report += f"Всего заказов: {stats[0]}\n"
            report += f"Уникальных клиентов: {stats[3]}\n"
            report += f"Общая выручка: {stats[1] or 0:.2f}\n"
            report += f"Средний чек: {stats[2] or 0:.2f}\n\n"
        
            report += "СТАТИСТИКА ПО СТАТУСАМ:\n"
            for status_stat in status_stats:
                report += f"{status_stat[0]}: {status_stat[1]} заказов, сумма: {status_stat[2] or 0:.2f}\n"
        
            report += "\nДЕТАЛЬНАЯ ИНФОРМАЦИЯ:\n"
            report += "-" * 100 + "\n"
        
            for order in orders:
                    report += f"Дата: {order[0]} | Клиент: {order[1]} | Товар: {order[2]} | "
            report += f"Кол-во: {order[3]} | Сумма: {order[4]:.2f} | Статус: {order[5]}\n"
        
            self.show_report_dialog("Отчет по заказам", report)
        
        except Exception as e:
            messagebox.showerror("Ошибка", f"Ошибка при генерации отчета: {str(e)}")
    
    def show_report_dialog(self, title, content):
        """Отображение отчета в диалоговом окне"""
        dialog = tk.Toplevel(self.root)
        dialog.title(title)
        dialog.geometry("700x600")
        dialog.transient(self.root)
    
    # Основной фрейм
        main_frame = ttk.Frame(dialog, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)
    
    # Заголовок
        title_label = ttk.Label(main_frame, text=title, font=('Arial', 12, 'bold'))
        title_label.pack(pady=(0, 10))
    
    # Текстовое поле с отчетом
        text_frame = ttk.Frame(main_frame)
        text_frame.pack(fill=tk.BOTH, expand=True)
    
        text_widget = scrolledtext.ScrolledText(text_frame, wrap=tk.WORD, width=80, height=30, 
                                          font=('Courier New', 10))
        text_widget.insert("1.0", content)
        text_widget.config(state=tk.DISABLED)
        text_widget.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
    
    # Фрейм с кнопками
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X, pady=10)
    
    def save_report():
        """Сохранение отчета в файл"""
        from tkinter import filedialog
        file_path = filedialog.asksaveasfilename(
            defaultextension=".txt",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")],
            title="Сохранить отчет как"
        )
        if file_path:
            try:
                with open(file_path, 'w', encoding='utf-8') as file:
                    file.write(content)
                messagebox.showinfo("Успех", f"Отчет сохранен в файл:\n{file_path}")
            except Exception as e:
                messagebox.showerror("Ошибка", f"Не удалось сохранить файл: {str(e)}")
    
    def print_report():
        """Печать отчета"""
        try:
            # Создаем временный файл для печати
            import tempfile
            import os
            with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8') as tmp_file:
                tmp_file.write(content)
                tmp_file_path = tmp_file.name
            
            # Открываем файл в программе по умолчанию для печати
            os.startfile(tmp_file_path, 'print')
            
            # Удаляем временный файл через некоторое время
            dialog.after(5000, lambda: os.unlink(tmp_file_path))
            
            messagebox.showinfo("Печать", "Отчет отправлен на печать")
        except Exception as e:
            messagebox.showerror("Ошибка", f"Не удалось отправить на печать: {str(e)}")
    
    def copy_to_clipboard():
        """Копирование отчета в буфер обмена"""
        dialog.clipboard_clear()
        dialog.clipboard_append(content)
        self.status_var.set("Отчет скопирован в буфер обмена")
    
    # Кнопки управления
    ttk.Button(button_frame, text="Сохранить в файл", command=save_report).pack(side=tk.LEFT, padx=5)
    ttk.Button(button_frame, text="Копировать", command=copy_to_clipboard).pack(side=tk.LEFT, padx=5)
    ttk.Button(button_frame, text="Печать", command=print_report).pack(side=tk.LEFT, padx=5)
    ttk.Button(button_frame, text="Закрыть", command=dialog.destroy).pack(side=tk.RIGHT, padx=5)
    
    # Устанавливаем фокус на диалог
    dialog.focus_set()


def main():
    """Основная функция запуска приложения"""
    root = tk.Tk()
    app = CustomerManagementSystem(root)
    root.mainloop()

if __name__ == "__main__":
    main()