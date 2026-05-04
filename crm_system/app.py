
import os
import pandas as pd
from datetime import datetime, date
from flask import Flask, render_template, request, jsonify, send_file
from database import db, Customer, Order
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import io
import base64
from sqlalchemy import func

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///crm.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db.init_app(app)

class OrderManager:
    @staticmethod
    def get_customer_orders(customer_id):
        return Order.query.filter_by(customer_id=customer_id).all()

    @staticmethod
    def get_customer_total_spent(customer_id):
        orders = Order.query.filter_by(customer_id=customer_id).all()
        return sum(order.total_amount for order in orders)

    @staticmethod
    def get_customer_order_statistics(customer_id):
        orders = Order.query.filter_by(customer_id=customer_id).all()
        if not orders:
            return {
                'total_orders': 0,
                'total_amount': 0,
                'average_order': 0,
                'last_order': None
            }
        total_amount = sum(order.total_amount for order in orders)
        last_order = max(orders, key=lambda x: x.order_date)
        return {
            'total_orders': len(orders),
            'total_amount': total_amount,
            'average_order': total_amount / len(orders),
            'last_order': last_order.order_date.strftime("%Y-%m-%d") if last_order.order_date else None
        }

order_manager = OrderManager()

def load_data_from_csv():
    """Загрузка данных из CSV файлов"""
    customers_file = "clients_100.csv"
    orders_file = "book_orders.csv"

    if os.path.exists(customers_file):
        df = pd.read_csv(customers_file, encoding='utf-8')
        for _, row in df.iterrows():
            if not Customer.query.filter_by(full_name=row.get('ФИО', '')).first():
                customer = Customer(
                    full_name=row.get('ФИО', ''),
                    email=row.get('Email', ''),
                    phone=row.get('Телефон', ''),
                    registration_date=datetime.strptime(str(row.get('Дата регистрации', date.today())), "%Y-%m-%d").date(),
                    notes=row.get('Примечания', '')
                )
                db.session.add(customer)
        db.session.commit()

    if os.path.exists(orders_file):
        df = pd.read_csv(orders_file, encoding='utf-8')
        for _, row in df.iterrows():
            customer = Customer.query.filter_by(full_name=row.get('ФИО_клиента', '')).first()
            if customer:
                try:
                    order_date = datetime.strptime(str(row.get('Дата_заказа', date.today())), "%Y-%m-%d").date()
                except:
                    order_date = date.today()

                order = Order(
                    id=str(row.get('ID_заказа', '')),
                    customer_id=customer.id,
                    customer_name=row.get('ФИО_клиента', ''),
                    order_date=order_date,
                    book_title=row.get('Название_книги', ''),
                    author=row.get('Автор', ''),
                    genre=row.get('Жанр', ''),
                    quantity=int(row.get('Количество', 1)),
                    price=float(row.get('Цена_за_шт', 0)),
                    discount=float(row.get('Скидка_%', 0)),
                    final_price=float(row.get('Итоговая_цена', 0)),
                    total_amount=float(row.get('Общая_сумма', 0)),
                    status=row.get('Статус_заказа', 'Ожидает оплаты'),
                    delivery_method=row.get('Способ_доставки', ''),
                    order_notes=row.get('Примечание_к_заказу', '')
                )
                if not Order.query.filter_by(id=order.id).first():
                    db.session.add(order)
        db.session.commit()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/customers')
def customers_page():
    return render_template('customers.html')

@app.route('/api/customers')
def get_customers():
    customers = Customer.query.all()
    result = []
    for c in customers:
        total_spent = order_manager.get_customer_total_spent(c.id)
        orders_count = len(order_manager.get_customer_orders(c.id))
        result.append({
            'id': c.id,
            'full_name': c.full_name,
            'email': c.email or '',
            'phone': c.phone or '',
            'registration_date': c.registration_date.strftime("%Y-%m-%d") if c.registration_date else '',
            'notes': c.notes or '',
            'total_orders': orders_count,
            'total_spent': total_spent
        })
    return jsonify(result)

@app.route('/api/customers', methods=['POST'])
def add_customer():
    data = request.json
    customer = Customer(
        full_name=data['full_name'],
        email=data.get('email', ''),
        phone=data.get('phone', ''),
        registration_date=datetime.strptime(data['registration_date'], "%Y-%m-%d").date() if data.get('registration_date') else date.today(),
        notes=data.get('notes', '')
    )
    db.session.add(customer)
    db.session.commit()
    return jsonify({'id': customer.id, 'message': 'Клиент добавлен'})

@app.route('/api/customers/<int:customer_id>', methods=['PUT'])
def update_customer(customer_id):
    customer = Customer.query.get_or_404(customer_id)
    data = request.json
    customer.full_name = data.get('full_name', customer.full_name)
    customer.email = data.get('email', customer.email)
    customer.phone = data.get('phone', customer.phone)
    customer.notes = data.get('notes', customer.notes)
    if data.get('registration_date'):
        customer.registration_date = datetime.strptime(data['registration_date'], "%Y-%m-%d").date()
    db.session.commit()
    return jsonify({'message': 'Клиент обновлен'})

@app.route('/api/customers/<int:customer_id>', methods=['DELETE'])
def delete_customer(customer_id):
    customer = Customer.query.get_or_404(customer_id)
    Order.query.filter_by(customer_id=customer_id).delete()
    db.session.delete(customer)
    db.session.commit()
    return jsonify({'message': 'Клиент удален'})

@app.route('/orders')
def orders_page():
    return render_template('orders.html')

@app.route('/api/orders')
def get_orders():
    customer_id = request.args.get('customer_id')
    if customer_id:
        orders = Order.query.filter_by(customer_id=customer_id).all()
    else:
        orders = Order.query.all()

    result = []
    for o in orders:
        result.append({
            'id': o.id,
            'customer_id': o.customer_id,
            'customer_name': o.customer_name,
            'order_date': o.order_date.strftime("%Y-%m-%d") if o.order_date else '',
            'book_title': o.book_title,
            'author': o.author or '',
            'genre': o.genre or '',
            'quantity': o.quantity,
            'price': float(o.price),
            'discount': float(o.discount),
            'final_price': float(o.final_price),
            'total_amount': float(o.total_amount),
            'status': o.status,
            'delivery_method': o.delivery_method or '',
            'order_notes': o.order_notes or ''
        })
    return jsonify(result)

@app.route('/api/orders', methods=['POST'])
def add_order():
    data = request.json
    quantity = int(data['quantity'])
    price = float(data['price'])
    discount = float(data.get('discount', 0))
    final_price = price * (1 - discount / 100)
    total_amount = final_price * quantity

    order = Order(
        id=f"ORD{Order.query.count() + 1:03d}",
        customer_id=int(data['customer_id']),
        customer_name=data['customer_name'],
        order_date=datetime.strptime(data['order_date'], "%Y-%m-%d").date(),
        book_title=data['book_title'],
        author=data.get('author', ''),
        genre=data.get('genre', ''),
        quantity=quantity,
        price=price,
        discount=discount,
        final_price=final_price,
        total_amount=total_amount,
        status=data.get('status', 'Ожидает оплаты'),
        delivery_method=data.get('delivery_method', ''),
        order_notes=data.get('order_notes', '')
    )
    db.session.add(order)
    db.session.commit()
    return jsonify({'id': order.id, 'message': 'Заказ добавлен'})

@app.route('/api/orders/<order_id>', methods=['PUT'])
def update_order(order_id):
    order = Order.query.get_or_404(order_id)
    data = request.json
    order.book_title = data.get('book_title', order.book_title)
    order.author = data.get('author', order.author)
    order.genre = data.get('genre', order.genre)
    order.quantity = int(data.get('quantity', order.quantity))
    order.price = float(data.get('price', order.price))
    order.discount = float(data.get('discount', order.discount))
    order.final_price = order.price * (1 - order.discount / 100)
    order.total_amount = order.final_price * order.quantity
    order.status = data.get('status', order.status)
    order.delivery_method = data.get('delivery_method', order.delivery_method)
    order.order_notes = data.get('order_notes', order.order_notes)
    db.session.commit()
    return jsonify({'message': 'Заказ обновлен'})

@app.route('/api/orders/<order_id>', methods=['DELETE'])
def delete_order(order_id):
    order = Order.query.get_or_404(order_id)
    db.session.delete(order)
    db.session.commit()
    return jsonify({'message': 'Заказ удален'})

@app.route('/reports')
def reports_page():
    return render_template('reports.html')

@app.route('/api/reports/<report_type>')
def generate_report(report_type):
    customers = Customer.query.all()
    orders = Order.query.all()

    if report_type == 'customer_summary':
        result = {
            'total_customers': len(customers),
            'customers_with_orders': len(set(o.customer_id for o in orders)),
            'total_revenue': sum(o.total_amount for o in orders),
            'customers': []
        }
        for c in customers:
            spent = order_manager.get_customer_total_spent(c.id)
            orders_count = len(order_manager.get_customer_orders(c.id))
            result['customers'].append({
                'id': c.id,
                'name': c.full_name,
                'orders': orders_count,
                'spent': spent
            })
        return jsonify(result)

    elif report_type == 'order_statistics':
        result = {
            'total_orders': len(orders),
            'total_revenue': sum(o.total_amount for o in orders),
            'avg_order': sum(o.total_amount for o in orders) / len(orders) if orders else 0,
            'by_status': {}
        }
        statuses = {}
        for o in orders:
            statuses[o.status] = statuses.get(o.status, 0) + 1
        result['by_status'] = statuses
        return jsonify(result)

    elif report_type == 'registration_analysis':
        from collections import defaultdict
        monthly = defaultdict(int)
        for c in customers:
            if c.registration_date:
                month = c.registration_date.strftime("%Y-%m")
                monthly[month] += 1
        return jsonify({'monthly': dict(monthly), 'total': len(customers)})

    elif report_type == 'customer_activity':
        customers_with_orders = []
        for c in customers:
            orders_count = len(order_manager.get_customer_orders(c.id))
            if orders_count > 0:
                customers_with_orders.append({
                    'id': c.id,
                    'name': c.full_name,
                    'orders': orders_count,
                    'spent': order_manager.get_customer_total_spent(c.id)
                })
        customers_with_orders.sort(key=lambda x: x['orders'], reverse=True)
        return jsonify({'top_customers': customers_with_orders[:10]})

    return jsonify({'error': 'Unknown report type'}), 400

@app.route('/visualization')
def visualization_page():
    return render_template('visualization.html')

@app.route('/api/chart/<chart_type>')
def get_chart(chart_type):
    orders = Order.query.all()
    customers = Customer.query.all()

    if chart_type == 'revenue':
        from collections import defaultdict
        monthly = defaultdict(float)
        for o in orders:
            if o.order_date:
                month = o.order_date.strftime("%Y-%m")
                monthly[month] += o.total_amount

        months = sorted(monthly.keys())
        revenues = [monthly[m] for m in months]

        plt.figure(figsize=(10, 6))
        plt.bar(months, revenues, color='skyblue')
        plt.xlabel('Месяц')
        plt.ylabel('Выручка, руб.')
        plt.title('Выручка по месяцам')
        plt.xticks(rotation=45)

        for i, v in enumerate(revenues):
            plt.text(i, v, f'{v:,.0f}', ha='center', va='bottom')

        plt.tight_layout()

    elif chart_type == 'genre':
        from collections import Counter
        genres = Counter()
        for o in orders:
            genres[o.genre or 'Не указан'] += 1

        top_genres = genres.most_common(10)
        names = [g[0] for g in top_genres]
        counts = [g[1] for g in top_genres]

        plt.figure(figsize=(10, 6))
        plt.barh(names, counts, color='lightgreen')
        plt.xlabel('Количество заказов')
        plt.ylabel('Жанр')
        plt.title('Распределение заказов по жанрам')
        plt.tight_layout()

    elif chart_type == 'customers':
        customer_spending = {}
        for o in orders:
            customer_spending[o.customer_name] = customer_spending.get(o.customer_name, 0) + o.total_amount

        top_customers = sorted(customer_spending.items(), key=lambda x: x[1], reverse=True)[:10]
        names = [c[0][:20] for c in top_customers]
        amounts = [c[1] for c in top_customers]

        plt.figure(figsize=(12, 6))
        plt.bar(names, amounts, color='coral')
        plt.xlabel('Клиент')
        plt.ylabel('Сумма покупок, руб.')
        plt.title('Топ-10 клиентов по выручке')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()

    elif chart_type == 'status':
        statuses = Counter()
        for o in orders:
            statuses[o.status] += 1

        names = list(statuses.keys())
        counts = list(statuses.values())
        colors = ['green' if s == 'Выполнен' else 'blue' if s == 'Доставлен' else 'orange' if s == 'Обработан' else 'red' for s in names]

        plt.figure(figsize=(10, 6))
        plt.bar(names, counts, color=colors)
        plt.xlabel('Статус')
        plt.ylabel('Количество заказов')
        plt.title('Распределение заказов по статусам')
        plt.xticks(rotation=45)

        for i, v in enumerate(counts):
            plt.text(i, v, str(v), ha='center', va='bottom')

        plt.tight_layout()

    else:
        return jsonify({'error': 'Unknown chart type'}), 400

    img = io.BytesIO()
    plt.savefig(img, format='png', dpi=100)
    img.seek(0)
    plt.close()

    return send_file(img, mimetype='image/png')

@app.route('/api/export/<data_type>')
def export_data(data_type):
    if data_type == 'customers':
        customers = Customer.query.all()
        data = []
        for c in customers:
            data.append({
                'ID': c.id,
                'ФИО': c.full_name,
                'Email': c.email,
                'Телефон': c.phone,
                'Дата регистрации': c.registration_date.strftime("%Y-%m-%d") if c.registration_date else '',
                'Всего заказов': len(order_manager.get_customer_orders(c.id)),
                'Общая сумма': order_manager.get_customer_total_spent(c.id),
                'Примечания': c.notes
            })
        df = pd.DataFrame(data)
        output = io.BytesIO()
        df.to_excel(output, index=False)
        output.seek(0)
        return send_file(output, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                        as_attachment=True, download_name='customers.xlsx')

    elif data_type == 'orders':
        orders = Order.query.all()
        data = []
        for o in orders:
            data.append({
                'ID заказа': o.id,
                'ID клиента': o.customer_id,
                'Клиент': o.customer_name,
                'Дата заказа': o.order_date.strftime("%Y-%m-%d") if o.order_date else '',
                'Название книги': o.book_title,
                'Автор': o.author,
                'Жанр': o.genre,
                'Количество': o.quantity,
                'Цена за шт': o.price,
                'Скидка %': o.discount,
                'Итоговая цена': o.final_price,
                'Общая сумма': o.total_amount,
                'Статус': o.status,
                'Способ доставки': o.delivery_method,
                'Примечание': o.order_notes
            })
        df = pd.DataFrame(data)
        output = io.BytesIO()
        df.to_excel(output, index=False)
        output.seek(0)
        return send_file(output, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                        as_attachment=True, download_name='orders.xlsx')

    return jsonify({'error': 'Unknown data type'}), 400

@app.route('/api/import/customers', methods=['POST'])
def import_customers():
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400

    df = pd.read_excel(file)
    imported = 0
    for _, row in df.iterrows():
        name = row.get('ФИО', row.get('full_name', ''))
        if name and not Customer.query.filter_by(full_name=name).first():
            customer = Customer(
                full_name=name,
                email=row.get('Email', row.get('email', '')),
                phone=row.get('Телефон', row.get('phone', '')),
                registration_date=date.today(),
                notes=row.get('Примечания', row.get('notes', ''))
            )
            db.session.add(customer)
            imported += 1
    db.session.commit()
    return jsonify({'imported': imported, 'message': f'Импортировано {imported} клиентов'})

@app.route('/api/import/orders', methods=['POST'])
def import_orders():
    try:
        print(f"Request files: {request.files}")
        print(f"Request form: {request.form}")

        if 'file' not in request.files:
            print("Error: No file uploaded")
            return jsonify({'error': 'No file uploaded'}), 400

        file = request.files['file']
        print(f"File received: {file.filename}")

        if file.filename == '':
            print("Error: No file selected")
            return jsonify({'error': 'No file selected'}), 400

        try:
            print("Attempting to read Excel file...")
            df = pd.read_excel(file)
            print(f"Excel file read successfully. Shape: {df.shape}")
            print(f"Columns: {df.columns.tolist()}")
        except Exception as e:
            print(f"Error reading Excel file: {str(e)}")
            return jsonify({'error': f'Ошибка чтения файла: {str(e)}'}), 400

        imported = 0
        errors = []

        for _, row in df.iterrows():
            try:
                customer_name = row.get('ФИО_клиента', row.get('client_name', ''))
                if not customer_name:
                    errors.append('Пропущена строка: отсутствует ФИО клиента')
                    continue

                customer = Customer.query.filter_by(full_name=customer_name).first()
                if not customer:
                    errors.append(f'Клиент не найден: {customer_name}')
                    continue

                # Безопасное получение даты заказа
                order_date_val = row.get('Дата_заказа', row.get('order_date', date.today()))
                try:
                    if isinstance(order_date_val, str):
                        order_date = datetime.strptime(order_date_val, "%Y-%m-%d").date()
                    elif isinstance(order_date_val, (datetime, date)):
                        order_date = order_date_val
                    else:
                        order_date = date.today()
                except:
                    order_date = date.today()

                # Безопасное получение числовых значений
                try:
                    quantity = int(row.get('Количество', row.get('quantity', 1)))
                except:
                    quantity = 1

                try:
                    price = float(row.get('Цена_за_шт', row.get('price', 0)))
                except:
                    price = 0

                try:
                    discount = float(row.get('Скидка_%', row.get('discount', 0)))
                except:
                    discount = 0

                order_id = row.get('ID_заказа', row.get('order_id', f'ORD{Order.query.count() + imported + 1:03d}'))

                order = Order(
                    id=str(order_id),
                    customer_id=customer.id,
                    customer_name=customer_name,
                    order_date=order_date,
                    book_title=row.get('Название_книги', row.get('book_title', '')),
                    author=row.get('Автор', row.get('author', '')),
                    genre=row.get('Жанр', row.get('genre', '')),
                    quantity=quantity,
                    price=price,
                    discount=discount,
                    final_price=0,
                    total_amount=0,
                    status=row.get('Статус_заказа', row.get('status', 'Ожидает оплаты')),
                    delivery_method=row.get('Способ_доставки', row.get('delivery_method', '')),
                    order_notes=row.get('Примечание_к_заказу', row.get('notes', ''))
                )
                order.final_price = order.price * (1 - order.discount / 100)
                order.total_amount = order.final_price * order.quantity

                if not Order.query.filter_by(id=order.id).first():
                    db.session.add(order)
                    imported += 1
            except Exception as e:
                errors.append(f'Ошибка в строке {imported + 1}: {str(e)}')
                continue

        db.session.commit()

        if errors:
            return jsonify({
                'imported': imported, 
                'message': f'Импортировано {imported} заказов с ошибками',
                'errors': errors[:10]  # Возвращаем только первые 10 ошибок
            })

        return jsonify({'imported': imported, 'message': f'Импортировано {imported} заказов'})
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': f'Ошибка при импорте: {str(e)}'}), 500

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
        load_data_from_csv()
    app.run(debug=True, host='0.0.0.0', port=5000)
