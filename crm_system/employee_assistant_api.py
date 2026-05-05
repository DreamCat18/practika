
from flask import Blueprint, request, jsonify
from sqlalchemy import func, extract
from datetime import datetime, timedelta, date
from collections import defaultdict, Counter
from database import db, Customer, Order

employee_assistant_bp = Blueprint('employee_assistant', __name__)

class EmployeeAssistant:
    """Ассистент для сотрудников книжного магазина"""

    def __init__(self, db_session):
        self.db = db_session

    def get_dashboard_stats(self):
        """Получение статистики для дашборда"""
        today = date.today()
        week_ago = today - timedelta(days=7)

        # Заказы сегодня
        today_orders = Order.query.filter(
            func.date(Order.order_date) == today
        ).count()

        # Ожидают обработки
        pending_orders = Order.query.filter(
            Order.status == 'Ожидает оплаты'
        ).count()

        # Выручка сегодня
        today_revenue = self.db.session.query(
            func.sum(Order.total_amount)
        ).filter(func.date(Order.order_date) == today).scalar() or 0

        # В доставке
        delivery_orders = Order.query.filter(
            Order.status == 'В доставке'
        ).count()

        # Еженедельная выручка
        weekly_revenue = []
        for i in range(6, -1, -1):
            day = today - timedelta(days=i)
            revenue = self.db.session.query(
                func.sum(Order.total_amount)
            ).filter(func.date(Order.order_date) == day).scalar() or 0
            weekly_revenue.append({
                'day': day.strftime('%d.%m'),
                'revenue': float(revenue)
            })

        # Топ книги
        top_books = self.db.session.query(
            Order.book_title,
            func.sum(Order.quantity).label('total_sold')
        ).filter(
            Order.order_date >= week_ago
        ).group_by(
            Order.book_title
        ).order_by(
            func.sum(Order.quantity).desc()
        ).limit(5).all()

        # Активные клиенты
        active_customers = self.db.session.query(
            Customer.full_name,
            func.count(Order.id).label('order_count')
        ).join(
            Order, Customer.id == Order.customer_id
        ).filter(
            Order.order_date >= week_ago
        ).group_by(
            Customer.id
        ).order_by(
            func.count(Order.id).desc()
        ).limit(5).all()

        # Сегментация клиентов
        customers = Customer.query.all()
        segments = defaultdict(int)
        for c in customers:
            orders_count = len(c.orders)
            if orders_count == 0:
                segments['Новые'] += 1
            elif orders_count <= 2:
                segments['Эпизодические'] += 1
            elif orders_count <= 5:
                segments['Регулярные'] += 1
            else:
                segments['VIP'] += 1

        # Продажи по жанрам
        genre_sales = self.db.session.query(
            Order.genre,
            func.sum(Order.quantity).label('total')
        ).filter(
            Order.genre.isnot(None),
            Order.genre != ''
        ).group_by(
            Order.genre
        ).order_by(
            func.sum(Order.quantity).desc()
        ).limit(5).all()

        # Срочные задачи
        urgent_tasks = []

        # Заказы в обработке более 3 дней
        long_pending = Order.query.filter(
            Order.status == 'Ожидает оплаты',
            Order.order_date <= today - timedelta(days=3)
        ).count()
        if long_pending > 0:
            urgent_tasks.append(f"{long_pending} заказов ожидают оплаты более 3 дней")

        # Заказы в доставке более 7 дней
        long_delivery = Order.query.filter(
            Order.status == 'В доставке',
            Order.order_date <= today - timedelta(days=7)
        ).count()
        if long_delivery > 0:
            urgent_tasks.append(f"{long_delivery} заказов в доставке более недели")

        # Возвраты за неделю
        returns = Order.query.filter(
            Order.status == 'Возврат',
            Order.order_date >= week_ago
        ).count()
        if returns > 3:
            urgent_tasks.append(f"⚠️ {returns} возвратов за неделю - требуют внимания")

        return {
            'today_orders': today_orders,
            'pending_orders': pending_orders,
            'today_revenue': float(today_revenue),
            'delivery_orders': delivery_orders,
            'weekly_revenue': weekly_revenue,
            'top_books': [{'title': b[0], 'sold': b[1]} for b in top_books],
            'active_customers': [{'name': c[0], 'orders': c[1]} for c in active_customers],
            'segments': dict(segments),
            'genre_sales': [{'genre': g[0] or 'Другое', 'count': g[1]} for g in genre_sales],
            'urgent_tasks': urgent_tasks
        }

    def get_orders_manage(self, status=None, date_from=None, date_to=None):
        """Получение заказов с фильтрацией"""
        query = Order.query

        if status and status != 'all':
            query = query.filter(Order.status == status)

        if date_from:
            query = query.filter(Order.order_date >= datetime.strptime(date_from, '%Y-%m-%d').date())

        if date_to:
            query = query.filter(Order.order_date <= datetime.strptime(date_to, '%Y-%m-%d').date())

        orders = query.order_by(Order.order_date.desc()).limit(100).all()

        return [{
            'id': o.id,
            'customer_name': o.customer_name,
            'order_date': o.order_date.strftime('%Y-%m-%d'),
            'book_title': o.book_title,
            'total_amount': float(o.total_amount),
            'status': o.status,
            'delivery_method': o.delivery_method or 'Не указан'
        } for o in orders]

    def update_order_status(self, order_id, new_status):
        """Обновление статуса заказа"""
        order = Order.query.get(order_id)
        if order:
            order.status = new_status
            self.db.session.commit()
            return True
        return False

    def search_customers(self, query):
        """Поиск клиентов"""
        customers = Customer.query.filter(
            (Customer.full_name.ilike(f'%{query}%')) |
            (Customer.email.ilike(f'%{query}%')) |
            (Customer.phone.ilike(f'%{query}%'))
        ).limit(20).all()

        return [{
            'id': c.id,
            'full_name': c.full_name,
            'email': c.email,
            'phone': c.phone,
            'total_orders': len(c.orders)
        } for c in customers]

    def get_customer_details(self, customer_id):
        """Детальная информация о клиенте"""
        customer = Customer.query.get(customer_id)
        if not customer:
            return None

        orders = Order.query.filter_by(customer_id=customer_id).order_by(Order.order_date.desc()).all()
        total_spent = sum(float(o.total_amount) for o in orders)

        return {
            'id': customer.id,
            'full_name': customer.full_name,
            'email': customer.email,
            'phone': customer.phone,
            'registration_date': customer.registration_date.strftime('%Y-%m-%d'),
            'notes': customer.notes,
            'total_orders': len(orders),
            'total_spent': total_spent,
            'avg_order': total_spent / len(orders) if orders else 0,
            'recent_orders': [{
                'order_date': o.order_date.strftime('%Y-%m-%d'),
                'book_title': o.book_title,
                'total_amount': float(o.total_amount),
                'status': o.status
            } for o in orders[:5]]
        }

    def get_inventory_analysis(self):
        """Анализ складских остатков и рекомендации по закупке"""
        # Группировка продаж по книгам за последний месяц
        month_ago = date.today() - timedelta(days=30)

        sales_by_book = self.db.session.query(
            Order.book_title,
            Order.author,
            func.sum(Order.quantity).label('sold')
        ).filter(
            Order.order_date >= month_ago
        ).group_by(
            Order.book_title, Order.author
        ).order_by(
            func.sum(Order.quantity).desc()
        ).all()

        books = []
        reorder_list = []

        for book in sales_by_book:
            forecast = int(book.sold * 1.2)  # Прогноз на следующий месяц
            need_reorder = book.sold > 10

            books.append({
                'title': book.book_title,
                'author': book.author,
                'sold_last_month': book.sold,
                'forecast': forecast,
                'in_stock': not need_reorder
            })

            if need_reorder:
                reorder_list.append({
                    'title': book.book_title,
                    'sold': book.sold,
                    'recommend': forecast
                })

        return {
            'books': books[:20],
            'reorder_list': reorder_list[:10]
        }

    def ai_advice(self, question):
        """AI-советник для сотрудника"""
        question_lower = question.lower()

        # Топ клиенты
        if 'топ' in question_lower and ('клиент' in question_lower or 'покупател' in question_lower):
            top_customers = self.db.session.query(
                Customer.full_name,
                Customer.phone,
                Customer.email,
                func.sum(Order.total_amount).label('total')
            ).join(
                Order, Customer.id == Order.customer_id
            ).group_by(
                Customer.id
            ).order_by(
                func.sum(Order.total_amount).desc()
            ).limit(10).all()

            answer = "🏆 Топ-10 клиентов по сумме заказов:\n\n"
            for i, c in enumerate(top_customers, 1):
                answer += f"{i}. {c.full_name}\n"
                answer += f"   📞 {c.phone or '—'} | 📧 {c.email or '—'}\n"
                answer += f"   💰 Потрачено: {float(c.total):,.0f} ₽\n\n"
            return answer

        # Топ книги
        elif 'книг' in question_lower and ('топ' in question_lower or 'лучш' in question_lower):
            top_books = self.db.session.query(
                Order.book_title,
                Order.author,
                func.sum(Order.quantity).label('sold'),
                func.sum(Order.total_amount).label('revenue')
            ).group_by(
                Order.book_title, Order.author
            ).order_by(
                func.sum(Order.quantity).desc()
            ).limit(5).all()

            answer = "📚 Самые популярные книги:\n\n"
            for i, b in enumerate(top_books, 1):
                answer += f"{i}. «{b.book_title}» — {b.author or 'Автор не указан'}\n"
                answer += f"   📈 Продано: {b.sold} шт\n"
                answer += f"   💰 Выручка: {float(b.revenue):,.0f} ₽\n\n"
            return answer

        # Статистика доставки
        elif 'доставк' in question_lower:
            delivery_stats = self.db.session.query(
                Order.delivery_method,
                func.count(Order.id).label('count')
            ).filter(
                Order.delivery_method.isnot(None),
                Order.delivery_method != ''
            ).group_by(
                Order.delivery_method
            ).all()

            total = sum(d[1] for d in delivery_stats)
            answer = "🚚 Статистика по способам доставки:\n\n"
            for method, count in delivery_stats:
                percent = count / total * 100 if total > 0 else 0
                answer += f"• {method}: {count} заказов ({percent:.1f}%)\n"
            return answer

        # Прогноз продаж
        elif 'прогноз' in question_lower or 'предсказ' in question_lower:
            month_ago = date.today() - timedelta(days=30)
            monthly_revenue = self.db.session.query(
                func.sum(Order.total_amount)
            ).filter(Order.order_date >= month_ago).scalar() or 0

            daily_avg = monthly_revenue / 30
            next_week = daily_avg * 7
            next_month = monthly_revenue * 1.1

            answer = f"📊 Прогноз продаж:\n\n"
            answer += f"• Средняя дневная выручка: {daily_avg:,.0f} ₽\n"
            answer += f"• Прогноз на следующую неделю: {next_week:,.0f} ₽\n"
            answer += f"• Прогноз на следующий месяц: {next_month:,.0f} ₽\n\n"
            answer += f"💡 Рекомендация: Ожидается рост продаж на 10%, рекомендуем увеличить заказ популярных позиций."
            return answer

        # Проблемные заказы
        elif 'проблем' in question_lower or 'возврат' in question_lower:
            problem_orders = self.db.session.query(
                Order.id,
                Order.customer_name,
                Order.status,
                Order.order_date
            ).filter(
                Order.status.in_(['Отменен', 'Возврат'])
            ).order_by(
                Order.order_date.desc()
            ).limit(10).all()

            if problem_orders:
                answer = "⚠️ Проблемные заказы:\n\n"
                for o in problem_orders:
                    answer += f"• Заказ {o.id} — {o.customer_name}\n"
                    answer += f"  Статус: {o.status}, Дата: {o.order_date}\n\n"
                return answer
            else:
                return "✅ Проблемных заказов нет. Все заказы в порядке!"

        # Что заказать
        elif 'заказат' in question_lower or 'закупк' in question_lower:
            inventory = self.get_inventory_analysis()
            if inventory['reorder_list']:
                answer = "📦 Рекомендации по закупке:\n\n"
                for book in inventory['reorder_list'][:5]:
                    answer += f"• «{book['title']}» — продано {book['sold']} шт\n"
                    answer += f"  Рекомендуемый заказ: {book['recommend']} шт\n\n"
                return answer
            else:
                return "📦 Склад укомплектован. Товары в наличии."

        else:
            return self.default_advice()

    def default_advice(self):
        """Советы по умолчанию"""
        return """🤝 Чем могу помочь?

📋 Команды:
• "Покажи топ клиентов" — список лучших покупателей
• "Лучшие книги" — популярные позиции
• "Статистика доставки" — распределение по способам
• "Прогноз продаж" — на следующую неделю/месяц
• "Проблемные заказы" — возвраты и отмены
• "Что заказать" — рекомендации по закупке
• "Статусы заказов" — текущая ситуация

Что вас интересует?"""


# API эндпоинты
@employee_assistant_bp.route('/api/assistant/stats')
def get_stats():
    assistant = EmployeeAssistant(db)
    return jsonify(assistant.get_dashboard_stats())

@employee_assistant_bp.route('/api/assistant/orders')
def get_orders():
    assistant = EmployeeAssistant(db)
    status = request.args.get('status')
    date_from = request.args.get('from')
    date_to = request.args.get('to')
    return jsonify(assistant.get_orders_manage(status, date_from, date_to))

@employee_assistant_bp.route('/api/assistant/update_status', methods=['POST'])
def update_status():
    data = request.json
    assistant = EmployeeAssistant(db)
    result = assistant.update_order_status(data.get('order_id'), data.get('status'))
    return jsonify({'success': result})

@employee_assistant_bp.route('/api/assistant/search_customers')
def search_customers():
    query = request.args.get('q', '')
    assistant = EmployeeAssistant(db)
    return jsonify(assistant.search_customers(query))

@employee_assistant_bp.route('/api/assistant/customer/<int:customer_id>')
def get_customer_details(customer_id):
    assistant = EmployeeAssistant(db)
    return jsonify(assistant.get_customer_details(customer_id))

@employee_assistant_bp.route('/api/assistant/inventory')
def get_inventory():
    assistant = EmployeeAssistant(db)
    return jsonify(assistant.get_inventory_analysis())

@employee_assistant_bp.route('/api/assistant/ai_advice', methods=['POST'])
def ai_advice():
    data = request.json
    assistant = EmployeeAssistant(db)
    answer = assistant.ai_advice(data.get('question', ''))
    return jsonify({'answer': answer})
