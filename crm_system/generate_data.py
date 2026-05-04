
import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker('ru_RU')

def generate_customers(count=500):
    """Генерация 500+ клиентов"""
    customers = []

    first_names = [
        'Александр', 'Дмитрий', 'Максим', 'Сергей', 'Андрей', 'Алексей', 'Владимир', 'Евгений',
        'Михаил', 'Николай', 'Иван', 'Павел', 'Роман', 'Олег', 'Игорь', 'Кирилл', 'Виктор',
        'Артем', 'Антон', 'Даниил', 'Егор', 'Никита', 'Константин', 'Юрий', 'Василий',
        'Анна', 'Елена', 'Мария', 'Ольга', 'Татьяна', 'Наталья', 'Ирина', 'Светлана',
        'Юлия', 'Екатерина', 'Анастасия', 'Ксения', 'Дарья', 'Валентина', 'Людмила'
    ]

    last_names = [
        'Кузнецов', 'Иванов', 'Петров', 'Сидоров', 'Михайлов', 'Федоров', 'Николаев',
        'Смирнов', 'Васильев', 'Алексеев', 'Егоров', 'Морозов', 'Новиков', 'Волков',
        'Соловьев', 'Васильева', 'Петрова', 'Соколова', 'Волкова', 'Козлова', 'Павлова'
    ]

    notes_list = [
        'Постоянный клиент', 'VIP клиент', 'Корпоративный заказчик', 'Оптовый покупатель',
        'Иногородний клиент', 'Новый клиент', 'Бывший сотрудник', 'Участвовал в акции',
        'Отсрочка платежа 14 дней', 'Требует особого подхода', 'Рекомендован партнером',
        'Любит получать подарки', 'Работает по предоплате', 'Часто звонит с вопросами',
        'Предпочитает email-рассылку', 'Участвует в бонусной программе', 'Местный житель',
        'Родственник руководства', 'Частное лицо', 'Лояльный клиент', ''
    ]

    for i in range(1, count + 1):
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        patronymic = fake.middle_name() if random.random() > 0.3 else ''

        full_name = f"{last_name} {first_name} {patronymic}".strip()

        # Генерация даты регистрации (последние 2 года)
        days_ago = random.randint(1, 730)
        reg_date = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")

        customer = {
            'ID': i,
            'ФИО': full_name,
            'Email': fake.email(),
            'Телефон': fake.phone_number(),
            'Дата регистрации': reg_date,
            'Примечания': random.choice(notes_list)
        }
        customers.append(customer)

    return customers

def load_orders_from_csv():
    """Загрузка заказов из book_orders.csv"""
    try:
        df = pd.read_csv('book_orders.csv', encoding='utf-8')
        return df.to_dict('records')
    except Exception as e:
        print(f"Ошибка загрузки заказов: {e}")
        return []

def save_customers_to_csv(customers, filename='clients_500.csv'):
    """Сохранение клиентов в CSV"""
    df = pd.DataFrame(customers)
    df.to_csv(filename, index=False, encoding='utf-8')
    print(f"Сохранено {len(customers)} клиентов в {filename}")
    return filename

if __name__ == "__main__":
    print("Генерация 500+ клиентов...")
    customers = generate_customers(500)

    # Добавляем существующих клиентов из original файла если есть
    try:
        original_df = pd.read_csv('clients_100.csv', encoding='utf-8')
        existing_names = set(original_df['ФИО'].tolist())

        for customer in customers:
            if customer['ФИО'] in existing_names:
                pass  # пропускаем дубликаты

            # Расширяем до 500
            while len(customers) < 500:
                customers.append(generate_customers(1)[0])

    except:
        pass

    save_customers_to_csv(customers[:500])

    # Проверка заказов
    orders = load_orders_from_csv()
    print(f"Загружено заказов: {len(orders)}")
