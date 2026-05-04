import pandas as pd
import os

def convert_orders_file(input_file='book_orders.xlsx', output_file='заказы_исправленный.xlsx'):
    """
    Преобразование файла заказов в правильный формат с нужными колонками
    """

    # Чтение исходного файла
    print(f"Чтение файла: {input_file}")
    df = pd.read_excel(input_file)

    print(f"Исходные колонки: {list(df.columns)}")
    print(f"Всего записей: {len(df)}")

    # Проверяем, какие колонки есть в файле
    column_mapping = {}

    # Маппинг для переименования колонок
    if 'ФИО_клиента' in df.columns:
        column_mapping['ФИО_клиента'] = 'ФИО_клиента'
    elif 'Клиент' in df.columns:
        column_mapping['Клиент'] = 'ФИО_клиента'
    elif 'customer_name' in df.columns:
        column_mapping['customer_name'] = 'ФИО_клиента'

    if 'Дата_заказа' in df.columns:
        column_mapping['Дата_заказа'] = 'Дата_заказа'
    elif 'order_date' in df.columns:
        column_mapping['order_date'] = 'Дата_заказа'

    if 'Название_книги' in df.columns:
        column_mapping['Название_книги'] = 'Название_книги'
    elif 'book_title' in df.columns:
        column_mapping['book_title'] = 'Название_книги'

    if 'Автор' in df.columns:
        column_mapping['Автор'] = 'Автор'
    elif 'author' in df.columns:
        column_mapping['author'] = 'Автор'

    if 'Жанр' in df.columns:
        column_mapping['Жанр'] = 'Жанр'
    elif 'genre' in df.columns:
        column_mapping['genre'] = 'Жанр'

    if 'Количество' in df.columns:
        column_mapping['Количество'] = 'Количество'
    elif 'quantity' in df.columns:
        column_mapping['quantity'] = 'Количество'

    if 'Цена_за_шт' in df.columns:
        column_mapping['Цена_за_шт'] = 'Цена_за_шт'
    elif 'price' in df.columns:
        column_mapping['price'] = 'Цена_за_шт'

    if 'Скидка_%' in df.columns:
        column_mapping['Скидка_%'] = 'Скидка_%'
    elif 'discount' in df.columns:
        column_mapping['discount'] = 'Скидка_%'

    if 'Статус_заказа' in df.columns:
        column_mapping['Статус_заказа'] = 'Статус_заказа'
    elif 'Статус' in df.columns:
        column_mapping['Статус'] = 'Статус_заказа'
    elif 'status' in df.columns:
        column_mapping['status'] = 'Статус_заказа'

    if 'Способ_доставки' in df.columns:
        column_mapping['Способ_доставки'] = 'Способ_доставки'
    elif 'delivery_method' in df.columns:
        column_mapping['delivery_method'] = 'Способ_доставки'

    if 'Примечание_к_заказу' in df.columns:
        column_mapping['Примечание_к_заказу'] = 'Примечание_к_заказу'
    elif 'Примечание' in df.columns:
        column_mapping['Примечание'] = 'Примечание_к_заказу'
    elif 'order_notes' in df.columns:
        column_mapping['order_notes'] = 'Примечание_к_заказу'

    # Переименовываем колонки
    if column_mapping:
        df = df.rename(columns=column_mapping)

    # Создаем список нужных колонок
    required_columns = [
        'ФИО_клиента', 'Дата_заказа', 'Название_книги', 'Автор', 'Жанр',
        'Количество', 'Цена_за_шт', 'Скидка_%', 'Статус_заказа',
        'Способ_доставки', 'Примечание_к_заказу'
    ]

    # Проверяем наличие колонок и создаем недостающие
    for col in required_columns:
        if col not in df.columns:
            df[col] = ''  # Добавляем пустую колонку если её нет
            print(f"Добавлена пустая колонка: {col}")

    # Выбираем только нужные колонки в правильном порядке
    df_final = df[required_columns]

    # Очистка данных
    df_final['ФИО_клиента'] = df_final['ФИО_клиента'].fillna('').astype(str)
    df_final['Название_книги'] = df_final['Название_книги'].fillna('').astype(str)
    df_final['Количество'] = pd.to_numeric(df_final['Количество'], errors='coerce').fillna(1).astype(int)
    df_final['Цена_за_шт'] = pd.to_numeric(df_final['Цена_за_шт', errors='coerce').fillna(0)
    df_final['Скидка_%'] = pd.to_numeric(df_final['Скидка_%'], errors='coerce').fillna(0)

    # Заполнение пустых значений
    df_final['Автор'] = df_final['Автор'].fillna('')
    df_final['Жанр'] = df_final['Жанр'].fillna('')
    df_final['Статус_заказа'] = df_final['Статус_заказа'].fillna('Ожидает оплаты')
    df_final['Способ_доставки'] = df_final['Способ_доставки'].fillna('Самовывоз')
    df_final['Примечание_к_заказу'] = df_final['Примечание_к_заказу'].fillna('')

    # Удаляем строки без ФИО клиента
    df_final = df_final[df_final['ФИО_клиента'] != '']

    print(f"\nПосле очистки записей: {len(df_final)}")
    print(f"Итоговые колонки: {list(df_final.columns)}")

    # Сохраняем в Excel
    df_final.to_excel(output_file, index=False)
    print(f"\nФайл сохранен: {output_file}")

    # Также сохраняем в CSV для удобства
    csv_file = output_file.replace('.xlsx', '.csv')
    df_final.to_csv(csv_file, index=False, encoding='utf-8-sig')
    print(f"Также сохранен CSV: {csv_file}")

    return df_final

def create_sample_orders_file(filename='заказы_образец.xlsx'):
    """
    Создание образца файла заказов с правильными колонками
    """
    sample_data = {
        'ФИО_клиента': [
            'Иванов Иван Иванович',
            'Петров Петр Петрович',
            'Сидорова Анна Сергеевна'
        ],
        'Дата_заказа': [
            '2025-01-15',
            '2025-01-20',
            '2025-01-25'
        ],
        'Название_книги': [
            'Мастер и Маргарита',
            'Война и мир',
            'Преступление и наказание'
        ],
        'Автор': [
            'Михаил Булгаков',
            'Лев Толстой',
            'Фёдор Достоевский'
        ],
        'Жанр': [
            'Классика',
            'Классика',
            'Классика'
        ],
        'Количество': [1, 2, 1],
        'Цена_за_шт': [450, 890, 380],
        'Скидка_%': [10, 0, 15],
        'Статус_заказа': [
            'Обработан',
            'В доставке',
            'Ожидает оплаты'
        ],
        'Способ_доставки': [
            'Курьерская доставка',
            'Почта России',
            'Самовывоз'
        ],
        'Примечание_к_заказу': [
            'Позвонить перед доставкой',
            '',
            'Подарочная упаковка'
        ]
    }

    df = pd.DataFrame(sample_data)
    df.to_excel(filename, index=False)
    print(f"Создан файл-образец: {filename}")
    print(f"Колонки: {list(df.columns)}")

    return df

if __name__ == "__main__":
    # Конвертируем существующий файл
    input_file = "book_orders.xlsx"
    output_file = "заказы_исправленный.xlsx"

    if os.path.exists(input_file):
        df = convert_orders_file(input_file, output_file)
        print("\n" + "="*50)
        print("Первые 5 строк результата:")
        print(df.head())
    else:
        print(f"Файл {input_file} не найден!")
        print("Создаю файл-образец...")
        create_sample_orders_file()
