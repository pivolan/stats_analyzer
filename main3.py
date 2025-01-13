import random
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from faker import Faker

# Инициализация генератора фейковых данных
fake = Faker('ru_RU')

# Список товаров с базовыми характеристиками и моделями
products = {
    'Смартфон': {
        'base_price': 799.99,
        'base_daily_sales': 15,
        'seasonality': 'high_end',
        'models': [
            ('Apple', 'iPhone 15 Pro'),
            ('Samsung', 'Galaxy S24 Ultra'),
            ('Google', 'Pixel 8 Pro'),
            ('Xiaomi', '14 Pro'),
            ('OnePlus', '12')
        ]
    },
    'Наушники': {
        'base_price': 129.99,
        'base_daily_sales': 25,
        'seasonality': 'regular',
        'models': [
            ('Apple', 'AirPods Pro 2'),
            ('Samsung', 'Galaxy Buds2 Pro'),
            ('Sony', 'WF-1000XM5'),
            ('Jabra', 'Elite 8 Active'),
            ('Nothing', 'Ear 2')
        ]
    },
    'Чехол': {
        'base_price': 19.99,
        'base_daily_sales': 40,
        'seasonality': 'regular',
        'models': [
            ('Spigen', 'Ultra Hybrid'),
            ('OtterBox', 'Defender'),
            ('Mous', 'Limitless'),
            ('UAG', 'Monarch'),
            ('Apple', 'Leather Case')
        ]
    },
    'Зарядное устройство': {
        'base_price': 29.99,
        'base_daily_sales': 30,
        'seasonality': 'regular',
        'models': [
            ('Anker', 'Nano Pro'),
            ('Belkin', 'BoostCharge Pro'),
            ('Apple', '20W USB-C'),
            ('Samsung', '25W Super Fast'),
            ('Baseus', 'GaN Pro')
        ]
    },
    'Планшет': {
        'base_price': 499.99,
        'base_daily_sales': 8,
        'seasonality': 'high_end',
        'models': [
            ('Apple', 'iPad Pro 12.9'),
            ('Samsung', 'Galaxy Tab S9 Ultra'),
            ('Lenovo', 'Tab P12 Pro'),
            ('Microsoft', 'Surface Pro 9'),
            ('Xiaomi', 'Pad 6 Pro')
        ]
    }
}

def generate_datetime_for_day(date):
    """Генерирует случайное время для указанной даты"""
    hour = random.randint(9, 21)  # Магазин работает с 9 до 22
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    microsecond = random.randint(0, 999999)
    return date.replace(hour=hour, minute=minute, second=second, microsecond=microsecond)

def generate_daily_sales(date, product_info):
    # Базовое количество продаж
    base_sales = product_info['base_daily_sales']

    # Факторы влияния на продажи
    month = date.month
    day = date.day

    # Предпраздничные периоды
    holiday_boost = 1.0
    if (month == 12 and day > 15) or (month == 11 and day > 20):  # Новый год
        holiday_boost = 2.5
    elif (month == 2 and day > 10) or (month == 3 and day < 9):  # 23 февраля и 8 марта
        holiday_boost = 1.8

    # День недели (больше продаж в выходные)
    weekday = date.weekday()
    weekend_boost = 1.3 if weekday >= 5 else 1.0

    # Сезонность для дорогих товаров
    season_boost = 1.0
    if product_info['seasonality'] == 'high_end':
        if month in [9, 12]:
            season_boost = 1.4
        elif month in [1, 7]:
            season_boost = 0.7

    # Случайные колебания
    random_factor = random.uniform(0.7, 1.3)

    # Итоговое количество продаж
    final_sales = int(base_sales * holiday_boost * weekend_boost * season_boost * random_factor)

    # Генерация цены с небольшими колебаниями
    price = product_info['base_price'] * random.uniform(0.95, 1.05)

    return final_sales, price

# Генерация данных за текущий год
current_year = datetime.now().year
start_date = datetime(current_year, 1, 1)
end_date = datetime(current_year, 12, 31)

# Создание списка для хранения данных
sales_data = []

# Генерация данных для каждого дня и продукта
current_date = start_date
while current_date <= end_date:
    for product_name, product_info in products.items():
        sales_count, price = generate_daily_sales(current_date, product_info)

        # Генерируем указанное количество продаж на этот день
        for _ in range(sales_count):
            # Генерация времени продажи
            sale_datetime = generate_datetime_for_day(current_date)

            # Выбор случайной модели для продукта
            brand, model = random.choice(product_info['models'])

            sales_data.append({
                'datetime': sale_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                'date': sale_datetime.date(),
                'datetime_ms': sale_datetime.strftime('%Y-%m-%d %H:%M:%S.%f'),
                'product': product_name,
                'brand': brand,
                'model': model,
                'price': round(price, 2),
                'username': fake.user_name(),
                'total_revenue': round(price, 2)
            })

    current_date += timedelta(days=1)

# Создание DataFrame
df = pd.DataFrame(sales_data)

# Сортировка по дате и времени
df = df.sort_values('datetime')

# Сохранение в CSV
filename = f'sales_data_{current_year}.csv'
df.to_csv(filename, index=False)

# Вывод примера данных и общей статистики
print("\nПример данных:")
print(df.head())

print("\nОбщая статистика:")
summary = df.groupby(['product', 'brand', 'model']).agg({
    'product': 'count',
    'total_revenue': 'sum'
}).rename(columns={'product': 'quantity'}).round(2)

print(summary)

print("\nФайл сохранен как:", filename)