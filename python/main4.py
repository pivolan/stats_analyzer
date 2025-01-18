import numpy as np
import pandas as pd

# Устанавливаем seed для воспроизводимости
np.random.seed(42)

# Количество записей
n_records = 10000

# Генерируем данные
data = {
    # Цены товаров (логнормальное распределение)
    'price': np.random.lognormal(mean=4.5, sigma=0.5, size=n_records),

    # Рейтинг товара (нормальное распределение от 1 до 5)
    'rating': np.clip(np.random.normal(loc=4.2, scale=0.5, size=n_records), 1, 5),

    # Количество просмотров (степенное распределение)
    'views': np.random.pareto(a=2, size=n_records) * 1000,

    # Время на странице в секундах (экспоненциальное распределение)
    'time_on_page': np.random.exponential(scale=60, size=n_records),

    # Размер скидки в процентах (дискретное распределение)
    'discount_percent': np.random.choice([0, 5, 10, 15, 20, 25, 30], size=n_records,
                                         p=[0.4, 0.2, 0.15, 0.1, 0.08, 0.05, 0.02]),

    # Количество покупок (пуассоновское распределение)
    'purchases': np.random.poisson(lam=2.5, size=n_records),

    # Возраст покупателя (нормальное распределение)
    'customer_age': np.random.normal(loc=35, scale=12, size=n_records).astype(int),

    # Размер корзины (логнормальное распределение)
    'cart_size': np.random.lognormal(mean=2, sigma=0.5, size=n_records),

    # Время до покупки в днях (экспоненциальное распределение)
    'days_to_purchase': np.random.exponential(scale=7, size=n_records),

    # Расстояние доставки в км (равномерное распределение)
    'delivery_distance': np.random.uniform(low=0.5, high=30, size=n_records)
}

# Создаем DataFrame
df = pd.DataFrame(data)

# Округляем некоторые значения для большей реалистичности
df['price'] = df['price'].round(2)
df['rating'] = df['rating'].round(1)
df['views'] = df['views'].astype(int)
df['time_on_page'] = df['time_on_page'].round(1)
df['cart_size'] = df['cart_size'].round(2)
df['days_to_purchase'] = df['days_to_purchase'].round(1)
df['delivery_distance'] = df['delivery_distance'].round(1)

# Заменяем отрицательные значения возраста на случайные значения между 18 и 80
df.loc[df['customer_age'] < 18, 'customer_age'] = np.random.randint(18, 80, size=len(df[df['customer_age'] < 18]))
df.loc[df['customer_age'] > 80, 'customer_age'] = np.random.randint(18, 80, size=len(df[df['customer_age'] > 80]))

# Сохраняем в CSV
df.to_csv('ecommerce_data.csv', index=False)

# Выводим описательную статистику
print("\nDescriptive Statistics:")
print(df.describe())

# Выводим первые несколько строк
print("\nFirst few rows:")
print(df.head())