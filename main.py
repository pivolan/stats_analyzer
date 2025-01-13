import pandas as pd
import numpy as np

def analyze_csv(file_path: str):
    """
    Анализ CSV файла: определение заголовков, типов данных и базовой статистики
    """
    # Читаем CSV файл, пытаемся автоматически определить разделитель
    try:
        df = pd.read_csv(file_path, sep=None, engine='python')
    except Exception as e:
        print(f"Ошибка при чтении файла: {e}")
        return

    print("\n=== АНАЛИЗ CSV ФАЙЛА ===")

    # 1. Информация о структуре файла
    print("\n1. СТРУКТУРА ФАЙЛА:")
    print(f"Количество строк: {len(df)}")
    print(f"Количество столбцов: {len(df.columns)}")

    # 2. Заголовки и типы данных
    print("\n2. ЗАГОЛОВКИ И ТИПЫ ДАННЫХ:")
    for column in df.columns:
        print(f"Столбец: {column}")
        print(f"  - Тип данных: {df[column].dtype}")
        print(f"  - Количество непустых значений: {df[column].count()}")
        print(f"  - Количество уникальных значений: {df[column].nunique()}")

        # Для числовых столбцов выводим базовую статистику
        if np.issubdtype(df[column].dtype, np.number):
            stats = df[column].describe()
            print(f"  - Минимум: {stats['min']}")
            print(f"  - Максимум: {stats['max']}")
            print(f"  - Среднее: {stats['mean']:.2f}")
            print(f"  - Медиана: {stats['50%']:.2f}")

        # Для строковых столбцов и дат
        elif df[column].dtype == 'object' or df[column].dtype == 'datetime64[ns]':
            # Проверяем, может ли столбец быть датой
            try:
                pd.to_datetime(df[column], errors='raise')
                print("  - Похоже на столбец с датами")
            except:
                pass

            if df[column].nunique() < 10:  # Показываем только если уникальных значений мало
                print("  - Примеры значений:", df[column].unique()[:5].tolist())

        print()  # Пустая строка между столбцами

    # 3. Проверка на пропущенные значения
    print("\n3. ПРОПУЩЕННЫЕ ЗНАЧЕНИЯ:")
    missing_data = df.isnull().sum()
    if missing_data.sum() > 0:
        for column in df.columns:
            missing_count = missing_data[column]
            if missing_count > 0:
                print(f"{column}: {missing_count} пропущенных значений")
    else:
        print("Пропущенных значений не найдено")


if __name__ == "__main__":
    import time
    from datetime import datetime
    import psutil
    import os

    # Укажите путь к вашему CSV файлу
    file_path = "./sales_data_2025.csv"

    # Получаем текущий процесс
    process = psutil.Process()
    # Измеряем начальное использование памяти
    start_memory = process.memory_info().rss / 1024 / 1024  # Конвертируем в МБ

    # Засекаем время начала
    start_time = time.time()
    start_datetime = datetime.now()

    # Выполняем анализ
    analyze_csv(file_path)

    # Вычисляем время выполнения
    end_time = time.time()
    end_datetime = datetime.now()

    # Измеряем конечное использование памяти
    end_memory = process.memory_info().rss / 1024 / 1024  # Конвертируем в МБ

    execution_time = end_time - start_time
    memory_used = end_memory - start_memory

    print("\n=== ВРЕМЯ ВЫПОЛНЕНИЯ ===")
    print(f"Начало выполнения: {start_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')}")
    print(f"Конец выполнения: {end_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')}")
    print(f"Затраченное время: {execution_time:.6f} секунд")

    print("\n=== ИСПОЛЬЗОВАНИЕ ПАМЯТИ ===")
    print(f"Начальное использование памяти: {start_memory:.2f} МБ")
    print(f"Конечное использование памяти: {end_memory:.2f} МБ")
    print(f"Использовано дополнительно: {memory_used:.2f} МБ")