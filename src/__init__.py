import pandas as pd


def load_data(file_path):
    """Загружает данные из CSV-файла, обрабатывает текстовые поля и даты."""
    df = pd.read_csv(file_path, low_memory=False)

    # Обработка дат
    for col in ['create_date', 'update_date']:
        df[col] = pd.to_datetime(df[col], errors='coerce')
        df[col] = df[col].apply(lambda x: None if pd.notna(x) and (x.year < 1924 or x.year > 2024) else x)

    # Приведение текстовых полей к верхнему регистру и удаление лишних пробелов
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].str.strip().str.upper()

    return df


def find_important_columns(df, threshold=25):
    """Находит важные колонки на основе комбинированного показателя."""
    important_columns = []
    for column in df.columns:
        total_cells = len(df[column])
        non_empty_cells = df[column].notna().sum()
        percentage_non_empty = (non_empty_cells / total_cells) * 100
        unique_values = df[column].nunique()
        percentage_unique = (unique_values / non_empty_cells) * 100 if non_empty_cells > 0 else 0
        combined_score = (percentage_non_empty * percentage_unique) / 100

        if combined_score > threshold and column not in ['client_id', 'create_date', 'update_date']:
            important_columns.append(column)
    return important_columns


def merge_records_optimized(df, key_columns):
    """Группирует и объединяет данные в золотые записи, сохраняя все колонки."""
    # Определяем правила агрегации
    aggregation_rules = {}
    for col in df.columns:
        if col in key_columns:
            aggregation_rules[col] = 'first'  # Ключевые столбцы — оставляем как есть
        elif col == 'update_date':
            aggregation_rules[col] = 'max'  # Последняя дата обновления
        elif col == 'create_date':
            aggregation_rules[col] = 'min'  # Первая дата создания
        else:
            aggregation_rules[col] = 'first'  # Берем первое значение в группе по умолчанию

    # Применяем группировку и агрегирование
    golden_data = df.groupby(key_columns, as_index=False).agg(aggregation_rules)
    return golden_data


def save_golden_records(df, file_path):
    """Сохраняет золотые записи в новый CSV-файл."""
    df.to_csv(file_path, index=False)


def main():
    # Путь к файлу с исходными данными
    input_file_path = 'ds_dirty_fin_202410041147.csv'

    # Путь к файлу для сохранения золотых записей
    output_file_path = 'trash.csv'

    # Загрузим данные
    data = load_data(input_file_path)

    # Определяем важные колонки
    key_columns = find_important_columns(data, threshold=25)

    # Проверяем, есть ли ключевые колонки
    if not key_columns:
        print("Нет колонок с комбинированным показателем выше заданного порога. Завершаем выполнение.")
        return

    print(f"Выбранные ключевые колонки для группировки: {', '.join(key_columns)}")

    # Группируем и объединяем данные
    golden_data = merge_records_optimized(data, key_columns)

    # Сохраняем результат
    save_golden_records(golden_data, output_file_path)

    print(f"Золотые записи сохранены в файл: {output_file_path}")
    print(f"Количество строк в золотой записи: {len(golden_data)}")


if __name__ == "__main__":
    main()