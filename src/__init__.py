import pandas as pd
from datetime import datetime


def load_data(file_path):
    """Загружает данные из CSV-файла."""
    df = pd.read_csv(file_path)
    for col in ['create_date', 'update_date']:
        df[col] = pd.to_datetime(df[col], errors='coerce')
    return df


def group_and_merge_data(df, key_columns, sub_key_columns):
    """Группирует данные по ключам и объединяет их в золотые записи."""
    grouped_data = {}

    for index, row in df.iterrows():

        key = tuple(tuple(row[key_column] for key_column in key_columns), tuple(row[sub_key_column] for sub_key_column in sub_key_columns))
        if key in grouped_data:
            grouped_data[key].append(row)
        else:
            grouped_data[key] = [row]

    golden_records = []
    for key, records in grouped_data.items():
        golden_records.append(merge_records(records))

    return pd.DataFrame(golden_records)


def merge_records(records):
    """Формирует золотую запись из списка записей."""
    merged_record = {}

    # Находим самую свежую запись по дате обновления
    latest_update = max(records, key=lambda r: r['update_date'])

    # Заполняем все поля из самой свежей записи
    for col in records[0].keys():
        merged_record[col] = latest_update[col]

    # Дополняем недостающие поля из других записей
    for record in records:
        for col in record.keys():
            if pd.isna(merged_record[col]) and not pd.isna(record[col]):
                merged_record[col] = record[col]

    return merged_record


def save_golden_records(df, file_path):
    """Сохраняет золотые записи в новый CSV-файл."""
    df.to_csv(file_path, index=False)


def main():
    # Путь к файлу с исходными данными
    input_file_path = 'ds_dirty_fin_202410041147.csv'

    # Путь к файлу для сохранения золотых записей
    output_file_path = 'golden_records.csv'

    # Список столбцов, по которым будем искать совпадения
    key_columns = ['contact_phone']
    sub_key_columns = ['client_snils', 'client_inn']

    # Загрузим данные
    data = load_data(input_file_path)

    # Сгруппируем и объединим данные
    golden_data = group_and_merge_data(data, key_columns, sub_key_columns)

    # Сохраняем результат
    save_golden_records(golden_data, output_file_path)
    print(f"Создано золотых записей: {len(golden_data)}. Результаты сохранены в '{output_file_path}'.")


if __name__ == "__main__":
    main()