import pandas as pd
import os
import sys


def load_data(file_path):
    """Загружает данные из CSV-файла, обрабатывает текстовые поля и даты."""
    df = pd.read_csv(file_path, low_memory=False)

    # Обработка дат
    for col in ['create_date', 'update_date', 'client_bday']:
        df[col] = pd.to_datetime(df[col], errors='coerce')
        df[col] = df[col].apply(lambda x: None if pd.notna(x) and (x.year < 1924 or x.year > 2024) else x)

    # Приведение текстовых полей к верхнему регистру и удаление лишних пробелов
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].str.strip().str.upper()

    return df


def find_important_columns(df, threshold=20):
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
    """
    Группирует и объединяет данные в золотые записи, выбирая для остальных колонок
    значения, соответствующие самой новой дате по update_date.
    """
    # Проверяем, что колонка update_date есть в датафрейме
    if 'update_date' not in df.columns:
        raise ValueError("Колонка 'update_date' отсутствует в датафрейме!")

    # Убедимся, что update_date — это тип даты
    df['update_date'] = pd.to_datetime(df['update_date'], errors='coerce')

    # Найдем индексы строк с максимальной update_date для каждой группы
    max_date_idx = (
        df.loc[df['update_date'].notna()]
        .sort_values(by=['update_date'], ascending=False)
        .drop_duplicates(subset=key_columns)
        .index
    )

    # Создаем новый датафрейм на основе группировок
    result = df.loc[max_date_idx].copy()

    # Для create_date берем минимальную дату в каждой группе
    min_create_date = (
        df.groupby(key_columns)['create_date']
        .min()
        .reset_index()
    )

    # Объединяем результат с минимальными значениями create_date
    result = result.merge(min_create_date, on=key_columns, suffixes=('', '_min'))

    # Переносим минимальную дату на основное место и удаляем временный столбец
    result['create_date'] = result['create_date_min']
    result.drop(columns=['create_date_min'], inplace=True)

    return result.reset_index(drop=True)


def save_golden_records(df, file_path):
    """Сохраняет золотые записи в новый CSV-файл."""
    df.to_csv(file_path, index=False)


def get_output_filename(input_file_path):
    """Создает имя выходного файла на основе имени входного файла."""
    base_name = os.path.basename(input_file_path).split('.')[0]
    return f'golden_records_{base_name}.csv'


def main():
    # Запрашиваем у пользователя путь к входному файлу
    input_file_path = input("Пожалуйста, введите полный путь к входному файлу: ")
    print("Алгоритм успешно запущен...")
    # Если файл не существует, сообщаем об ошибке и завершаем работу
    if not os.path.exists(input_file_path):
        print(f"Входной файл '{input_file_path}' не найден. Программа завершает работу.")
        sys.exit(1)

    # Генерируем имя выходного файла
    output_file_path = get_output_filename(input_file_path)

    # Загружаем данные
    data = load_data(input_file_path)

    # Находим важные колонки
    key_columns = find_important_columns(data, threshold=20)

    # Проверяем наличие ключевых колонок
    if not key_columns:
        print("Нет колонок с комбинированным показателем выше заданного порога. Завершаем выполнение.")
        sys.exit(1)

    print(f"Выбранные ключевые колонки для группировки: {', '.join(key_columns)}")

    # Группируем и объединяем данные
    golden_data = merge_records_optimized(data, key_columns)

    # Сохраняем результат
    save_golden_records(golden_data, output_file_path)

    print(f"Золотые записи сохранены в файл: {output_file_path}")
    print(f"Количество строк в золотой записи: {len(golden_data)}")


if __name__ == "__main__":
    main()