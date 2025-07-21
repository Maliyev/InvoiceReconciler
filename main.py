
import pandas as pd
import numpy as np
import re

# --- 1. Конфигурация ---
BANK_HISTORY_FILE = 'Bank History.xls'
INVOICES_FILE = 'Invoices.xlsx'
OUTPUT_FILE = 'reconciliation_report.xlsx'

# --- 2. Загрузка и подготовка данных ---

def clean_voen(voen_series):
    """Очищает столбец VÖEN, извлекая только 10-значный номер."""
    # Используем регулярное выражение для извлечения чисел
    return voen_series.str.extract(r'(\d{10})', expand=False).fillna('').astype(str)

def load_invoices(file_path):
    """Загружает и подготавливает данные из файла счетов-фактур."""
    print(f"Загрузка счетов из {file_path}...")
    try:
        # Выбираем только нужные столбцы по их индексам
        # A=0, B=1, C=2, F=5, T=19
        invoice_cols = {
            '№': 'Invoice_Num',
            'Müştəri VÖEN': 'VOEN',
            'Müştəri Adı': 'Company_Name',
            'Tarix': 'Invoice_Date',
            'Cəmi': 'Total_Amount'
        }
        # Используем более надежный способ чтения, указывая usecols и header
        df = pd.read_excel(
            file_path,
            skiprows=11,
            header=0, # Первая строка после skiprows - это заголовок
            usecols=[0, 1, 2, 5, 19] # Используем индексы столбцов
        )
        # Переименовываем столбцы для удобства
        df.columns = ['Invoice_Num', 'VOEN', 'Company_Name', 'Invoice_Date', 'Total_Amount']

        df['VOEN'] = clean_voen(df['VOEN'].astype(str))
        df['Invoice_Date'] = pd.to_datetime(df['Invoice_Date'], format='%d-%m-%Y', errors='coerce')
        df['Total_Amount'] = pd.to_numeric(df['Total_Amount'], errors='coerce')

        # Создаем столбец для отслеживания остатка
        df['Remaining_Amount'] = df['Total_Amount']
        df['Status'] = 'Не оплачен'

        print(f"Загружено {len(df)} счетов.")
        return df.dropna(subset=['VOEN', 'Total_Amount'])
    except FileNotFoundError:
        print(f"ОШИБКА: Файл {file_path} не найден.")
        return None
    except Exception as e:
        print(f"ОШИБКА при чтении файла счетов: {e}")
        return None


def load_bank_history(file_path):
    """Загружает и подготавливает данные из банковской выписки."""
    print(f"Загрузка банковской истории из {file_path}...")
    try:
        # Используем pd.read_html для обработки файла, который является HTML-таблицей
        tables = pd.read_html(file_path, skiprows=17, header=None)
        df = tables[0]  # Обычно нужная таблица - первая в списке

        # Поскольку заголовков нет, присваиваем их вручную по индексам
        # VÖEN=0, Дата=1, Тип=2, Сумма=3, Описание=5
        df = df[[0, 1, 2, 3, 5]]
        df.columns = ['Payment_VOEN', 'Payment_Date', 'Transaction_Type', 'Payment_Amount', 'Description']

        # 1. Оставляем только поступления на счет
        df = df[df['Transaction_Type'].str.contains(r'\(\+\) CR', na=False)].copy()

        # 2. Очищаем и преобразуем данные
        df['Payment_VOEN'] = clean_voen(df['Payment_VOEN'].astype(str))
        df['Payment_Date'] = pd.to_datetime(df['Payment_Date'], format='%d.%m.%Y', errors='coerce')

        # Заменяем запятые на точки для корректного преобразования в число
        df['Payment_Amount'] = df['Payment_Amount'].astype(str).str.replace(',', '.', regex=False)
        df['Payment_Amount'] = pd.to_numeric(df['Payment_Amount'], errors='coerce')

        # 3. Сортируем по дате
        df = df.sort_values(by='Payment_Date').reset_index(drop=True)

        print(f"Загружено {len(df)} входящих транзакций.")
        return df.dropna(subset=['Payment_VOEN', 'Payment_Amount'])
    except FileNotFoundError:
        print(f"ОШИБКА: Файл {file_path} не найден.")
        return None
    except Exception as e:
        print(f"ОШИБКА при чтении банковской выписки: {e}")
        return None

# --- 3. Основная логика (пока пустая) ---
def reconcile_invoices(invoices_df, bank_history_df):
    """Основная функция для сопоставления счетов и транзакций."""
    print("Начало процесса сопоставления...")
    # (Здесь будет основная логика, которую мы добавим на следующем шаге)
    pass

# --- 4. Точка входа ---
if __name__ == "__main__":
    invoices = load_invoices(INVOICES_FILE)
    bank_history = load_bank_history(BANK_HISTORY_FILE)

    if invoices is not None and bank_history is not None:
        reconcile_invoices(invoices, bank_history)
        print("\nПроцесс завершен.")
    else:
        print("\nНе удалось загрузить один из файлов. Проверьте ошибки выше.")

