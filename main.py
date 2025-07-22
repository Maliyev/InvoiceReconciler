
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
        # Делим на 100, так как суммы могут быть завышены из-за отсутствия десятичной точки
        df['Payment_Amount'] = df['Payment_Amount'] / 100

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

    # Создаем копию DataFrame для отслеживания изменений
    invoices_working_df = invoices_df.copy()
    bank_history_working_df = bank_history_df.copy()

    # Список для хранения результатов сопоставления
    reconciliation_results = []

    # Проходим по каждой банковской транзакции
    for index, payment in bank_history_working_df.iterrows():
        current_payment_amount = payment['Payment_Amount']
        current_payment_voen = payment['Payment_VOEN']
        payment_processed = False # Флаг, был ли платеж сопоставлен хоть с чем-то

        # Ищем неоплаченные счета для текущего VÖEN
        matching_invoices = invoices_working_df[
            (invoices_working_df['VOEN'] == current_payment_voen) &
            (invoices_working_df['Remaining_Amount'] > 0)
        ].sort_values(by='Invoice_Date') # Сортируем по дате, чтобы сначала погашать старые счета

        for inv_idx, invoice in matching_invoices.iterrows():
            if current_payment_amount <= 0:
                break # Платеж исчерпан

            amount_to_apply = min(current_payment_amount, invoice['Remaining_Amount'])

            # Обновляем остаток по счету
            invoices_working_df.loc[inv_idx, 'Remaining_Amount'] -= amount_to_apply
            # Обновляем статус счета
            if invoices_working_df.loc[inv_idx, 'Remaining_Amount'] <= 0:
                invoices_working_df.loc[inv_idx, 'Status'] = 'Оплачен полностью'
            elif invoices_working_df.loc[inv_idx, 'Remaining_Amount'] < invoices_working_df.loc[inv_idx, 'Total_Amount']:
                invoices_working_df.loc[inv_idx, 'Status'] = 'Оплачен частично'

            # Уменьшаем текущую сумму платежа
            current_payment_amount -= amount_to_apply
            payment_processed = True

            # Добавляем запись о сопоставлении
            reconciliation_results.append({
                'Bank_Payment_Date': payment['Payment_Date'],
                'Bank_Payment_VOEN': payment['Payment_VOEN'],
                'Bank_Payment_Amount': payment['Payment_Amount'],
                'Bank_Description': payment['Description'],
                'Applied_Amount': amount_to_apply,
                'Invoice_Num': invoice['Invoice_Num'],
                'Invoice_VOEN': invoice['VOEN'],
                'Invoice_Company_Name': invoice['Company_Name'],
                'Invoice_Date': invoice['Invoice_Date'],
                'Invoice_Total_Amount': invoice['Total_Amount'],
                'Invoice_Remaining_Amount_After_Payment': invoices_working_df.loc[inv_idx, 'Remaining_Amount'],
                'Invoice_Status': invoices_working_df.loc[inv_idx, 'Status']
            })

        # Если платеж не был полностью использован или не нашел соответствий
        if current_payment_amount > 0 and not payment_processed:
            reconciliation_results.append({
                'Bank_Payment_Date': payment['Payment_Date'],
                'Bank_Payment_VOEN': payment['Payment_VOEN'],
                'Bank_Payment_Amount': payment['Payment_Amount'],
                'Bank_Description': payment['Description'],
                'Applied_Amount': 0, # Не сопоставлено
                'Invoice_Num': None,
                'Invoice_VOEN': None,
                'Invoice_Company_Name': None,
                'Invoice_Date': None,
                'Invoice_Total_Amount': None,
                'Invoice_Remaining_Amount_After_Payment': None,
                'Invoice_Status': 'Платеж без соответствия'
            })
        elif current_payment_amount > 0 and payment_processed:
             # Если платеж был частично использован, но остался остаток
             reconciliation_results.append({
                'Bank_Payment_Date': payment['Payment_Date'],
                'Bank_Payment_VOEN': payment['Payment_VOEN'],
                'Bank_Payment_Amount': payment['Payment_Amount'],
                'Bank_Description': payment['Description'],
                'Applied_Amount': payment['Payment_Amount'] - current_payment_amount, # Сколько было применено
                'Invoice_Num': None, # Это остаток платежа, не привязанный к конкретному инвойсу
                'Invoice_VOEN': None,
                'Invoice_Company_Name': None,
                'Invoice_Date': None,
                'Invoice_Total_Amount': None,
                'Invoice_Remaining_Amount_After_Payment': None,
                'Invoice_Status': f'Остаток платежа: {current_payment_amount:.2f}'
            })


    # Создаем DataFrame из результатов сопоставления
    results_df = pd.DataFrame(reconciliation_results)

    # Переименовываем столбцы для отчета
    results_df.rename(columns={
        'Bank_Payment_Date': 'Дата Оп.',
        'Bank_Payment_VOEN': 'ВОЕН Банк',
        'Bank_Payment_Amount': 'Сумма Оп.',
        'Bank_Description': 'Описание Оп.',
        'Applied_Amount': 'Применено',
        'Invoice_Num': '№ Инв.',
        'Invoice_VOEN': 'ВОЕН Инв.',
        'Invoice_Company_Name': 'Компания',
        'Invoice_Date': 'Дата Инв.',
        'Invoice_Total_Amount': 'Сумма Инв.',
        'Invoice_Remaining_Amount_After_Payment': 'Остаток Инв.',
        'Invoice_Status': 'Статус Инв.'
    }, inplace=True)

    # Добавляем неоплаченные/частично оплаченные счета в конец отчета
    unpaid_invoices = invoices_working_df[invoices_working_df['Remaining_Amount'] > 0].copy()
    if not unpaid_invoices.empty:
        # Создаем пустые столбцы для банковских данных
        for col in ['Bank_Payment_Date', 'Bank_Payment_VOEN', 'Bank_Payment_Amount', 'Bank_Description', 'Applied_Amount']:
            unpaid_invoices[col] = np.nan

        # Переименовываем столбцы для соответствия общему формату
        unpaid_invoices.rename(columns={
            'Invoice_Num': '№ Инв.',
            'VOEN': 'ВОЕН Инв.',
            'Company_Name': 'Компания',
            'Invoice_Date': 'Дата Инв.',
            'Total_Amount': 'Сумма Инв.',
            'Remaining_Amount': 'Остаток Инв.',
            'Status': 'Статус Инв.'
        }, inplace=True)

        # Выбираем и переупорядочиваем столбцы для неоплаченных счетов
        unpaid_invoices = unpaid_invoices[[
            'Bank_Payment_Date', 'Bank_Payment_VOEN', 'Bank_Payment_Amount', 'Bank_Description', 'Applied_Amount',
            '№ Инв.', 'ВОЕН Инв.', 'Компания', 'Дата Инв.', 'Сумма Инв.',
            'Остаток Инв.', 'Статус Инв.'
        ]]
        results_df = pd.concat([results_df, unpaid_invoices], ignore_index=True)

    # Явно преобразуем столбцы с датами в datetime после конкатенации
    for col in ['Дата Оп.', 'Дата Инв.']:
        if col in results_df.columns:
            results_df[col] = pd.to_datetime(results_df[col], errors='coerce')

    # Форматируем столбцы с датами перед сохранением
    for col in ['Дата Оп.', 'Дата Инв.']:
        if col in results_df.columns:
            results_df[col] = results_df[col].dt.date

    # Сохраняем результат в Excel файл
    try:
        writer = pd.ExcelWriter(OUTPUT_FILE, engine='xlsxwriter')
        results_df.to_excel(writer, index=False, sheet_name='Reconciliation Report')

        workbook  = writer.book
        worksheet = writer.sheets['Reconciliation Report']

        # Определяем ширину столбцов в пикселях, затем конвертируем в единицы Excel
        # Это приблизительная конвертация (пиксели / 7), может потребоваться корректировка
        column_widths_px = [
            80,  # Bank_Payment_Date
            75,  # Bank_Payment_VOEN
            30,  # Bank_Payment_Amount
            140, # Bank_Description
            80,  # Applied_Amount
            40,  # Invoice_Num
            80,  # Invoice_VOEN
            140, # Invoice_Company_Name
            80,  # Invoice_Date
            110, # Invoice_Total_Amount
            90,  # Invoice_Remaining_Amount_After_Payment
            90   # Invoice_Status (предполагаем такую же ширину)
        ]

        for i, width_px in enumerate(column_widths_px):
            excel_width = width_px / 7.0  # Примерный коэффициент конвертации
            worksheet.set_column(i, i, excel_width)

        writer.close() # Используем close() вместо save() для новых версий pandas
        print(f"Процесс сопоставления завершен. Отчет сохранен в {OUTPUT_FILE}")
    except Exception as e:
        print(f"ОШИБКА при сохранении отчета: {e}")


# --- 4. Точка входа ---
if __name__ == "__main__":
    invoices = load_invoices(INVOICES_FILE)
    bank_history = load_bank_history(BANK_HISTORY_FILE)

    if invoices is not None and bank_history is not None:
        reconcile_invoices(invoices, bank_history)
        print("\nПроцесс завершен.")
    else:
        print("\nНе удалось загрузить один из файлов. Проверьте ошибки выше.")

