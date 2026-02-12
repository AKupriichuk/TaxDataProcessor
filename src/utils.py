import pandas as pd
import calendar
from datetime import datetime


def get_last_day_of_month(year, month):
    """Повертає дату останнього дня місяця."""
    day = calendar.monthrange(int(year), int(month))[1]
    return datetime(int(year), int(month), day)


def apply_vat_logic(df, settings):
    """Виправлена логіка ПДВ з урахуванням різних форматів ставок."""
    # 1. Типи, що підлягають оподаткуванню згідно з ТЗ
    vat_eligible_types = ['sale', 'sales', 'refund', 'chargeback', 'chb']

    # 2. Мапінг країни
    geo = settings.get('geo')
    c_col = 'country' if 'country' in df.columns else 'buyer_country'
    if geo is not None:
        geo_dict = geo.set_index('Alpha-2 code')['English short name'].to_dict()
        df['COUNTRY NAME'] = df[c_col].map(geo_dict)

    # 3. Обробка ставок ПДВ (0.21 -> 21.0)
    vat_sheet = settings.get('vat_rates')
    if vat_sheet is not None:
        # Видаляємо символ % якщо він є, і перетворюємо на число
        vat_sheet['Rate_Num'] = vat_sheet['Standard Rate'].astype(str).str.replace('%', '').astype(float)
        vat_dict = vat_sheet.set_index('Code')['Rate_Num'].to_dict()
        df['VAT RATE'] = df[c_col].map(vat_dict).fillna(0)

    # Виправляємо масштаб ставки (якщо прийшло 0.21 замість 21)
    mask_small = (df['VAT RATE'] > 0) & (df['VAT RATE'] < 1)
    df.loc[mask_small, 'VAT RATE'] = df.loc[mask_small, 'VAT RATE'] * 100

    # 4. Розрахунок ПДВ
    df['type_low'] = df['mapped_type'].astype(str).str.lower()
    df['VAT Currency'] = 0.0
    mask_eligible = df['type_low'].isin(vat_eligible_types)
    df.loc[mask_eligible, 'VAT Currency'] = df['AMOUNT'] * (df['VAT RATE'] / 100)

    return df