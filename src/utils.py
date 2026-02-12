import calendar
from datetime import datetime
import pandas as pd


def get_last_day_of_month(year, month):
    day = calendar.monthrange(int(year), int(month))[1]
    return datetime(int(year), int(month), day)


def get_usd_rate(rates_df, date, currency):
    currency = str(currency).upper()
    if currency == 'USD': return 1.0
    try:
        search_date = date.date() if hasattr(date, 'date') else pd.to_datetime(date).date()
        return rates_df.loc[(search_date, currency), 'Exchange Rate Amount']
    except Exception:
        return None


def apply_vat_logic(df, settings):
    """Професійний розрахунок VAT з валідацією типів."""
    # Тільки ці типи мають ПДВ зобов'язання за ТЗ
    vat_eligible = ['sales', 'sale', 'refund', 'chb', 'chargeback']
    c_col = 'buyer_country' if 'buyer_country' in df.columns else 'country'

    # Мепінг країни
    geo_df = settings.get('geo')
    geo_map = geo_df.set_index('Alpha-2 code')['English short name'].to_dict()
    df['COUNTRY NAME'] = df[c_col].map(geo_map).fillna("Unknown")

    # Мепінг ставок (обробка 0.21 -> 21.0)
    vat_sheet = settings.get('vat_rates').copy()
    vat_sheet['rate_val'] = pd.to_numeric(vat_sheet['Standard Rate'], errors='coerce')
    vat_sheet.loc[vat_sheet['rate_val'] < 1, 'rate_val'] *= 100
    vat_map = vat_sheet.set_index('Code')['rate_val'].to_dict()

    df['VAT RATE'] = df[c_col].map(vat_map).fillna(0)

    # Розрахунок VAT Currency
    df['type_low'] = df['TYPE'].astype(str).str.lower()
    df['VAT Currency'] = 0.0

    # Рахуємо ПДВ ТІЛЬКИ для eligible типів
    mask = df['type_low'].isin(vat_eligible) & (df['VAT RATE'] > 0)
    # ПДВ бере знак від суми (Amount)
    df.loc[mask, 'VAT Currency'] = (df['AMOUNT'] * (df['VAT RATE'] / 100)).round(4)

    return df