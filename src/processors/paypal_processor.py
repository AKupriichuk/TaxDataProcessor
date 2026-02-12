import pandas as pd
from src.utils import get_last_day_of_month


def process_paypal(file_path, le_short, month, year, settings):
    df = pd.read_csv(file_path)
    df.columns = [c.strip() for c in df.columns]

    df['date_proc'] = get_last_day_of_month(year, month)
    df['le_ref'] = le_short
    df['YEAR'] = int(year)
    df['MONTH'] = int(month)

    # Мепінг типу
    t_map = settings.get('tr mapping pp').set_index('Paypal T-code')['Mapping'].to_dict()
    df['mapped_type'] = df['type'].map(t_map)

    # Проект
    psp_sheet = settings.get('psp project')
    p_code = psp_sheet[psp_sheet['LE'] == le_short]['Project'].values
    df['proj_code'] = p_code[0] if len(p_code) > 0 else "Unknown"

    # Знак суми
    df['type_low'] = df['mapped_type'].astype(str).str.lower()
    df['AMOUNT'] = df.apply(lambda x: -abs(x['amount']) if x['type_low'] in ['refund', 'chargeback'] else x['amount'],
                            axis=1)

    return df