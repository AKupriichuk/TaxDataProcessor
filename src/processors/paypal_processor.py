import pandas as pd
from src import utils


def process_paypal(file_path, le_short, month, year, settings):
    df = pd.read_csv(file_path)
    df['DATE'] = utils.get_last_day_of_month(year, month)
    df['LE_REF'] = le_short

    # Мепінг типу (вкладка 'tr mapping pp')
    tr_map_df = settings.get('tr mapping pp')
    if tr_map_df is not None:
        t_dict = tr_map_df.set_index('Paypal T-code')['Mapping'].to_dict()
        df['TYPE'] = df['type'].map(t_dict)

    # Проєкт (PSP=PAYPAL + LE)
    psp_df = settings.get('psp project')
    if psp_df is not None:
        proj = psp_df[(psp_df['PSP'] == 'PAYPAL') & (psp_df['LE'] == le_short)]['Project'].values
        df['PROJ_CODE'] = proj[0] if len(proj) > 0 else "Unknown"

    df['currency'] = df.get('currency', 'USD')
    return df