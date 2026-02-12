import pandas as pd


def process_stripe(file_path, settings):
    df = pd.read_csv(file_path)
    df.columns = [c.strip() for c in df.columns]

    df['date_proc'] = pd.to_datetime(df['date'])
    df['YEAR'] = df['date_proc'].dt.year
    df['MONTH'] = df['date_proc'].dt.month
    df['le_ref'] = df['Legal entity']

    le_sheet = settings.get('le')
    if le_sheet is not None:
        le_map = le_sheet.set_index('LE')['FULL NAME'].to_dict()
        df['LE'] = df['le_ref'].map(le_map)

    df['proj_code'] = df['project']
    df['mapped_type'] = df['operation_type']

    df['type_low'] = df['mapped_type'].astype(str).str.lower()
    df['AMOUNT'] = df.apply(lambda x: -abs(x['amount']) if x['type_low'] in ['refund', 'chargeback'] else x['amount'],
                            axis=1)

    return df