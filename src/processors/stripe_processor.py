import pandas as pd

def process_stripe(file_path):
    df = pd.read_csv(file_path)
    df['DATE'] = pd.to_datetime(df['date'])
    df['LE_REF'] = df['Legal entity']
    df['TYPE'] = df['operation_type']
    df['PROJ_CODE'] = df['project']
    return df