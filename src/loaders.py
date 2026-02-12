import pandas as pd
from pathlib import Path


def load_settings(settings_path: Path) -> dict:
    """Завантажує всі вкладки Excel налаштувань."""
    raw_sheets = pd.read_excel(settings_path, sheet_name=None)
    return {k.strip().lower(): v for k, v in raw_sheets.items()}


def load_exchange_rates(rates_dir: Path) -> pd.DataFrame:
    """Завантажує ВСІ файли курсів та об'єднує їх."""
    all_files = list(rates_dir.glob("*.xlsx"))
    if not all_files:
        print("[!] Попередження: файли курсів не знайдено.")
        return pd.DataFrame()

    df_list = [pd.read_excel(f) for f in all_files]
    df_rates = pd.concat(df_list, ignore_index=True)

    # Приводимо дату до формату date для швидкого пошуку в словнику
    df_rates['Starting Date'] = pd.to_datetime(df_rates['Starting Date']).dt.date
    # Залишаємо тільки унікальні курси (на випадок дублікатів у файлах)
    df_rates = df_rates.drop_duplicates(subset=['Starting Date', 'Currency Code'])
    return df_rates