import pandas as pd
from pathlib import Path


def load_settings(path: Path) -> dict:
    """Завантажує налаштування з Excel та нормалізує назви вкладок."""
    print(f"[*] Читання налаштувань: {path}")
    if not path.exists():
        raise FileNotFoundError(f"Файл settings не знайдено: {path}")

    # Читаємо всі листи та приводимо назви до нижнього регістру без пробілів
    sheets = pd.read_excel(path, sheet_name=None)
    return {k.strip().lower(): v for k, v in sheets.items()}


def load_exchange_rates(rates_dir: Path) -> pd.DataFrame:
    """Завантажує всі курси валют з папки _rates."""
    all_files = list(rates_dir.glob("*.xlsx"))
    if not all_files:
        print("[!] Попередження: Папка _rates порожня!")
        return pd.DataFrame()

    df_list = [pd.read_excel(f) for f in all_files]
    df_rates = pd.concat(df_list, ignore_index=True)
    df_rates['Starting Date'] = pd.to_datetime(df_rates['Starting Date']).dt.date
    return df_rates.set_index(['Starting Date', 'Currency Code'])