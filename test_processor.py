import pandas as pd
from pathlib import Path
import pytest
from src import loaders


def test_reconciliation():
    """Звірка вхідних та вихідних сум з урахуванням мепінгів типів."""
    root = Path(__file__).parent
    q_path = root / "data" / "2025" / "Q4"
    results_path = root / "data" / "results" / "2025" / "Q4"

    # Завантажуємо мепінг T-кодів для правильної звірки PayPal
    settings = loaders.load_settings(root / "data" / "settings.xlsx")
    pp_map = settings.get('tr mapping pp').set_index('Paypal T-code')['Mapping'].to_dict()

    in_total = 0
    in_files = [f for f in q_path.rglob("*.csv") if "results" not in str(f) and "_output" not in str(f)]

    print("\n🔍 АНАЛІЗ ВХІДНИХ ДАНИХ:")
    for f in in_files:
        df = pd.read_csv(f)
        df.columns = [c.strip() for c in df.columns]

        # Визначаємо тип транзакції для зміни знаку
        if 'paypal' in str(f).lower():
            mapped_t = df['type'].map(pp_map).fillna('other').str.lower()
        else:
            mapped_t = df['operation_type'].str.lower()

        # Логіка інверсії знаку для видатків (refund, chargeback, chb)
        df['adj'] = df.apply(
            lambda x: -abs(x['amount']) if any(t in str(mapped_t.loc[x.name]) for t in ['refund', 'chargeback', 'chb'])
            else x['amount'], axis=1
        )
        file_sum = df['adj'].sum()
        in_total += file_sum
        print(f"   📄 {f.name} | Сума: {file_sum:,.2f}")

    # Сума з оброблених файлів
    out_total = sum(pd.read_csv(f)['AMOUNT'].sum() for f in results_path.rglob("*_output.csv"))

    print(f"\n📊 ФІНАЛЬНА ЗВІРКА: Вхід ({in_total:,.2f}) vs Вихід ({out_total:,.2f})")
    assert abs(in_total - out_total) < 1.0


def test_vat_is_calculated():
    """Перевірка, що ПДВ розрахований (не нуль) для рядків зі ставкою."""
    root = Path(__file__).parent
    results_path = root / "data" / "results" / "2025" / "Q4"

    for csv in results_path.rglob("*_output.csv"):
        df = pd.read_csv(csv)
        # Якщо в файлі є рядки з ПДВ > 0, перевіряємо, що VAT USD теж не 0
        taxable_rows = df[df['VAT RATE'] > 0]
        if not taxable_rows.empty:
            assert (taxable_rows['VAT USD'] != 0).any(), f"ПДВ не розрахований у файлі {csv.name}"


def test_eu_vat_logic():
    """Перевірка коректності ставок ПДВ за кодами країн."""
    root = Path(__file__).parent
    results_path = root / "data" / "results" / "2025" / "Q4"

    # Очікувані ставки (після виправлення масштабу в utils.py)
    expected = {'LV': 21.0, 'CZ': 21.0, 'EE': 24.0, 'CY': 19.0, 'PL': 23.0}

    for csv in results_path.rglob("*_output.csv"):
        df = pd.read_csv(csv)
        # Використовуємо COUNTRY_CODE, який ми додали в target_cols
        if 'COUNTRY_CODE' in df.columns:
            for code, rate in expected.items():
                sample = df[df['COUNTRY_CODE'] == code]
                if not sample.empty:
                    val = sample.iloc[0]['VAT RATE']
                    assert val == rate, f"Помилка ставки для {code} у {csv.name}: чекали {rate}, маємо {val}"


def test_mapping_coverage():
    """Перевірка, що всі рядки отримали назву компанії та проекту."""
    root = Path(__file__).parent
    pivot_file = root / "data" / "results" / "2025" / "Q4" / "vat_quarter.csv"

    if pivot_file.exists():
        df = pd.read_csv(pivot_file)
        assert df['PROJECT NAME'].isna().sum() == 0, "Знайдено рядки без назви проекту!"
        assert df['LE'].isna().sum() == 0, "Знайдено рядки без назви компанії (LE)!"
    else:
        pytest.skip("Фінальний звіт ще не створено.")