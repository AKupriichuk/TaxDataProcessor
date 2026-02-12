import pandas as pd
from pathlib import Path
import shutil
from src import loaders, utils
from src.processors import paypal_processor, stripe_processor


def main():
    print("\n" + "=" * 50 + "\nTAX DATA PROCESSOR v1.5 (FINAL STABLE)\n" + "=" * 50)
    input_str = input("[?] Шлях до кварталу: ").strip()
    q_path = Path(input_str).resolve()

    if not q_path.exists(): return

    data_root = q_path.parents[1]
    results_base = data_root / "results"

    # 1. Повне очищення результатів перед запуском
    if results_base.exists():
        shutil.rmtree(results_base)
    results_base.mkdir(parents=True, exist_ok=True)

    # 2. Завантаження ресурсів
    settings = loaders.load_settings(data_root / "settings.xlsx")
    rates_df = loaders.load_exchange_rates(data_root / "_rates")

    all_data = []

    # 3. Обробка файлів
    files = [f for f in q_path.rglob("*.csv") if "_output" not in f.name]

    for file_path in files:
        parts = [p.lower() for p in file_path.parts]
        source = 'paypal' if 'paypal' in parts else 'stripe'

        if source == 'paypal':
            le_folder, month_folder = file_path.parent.name, file_path.parent.parent.name
            df = paypal_processor.process_paypal(file_path, le_folder, month_folder, q_path.parent.name, settings)
            file_id = f"paypal_{le_folder.replace(' ', '_')}"
        else:
            df = stripe_processor.process_stripe(file_path)
            le_val = str(df['LE_REF'].iloc[0]).replace(' ', '_')
            file_id = f"stripe_{le_val}_{file_path.stem}"

        # Корекція знаків: Refund/CHB завжди мінус, решта завжди плюс
        df['type_tmp'] = df['TYPE'].astype(str).str.lower()
        df['AMOUNT'] = df.apply(
            lambda x: -abs(x['amount']) if any(t in x['type_tmp'] for t in ['refund', 'chb', 'chargeback']) else abs(
                x['amount']), axis=1)

        df['YEAR'], df['MONTH'] = df['DATE'].dt.year, df['DATE'].dt.month

        # Складений мепінг LE
        le_sheet = settings.get('le')
        le_map = {**le_sheet.set_index('SHORT')['FULL NAME'].to_dict(),
                  **le_sheet.set_index('LE')['FULL NAME'].to_dict()}
        df['LE'] = df['LE_REF'].map(le_map).fillna("NOT_MAPPED_LE_" + df['LE_REF'])

        # Мепінг Проекту
        proj_map = settings.get('project mapping').set_index('SUBPROJECT_TECH')['SUBPROJECT_MA'].to_dict()
        df['PROJECT NAME'] = df['PROJ_CODE'].map(proj_map).fillna("NOT_MAPPED_PROJ_" + df['PROJ_CODE'])

        # VAT та USD
        df = utils.apply_vat_logic(df, settings)
        df['rate_usd'] = df.apply(lambda x: utils.get_usd_rate(rates_df, x['DATE'], x.get('currency', 'USD')),
                                  axis=1).fillna(1.0)

        df['AMOUNT USD'] = (df['AMOUNT'] * df['rate_usd']).round(2)
        df['VAT USD'] = (df['VAT Currency'] * df['rate_usd']).round(2)

        # Збереження окремих результатів
        out_month = f"{df['MONTH'].iloc[0]:02d}"
        month_dir = results_base / str(df['YEAR'].iloc[0]) / q_path.name / out_month
        month_dir.mkdir(parents=True, exist_ok=True)

        cols = ['YEAR', 'MONTH', 'LE', 'PROJECT NAME', 'AMOUNT', 'AMOUNT USD', 'TYPE', 'COUNTRY NAME', 'VAT RATE',
                'VAT Currency', 'VAT USD']
        df[cols].to_csv(month_dir / f"{file_id}_output.csv", index=False)
        all_data.append(df[cols])
        print(f"   [OK] {source.upper()}: {file_path.name}")

    # 4. ФІНАЛЬНИЙ ПІВОТ (Тільки податкові типи)
    if all_data:
        grand_df = pd.concat(all_data)

        # Критично: фільтруємо тільки Sales, Refund, CHB для фінального звіту
        tax_eligible = ['sales', 'sale', 'refund', 'chb', 'chargeback']
        tax_only = grand_df[grand_df['TYPE'].astype(str).str.lower().isin(tax_eligible)].copy()

        pivot = tax_only.groupby(['YEAR', 'MONTH', 'LE', 'PROJECT NAME'])[['AMOUNT USD', 'VAT USD']].sum().reset_index()
        pivot = pivot.round(2)

        pivot_path = results_base / q_path.parent.name / q_path.name / "vat_quarter.csv"
        pivot_path.parent.mkdir(parents=True, exist_ok=True)
        pivot.to_csv(pivot_path, index=False)
        print(f"\n📊 ГОТОВО! Звіт: {pivot_path}")


if __name__ == "__main__":
    main()