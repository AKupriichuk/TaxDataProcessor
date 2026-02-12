import pandas as pd
from pathlib import Path
import shutil
from src import loaders, utils
from src.processors import paypal_processor, stripe_processor


def main():
    input_str = input("Введіть шлях до кварталу (напр. data/2025/Q4): ").strip()
    q_path = Path(input_str).resolve()
    data_root = q_path.parents[1]

    # Очистка папки результатів
    results_base = data_root / "results"
    if results_base.exists():
        shutil.rmtree(results_base)
    results_base.mkdir(parents=True, exist_ok=True)

    # Завантаження бази
    settings = loaders.load_settings(data_root / "settings.xlsx")
    rates_df = loaders.load_exchange_rates(data_root / "_rates")

    # Підготовка словника курсів для швидкодії (Senior advice)
    rates_dict = rates_df.set_index(['Starting Date', 'Currency Code'])['Exchange Rate Amount'].to_dict()

    all_dfs = []

    for file_path in q_path.rglob("*.csv"):
        if "results" in str(file_path) or "_output" in str(file_path):
            continue

        parts = [p.lower() for p in file_path.parts]

        if 'paypal' in parts:
            source = 'paypal'
            le_short = file_path.parent.name
            month = file_path.parent.parent.name
            year = q_path.parent.name
            df = paypal_processor.process_paypal(file_path, le_short, month, year, settings)

            # Мепінг FULL NAME для PayPal
            le_map = settings['le'].set_index('SHORT')['FULL NAME'].to_dict()
            df['LE'] = df['le_ref'].map(le_map)
            file_id = f"paypal_{month}_{le_short}_{file_path.stem}"

        elif 'stripe' in parts:
            source = 'stripe'
            df = stripe_processor.process_stripe(file_path, settings)
            month = str(df['MONTH'].iloc[0])
            year = str(df['YEAR'].iloc[0])
            file_id = f"stripe_{month}_{file_path.stem}"
        else:
            continue

        # Розрахунки ПДВ
        df = utils.apply_vat_logic(df, settings)

        # Виправлена обробка валюти (fix AttributeError)
        if 'currency' not in df.columns:
            df['currency'] = 'USD'
        df['currency'] = df['currency'].astype(str).str.upper()

        # Розрахунок курсів
        df['rate'] = df.apply(
            lambda x: rates_dict.get((x['date_proc'].date(), x['currency']), 1.0)
            if x['currency'] != 'USD' else 1.0, axis=1
        )

        df['AMOUNT USD'] = (df['AMOUNT'] * df['rate']).round(2)
        df['VAT USD'] = (df['VAT Currency'] * df['rate']).round(2)

        # Перейменування та мепінг проектів
        df = df.rename(columns={'mapped_type': 'TYPE', 'date_proc': 'DATE'})
        p_map = settings['project mapping'].set_index('SUBPROJECT_TECH')['SUBPROJECT_MA'].to_dict()
        df['PROJECT NAME'] = df['proj_code'].map(p_map)

        # Сортування колонок згідно ТЗ
        target_cols = ['YEAR', 'MONTH', 'LE', 'PROJECT NAME', 'AMOUNT', 'AMOUNT USD',
                       'TYPE', 'COUNTRY NAME', 'VAT RATE', 'VAT Currency', 'VAT USD']
        final_out = df.reindex(columns=target_cols)

        # Збереження
        out_dir = results_base / str(year) / q_path.name / str(month)
        out_dir.mkdir(parents=True, exist_ok=True)
        final_out.to_csv(out_dir / f"{file_id}_output.csv", index=False)

        all_dfs.append(final_out)
        print(f"✅ Оброблено: {file_id}")

    if all_dfs:
        final_concat = pd.concat(all_dfs, ignore_index=True)
        pivot = final_concat.groupby(['YEAR', 'MONTH', 'LE', 'PROJECT NAME'])[
            ['AMOUNT USD', 'VAT USD']].sum().reset_index()
        pivot = pivot.round(2)

        pivot_path = results_base / q_path.parent.name / q_path.name / "vat_quarter.csv"
        pivot_path.parent.mkdir(parents=True, exist_ok=True)
        pivot.to_csv(pivot_path, index=False)
        print(f"\n📊 ФІНАЛЬНИЙ ЗВІТ СФОРМОВАНО: {pivot_path}")


if __name__ == "__main__":
    main()