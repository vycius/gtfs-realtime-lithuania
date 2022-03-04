import os
import time
from datetime import datetime, timedelta

import pandas as pd


def fetch_vehicle_positions_df() -> pd.DataFrame:
    df = pd.read_csv(
        'http://www.stops.lt/vilnius/gps_full.txt',
        engine='pyarrow',
        usecols=[
            'Transportas',
            'Marsrutas',
            'ReisoID',
            'MasinosNumeris',
            'Ilguma',
            'Platuma',
            'Greitis',
            'Azimutas',
            'ReisoPradziaMinutemis',
            'NuokrypisSekundemis',
            'MatavimoLaikas',
            'MasinosTipas',
        ],
        dtype={
            'Transportas': pd.CategoricalDtype(),
            'Marsrutas': pd.StringDtype(),
            'MasinosNumeris': pd.StringDtype(),
            'ReisoID': pd.Int64Dtype(),
            'NuokrypisSekundemis': pd.Int64Dtype(),
            'ReisoPradziaMinutemis': pd.Int64Dtype(),
            'MasinosTipas': pd.CategoricalDtype(),
        }
    )

    df['Platuma'] = df['Platuma'] / 1000000
    df['Ilguma'] = df['Ilguma'] / 1000000

    return df


def parse_and_listen_for_vehicle_positions(started_at: datetime) -> pd.DataFrame:
    df_unique_vehicle_positions = None

    while datetime.now() - started_at < timedelta(hours=1, minutes=10):
        fetched_at = int(datetime.timestamp(datetime.now()))
        df_new_vehicle_positions = fetch_vehicle_positions_df()

        df_new_vehicle_positions['Gauta'] = fetched_at

        if df_unique_vehicle_positions is None:
            df_unique_vehicle_positions = df_new_vehicle_positions

        df_unique_vehicle_positions = pd.concat([df_unique_vehicle_positions, df_new_vehicle_positions]) \
            .drop_duplicates(
            subset=[
                'Transportas',
                'Marsrutas',
                'ReisoID',
                'MasinosNumeris',
                'MasinosTipas',
                'MatavimoLaikas',
            ]
        )

        time.sleep(3)

    return df_unique_vehicle_positions


def fetch_vehicle_positions():
    started_at = datetime.now()
    df_vehicle_positions = parse_and_listen_for_vehicle_positions(started_at)

    file_date = (started_at - timedelta(hours=3, minutes=59)).date().isoformat()

    os.makedirs('data/temp', exist_ok=True)
    df_vehicle_positions.to_parquet(f'data/temp/{file_date}.parquet')


if __name__ == '__main__':
    fetch_vehicle_positions()
