import shutil
from os import listdir, path

import pandas as pd

temp_directory = 'data/temp/'


def write_permanent_vehicle_positions(file_path: str, df: pd.DataFrame):
    sorted_df = df.sort_values(
        by=[
            'Transportas',
            'Marsrutas',
            'ReisoID',
            'MasinosNumeris',
            'MasinosTipas',
            'MatavimoLaikas',
            'Gauta',
        ],
    )
    sorted_df.to_parquet(file_path, index=False)

    print(sorted_df.info())


def combine_vehicle_position_files(file_name: str, file_path: str):
    combined_df = new_df = pd.read_parquet(file_path)
    permanent_file_path = f'data/vehicle_positions/vilnius/{file_name}'

    if path.exists(permanent_file_path):
        previous_df = pd.read_parquet(file_path)
        combined_df = pd.concat([previous_df, new_df], axis=0) \
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

    write_permanent_vehicle_positions(permanent_file_path, combined_df)


def combine_vehicle_position_files_in_directory():
    for filename in listdir(temp_directory):
        file_path = path.join(temp_directory, filename)

        if path.isfile(file_path):
            combine_vehicle_position_files(filename, file_path)

    shutil.rmtree(temp_directory)


if __name__ == '__main__':
    combine_vehicle_position_files_in_directory()
