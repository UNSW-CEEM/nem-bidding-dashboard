import os

import pandas as pd
from defaults import test_cache


def down_size_parquet_files(cache_location, file_name, start_time, end_time, time_col):
    file_path = os.path.join(cache_location, file_name)
    data = pd.read_parquet(file_path)
    data = data[data[time_col] <= end_time].copy()
    data = data[data[time_col] > start_time].copy()
    data.to_parquet(file_path)


if __name__ == "__main__":
    files_to_down_size = [
        "PUBLIC_DVD_DISPATCHLOAD_202201010000.parquet",
        "PUBLIC_DVD_DISPATCHREGIONSUM_202201010000.parquet",
        "PUBLIC_DVD_BIDPEROFFER_D_20220101.parquet",
        "PUBLIC_DVD_DISPATCHPRICE_202201010000.parquet",
        "PUBLIC_DVD_BIDPEROFFER_D_20211231.parquet",
    ]
    for file in files_to_down_size:
        if "BIDPEROFFER" in file:
            time_col = "INTERVAL_DATETIME"
        else:
            time_col = "SETTLEMENTDATE"
        down_size_parquet_files(
            test_cache, file, "2022/01/01 00:00:00", "2022/01/01 04:00:00", time_col
        )
