import pandas as pd
from nemosis import custom_errors, defaults, static_table

mock_tables = {
    'DISPATCHPRICE': pd.DataFrame(
        columns=["REGIONID", "SETTLEMENTDATE", "RRP", "INTERVENTION"],
        data=[('SA', '2020/01/01 01:00:00', 55.4, 0),
              ('SA', '2020/01/01 01:00:00', 65.4, 1),
              ('TAS', '2020/01/01 01:00:00', 75.4, 0),
              ('TAS', '2020/01/01 01:00:00', 85.4, 1)]
    ),
    'DISPATCHREGIONSUM': pd.DataFrame(
        columns=["REGIONID", "SETTLEMENTDATE", "TOTALDEMAND", "INTERVENTION"],
        data=[('SA', '2020/01/01 01:00:00', 1000.0, 0),
              ('SA', '2020/01/01 01:00:00', 1001.0, 1),
              ('TAS', '2020/01/01 01:00:00', 2000.0, 0),
              ('TAS', '2020/01/01 01:00:00', 2001.0, 1)]
    ),
    'DAILY_REGION_SUMMARY': pd.DataFrame(
        columns=["REGIONID", "SETTLEMENTDATE", "TOTALDEMAND", "RRP", "INTERVENTION"],
        data=[('SA', '2020/01/01 05:00:00', 1010.0, 80.1, 0),
              ('SA', '2020/01/01 05:00:00', 1011.0, 80.0, 1),
              ('TAS', '2020/01/01 05:00:00', 2010.0, 90.0, 0),
              ('TAS', '2020/01/01 05:00:00', 2011.0, 90.1, 1)]
    ),
    'DISPATCHLOAD': pd.DataFrame(
        columns=['SETTLEMENTDATE', 'INTERVENTION', 'DUID', 'AVAILABILITY', 'TOTALCLEARED', 'INITIALMW', 'RAMPDOWNRATE',
                 'RAMPUPRATE'],
        data=[('2020/01/01 01:00:00', 0, 'A', 100.0, 80.0, 70.0, 121.0, 125.0),
              ('2020/01/01 01:00:00', 1, 'A', 101.0, 81.0, 71.0, 122.0, 126.0),
              ('2020/01/01 01:00:00', 0, 'B', 200.0, 90.0, 60.0, 123.0, 127.0),
              ('2020/01/01 01:00:00', 1, 'B', 201.0, 91.0, 61.0, 124.0, 128.0)]
    ),
    'NEXT_DAY_DISPATCHLOAD': pd.DataFrame(
        columns=['SETTLEMENTDATE', 'INTERVENTION', 'DUID', 'AVAILABILITY', 'TOTALCLEARED', 'INITIALMW', 'RAMPDOWNRATE',
                 'RAMPUPRATE'],
        data=[('2020/01/01 05:00:00', 0, 'A', 102.0, 81.0, 71.0, 241.0, 245.0),
              ('2020/01/01 05:00:00', 1, 'A', 103.0, 82.0, 72.0, 242.0, 246.0),
              ('2020/01/01 05:00:00', 0, 'B', 202.0, 91.0, 61.0, 243.0, 247.0),
              ('2020/01/01 05:00:00', 1, 'B', 203.0, 92.0, 62.0, 244.0, 248.0)]
    )
}


def dynamic_data_compiler(start_time, end_time, table_name, raw_data_location,
                          select_columns=None, filter_cols=None,
                          filter_values=None, fformat='feather',
                          keep_csv=True, parse_data_types=True,
                          **kwargs):

    data = mock_tables[table_name]

    if 'SETTLEMENTDATE' in data.columns:
        data['SETTLEMENTDATE'] = pd.to_datetime(data['SETTLEMENTDATE'])

    if 'INTERVAL_DATETIME' in data.columns:
        data['INTERVAL_DATETIME'] = pd.to_datetime(data['INTERVAL_DATETIME'])

    return data

