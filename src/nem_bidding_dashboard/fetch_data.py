import time

import nemosis.custom_errors
import pandas as pd
from nemosis import defaults, dynamic_data_compiler, static_table

defaults.table_columns["BIDPEROFFER_D"] += ["PASAAVAILABILITY", "ROCDOWN", "ROCUP"]

pd.set_option('display.width', None)


def get_region_data(start_time, end_time, raw_data_cache):
    """
    Fetch electricity price and demand data using `NEMOSIS <https://github.com/UNSW-CEEM/NEMOSIS>`_. Attempts to
    pull data from AEMO monthly archive tables DISPATCHPRICE and DISPATCHREGIONSUM, if all the required data cannot
    be fetched from these tables then AEMO current table PUBLIC_DAILY is also queried. This should allow all historical
    AEMO data to fetched including data from the previous day.

    Examples:

    >>> get_region_data(
    ... '2020/01/01 00:00:00',
    ... '2020/01/01 00:05:00',
    ... 'D:/nemosis_data_cache')
      REGIONID      SETTLEMENTDATE  TOTALDEMAND       RRP
    0     NSW1 2020-01-01 00:05:00      7245.31  49.00916
    1     QLD1 2020-01-01 00:05:00      6095.75  50.81148
    2      SA1 2020-01-01 00:05:00      1466.53  68.00000
    3     TAS1 2020-01-01 00:05:00      1010.06  81.79115
    4     VIC1 2020-01-01 00:05:00      4267.32  65.67826

    Args:
       start_time: str formatted "DD/MM/YYYY HH:MM:SS", data with date times greater than start_time are returned
       end_time: str formatted identical to start_time, data with date times less than or equal to end_time are returned
       raw_data_cache: Filepath to directory for caching files downloaded from AEMO

    Returns: pandas dataframe with columns SETTLEMENTDATE, REGIONID, TOTALDEMAND (the operational demand AEMO dispatches
             generation to meet), and RRP (the regional reference price for energy).
    """
    try:
        price_data = dynamic_data_compiler(
            start_time,
            end_time,
            "DISPATCHPRICE",
            raw_data_cache,
            keep_csv=False,
            fformat="parquet",
            select_columns=["REGIONID", "SETTLEMENTDATE", "RRP", "INTERVENTION"],
        )
    except nemosis.custom_errors.NoDataToReturn:
        price_data = pd.DataFrame(
            columns=["REGIONID", "SETTLEMENTDATE", "RRP", "INTERVENTION"]
        )

    try:
        demand_data = dynamic_data_compiler(
            start_time,
            end_time,
            "DISPATCHREGIONSUM",
            raw_data_cache,
            keep_csv=False,
            fformat="parquet",
            select_columns=[
                "REGIONID",
                "SETTLEMENTDATE",
                "TOTALDEMAND",
                "INTERVENTION",
            ],
        )
    except nemosis.custom_errors.NoDataToReturn:
        demand_data = pd.DataFrame(
            columns=["REGIONID", "SETTLEMENTDATE", "TOTALDEMAND", "INTERVENTION"]
        )

    assert price_data.empty == demand_data.empty

    if not price_data.empty:
        assert price_data["SETTLEMENTDATE"].max() == demand_data["SETTLEMENTDATE"].max()
        assert price_data["SETTLEMENTDATE"].min() == demand_data["SETTLEMENTDATE"].min()

    price_and_demand_data = pd.merge(
        price_data, demand_data, on=["SETTLEMENTDATE", "REGIONID", "INTERVENTION"]
    )

    if (
        price_and_demand_data.empty
        or price_and_demand_data["SETTLEMENTDATE"].max().strftime("%Y/%m/%d %X")
        < end_time
    ):
        if not price_and_demand_data.empty:
            start_time = (
                price_and_demand_data["SETTLEMENTDATE"].max().strftime("%Y/%m/%d %X")
            )
        try:
            recent_price_and_demand_data = dynamic_data_compiler(
                start_time,
                end_time,
                "DAILY_REGION_SUMMARY",
                raw_data_cache,
                keep_csv=False,
                fformat="parquet",
                select_columns=[
                    "REGIONID",
                    "SETTLEMENTDATE",
                    "TOTALDEMAND",
                    "RRP",
                    "INTERVENTION",
                ],
            )
            price_and_demand_data = pd.concat(
                [price_and_demand_data, recent_price_and_demand_data]
            )
        except nemosis.custom_errors.NoDataToReturn:
            pass
    price_and_demand_data = price_and_demand_data.loc[
        price_and_demand_data["INTERVENTION"] == 0
    ]
    return price_and_demand_data.loc[
        :, ["REGIONID", "SETTLEMENTDATE", "TOTALDEMAND", "RRP"]
    ]


def get_duid_availability_data(start_time, end_time, raw_data_cache):
    """
    Fetch unit availability and other dispatch values using `NEMOSIS <https://github.com/UNSW-CEEM/NEMOSIS>`_.
    Attempts to pull data from AEMO monthly archive table DISPATCHLOAD, if all the required data cannot be fetched from
    this table then the AEMO current table PUBLIC_NEXT_DAY_DISPATCH is also queried. This should allow all historical
    AEMO data to fetched including data from the previous day. Data is filtered for INTERVENTION values equal to 1 where
    an intervention dispatch run is present, such that the values returned are those assocaited with the dispatch run
    used to set unit dispatch targets.

    Examples:

    >>> get_duid_availability_data(
    ... '2020/01/01 00:00:00',
    ... '2020/01/01 00:05:00',
    ... 'D:/nemosis_data_cache')
             SETTLEMENTDATE     DUID  AVAILABILITY  TOTALCLEARED  INITIALMW  RAMPDOWNRATE  RAMPUPRATE
    0   2020-01-01 00:05:00   AGLHAL       181.000         0.000    0.00000        720.00      720.00
    1   2020-01-01 00:05:00   AGLSOM       140.000         0.000    0.00000        480.00      480.00
    2   2020-01-01 00:05:00  ANGAST1        44.000         0.000    0.00000        840.00      840.00
    3   2020-01-01 00:05:00    APD01         0.000         0.000    0.00000          0.00        0.00
    4   2020-01-01 00:05:00    ARWF1        29.745        29.745   30.40000        600.00     1200.00
    ..                  ...      ...           ...           ...        ...           ...         ...
    305 2020-01-01 00:05:00  YENDWF1         6.105         6.105    6.46000       1680.00     1680.00
    306 2020-01-01 00:05:00    YWPS1       330.000       330.000  329.32153        180.00      180.00
    307 2020-01-01 00:05:00    YWPS2       340.000       340.000  341.60254        176.62      176.62
    308 2020-01-01 00:05:00    YWPS3       365.000       365.000  365.59872        177.75      177.75
    309 2020-01-01 00:05:00    YWPS4       365.000       365.000  356.28354        180.00      180.00
    <BLANKLINE>
    [310 rows x 7 columns]

    Args:
       start_time: str formatted "DD/MM/YYYY HH:MM:SS", data with date times greater than start_time are returned
       end_time: str formatted identical to start_time, data with date times less than or equal to end_time are returned
       raw_data_cache: Filepath to directory for caching files downloaded from AEMO

    Returns:
        pandas dataframe with columns INTERVAL_DATETIME, DUID, AVAILABILITY, TOTALCLEARED, INITIALMW,
        RAMPDOWNRATE, RAMPUPRATE
    """
    try:
        availability_data = dynamic_data_compiler(
            start_time,
            end_time,
            "DISPATCHLOAD",
            raw_data_cache,
            keep_csv=False,
            fformat="parquet",
            select_columns=[
                "SETTLEMENTDATE",
                "INTERVENTION",
                "DUID",
                "AVAILABILITY",
                "TOTALCLEARED",
                "INITIALMW",
                "RAMPDOWNRATE",
                "RAMPUPRATE",
            ],
        )
    except nemosis.custom_errors.NoDataToReturn:
        availability_data = pd.DataFrame(
            columns=[
                "SETTLEMENTDATE",
                "INTERVENTION",
                "DUID",
                "AVAILABILITY",
                "TOTALCLEARED",
                "INITIALMW",
                "RAMPDOWNRATE",
                "RAMPUPRATE",
            ]
        )

    if (
        availability_data.empty
        or availability_data["SETTLEMENTDATE"].max().strftime("%Y/%m/%d %X") != end_time
    ):
        if not availability_data.empty:
            start_time = (
                availability_data["SETTLEMENTDATE"].max().strftime("%Y/%m/%d %X")
            )
        try:
            recent_availability_data = dynamic_data_compiler(
                start_time,
                end_time,
                "NEXT_DAY_DISPATCHLOAD",
                raw_data_cache,
                keep_csv=False,
                fformat="parquet",
                select_columns=[
                    "SETTLEMENTDATE",
                    "INTERVENTION",
                    "DUID",
                    "AVAILABILITY",
                    "TOTALCLEARED",
                    "INITIALMW",
                    "RAMPDOWNRATE",
                    "RAMPUPRATE",
                ],
            )
            availability_data = pd.concat([availability_data, recent_availability_data])
        except nemosis.custom_errors.NoDataToReturn:
            pass
    availability_data = availability_data.sort_values(["SETTLEMENTDATE", "INTERVENTION"])
    availability_data = availability_data.drop_duplicates(keep='last', subset=['SETTLEMENTDATE', 'DUID'])
    return availability_data.loc[
        :,
        [
            "SETTLEMENTDATE",
            "DUID",
            "AVAILABILITY",
            "TOTALCLEARED",
            "INITIALMW",
            "RAMPDOWNRATE",
            "RAMPUPRATE",
        ],
    ]


def get_duid_data(raw_data_cache):
    """
    Fetch unit data using `NEMOSIS <https://github.com/UNSW-CEEM/NEMOSIS>`_. Data is sourced from AEMO's
    NEM Registration and Exemption List workbook (Generators and Scheduled Loads tab). This only includes currently
    registered generator, so if historical analysis is being conducted care needs to taken that data for some generators
    is not missing.

    Examples:

    >>> get_duid_data('D:/nemosis_data_cache')
                           STATION NAME REGION DISPATCH TYPE FUEL SOURCE - DESCRIPTOR       TECHNOLOGY TYPE - DESCRIPTOR      DUID
    0       Adelaide Desalination Plant    SA1     Generator                     Grid               Battery and Inverter   ADPBA1G
    1       Adelaide Desalination Plant    SA1          Load                     Grid               Battery and Inverter   ADPBA1L
    4       Adelaide Desalination Plant    SA1     Generator                    Water                       Run of River    ADPMH1
    5       Adelaide Desalination Plant    SA1     Generator                    Solar            Photovoltaic Flat panel    ADPPV3
    6       Adelaide Desalination Plant    SA1     Generator                    Solar            Photovoltaic Flat panel    ADPPV2
    ..                              ...    ...           ...                      ...                                ...       ...
    578  Yarrawonga Hydro Power Station   VIC1     Generator                    Water                    Hydro - Gravity  YWNGAHYD
    579            Yarwun Power Station   QLD1     Generator              Natural Gas  Combined Cycle Gas Turbine (CCGT)  YARWUN_1
    580              Yatpool Solar Farm   VIC1     Generator                    Solar   Photovoltaic Tracking Flat panel    YATSF1
    581                Yawong Wind Farm   VIC1     Generator                     Wind                     Wind - Onshore    YAWWF1
    582                Yendon Wind Farm   VIC1     Generator                     Wind                     Wind - Onshore   YENDWF1
    <BLANKLINE>
    [490 rows x 6 columns]

    Args:
        raw_data_cache: Filepath to directory for caching files downloaded from AEMO

    Returns:
        pandas dataframe with columns DUID, REGIONID, "FUEL SOURCE - DESCRIPTOR", "DISPATCH TYPE",
        "TECHNOLOGY TYPE - DESCRIPTOR", "STATION NAME"

    """
    duid_data = static_table(
        "Generators and Scheduled Loads",
        raw_data_cache,
        select_columns=[
            "DUID",
            "Region",
            "Fuel Source - Descriptor",
            "Dispatch Type",
            "Technology Type - Descriptor",
            "Station Name",
        ],
    )
    duid_data = duid_data[~duid_data["DUID"].isin(["BLNKVIC", "BLNKTAS"])]
    duid_data.columns = duid_data.columns.str.upper()
    return duid_data


def get_volume_bids(start_time, end_time, raw_data_cache):
    """
    Fetch unit volume bid data using `NEMOSIS <https://github.com/UNSW-CEEM/NEMOSIS>`_. Data source from AEMO monthly
    archive tables BIDPEROFFER_D and current tables BIDMOVE_COMPETE. This should allow all historical AEMO
    data to fetched including data from the previous day.

    Examples:

    >>> get_volume_bids(
    ... '2020/01/01 00:00:00',
    ... '2020/01/01 01:00:00',
    ... 'D:/nemosis_data_cache')
            SETTLEMENTDATE      DUID     BIDTYPE  MAXAVAIL  ROCUP  ROCDOWN  BANDAVAIL1  BANDAVAIL2  BANDAVAIL3  BANDAVAIL4  BANDAVAIL5  BANDAVAIL6  BANDAVAIL7  BANDAVAIL8  BANDAVAIL9  BANDAVAIL10  PASAAVAILABILITY   INTERVAL_DATETIME
    9547382     2019-12-31    AGLHAL      ENERGY       181   12.0     12.0           0           0           0           0           0           0          60           0           0          195             181.0 2020-01-01 00:05:00
    9547383     2019-12-31    AGLSOM      ENERGY       140    8.0      8.0           0           0           0           0           0           0         170           0           0            0             140.0 2020-01-01 00:05:00
    9547384     2019-12-31   ANGAST1      ENERGY        44   14.0     14.0           0           0           0           0           0          50           0           0           0           50              44.0 2020-01-01 00:05:00
    9547385     2019-12-31     APD01   LOWER5MIN         0    0.0      0.0           0           0           0           0           0           0           0           0           0          300               0.0 2020-01-01 00:05:00
    9547386     2019-12-31     APD01  LOWER60SEC         0    0.0      0.0           0           0           0           0           0           0           0           0           0          300               NaN 2020-01-01 00:05:00
    ...                ...       ...         ...       ...    ...      ...         ...         ...         ...         ...         ...         ...         ...         ...         ...          ...               ...                 ...
    9563967     2019-12-31  VSSEL1V1  LOWER60SEC         2    0.0      0.0           2           0           0           0           0           0           0           0           0            0               0.0 2020-01-01 01:00:00
    9563970     2019-12-31  VSSEL1V1   LOWER6SEC         2    0.0      0.0           2           0           0           0           0           0           0           0           0            0               0.0 2020-01-01 01:00:00
    9563971     2019-12-31  VSSEL1V1   RAISE5MIN         2    0.0      0.0           2           0           0           0           0           0           0           0           0            0               0.0 2020-01-01 01:00:00
    9563974     2019-12-31  VSSEL1V1  RAISE60SEC         2    0.0      0.0           2           0           0           0           0           0           0           0           0            0               0.0 2020-01-01 01:00:00
    9563975     2019-12-31  VSSEL1V1   RAISE6SEC         2    0.0      0.0           2           0           0           0           0           0           0           0           0            0               0.0 2020-01-01 01:00:00
    <BLANKLINE>
    [12912 rows x 18 columns]

    Args:
       start_time: str formatted "DD/MM/YYYY HH:MM:SS", data with date times greater than start_time are returned
       end_time: str formatted identical to start_time, data with date times less than or equal to end_time are returned
       raw_data_cache: Filepath to directory for caching files downloaded from AEMO

    Returns:
        pandas dataframe with columns INTERVAL_DATETIME, SETTLEMENTDATE, DUID, BIDTYPE, BANDAVAIL1,
        BANDAVAIL2, BANDAVAIL3, BANDAVAIL4, BANDAVAIL5, BANDAVAIL6, BANDAVAIL7, BANDAVAIL8,
        BANDAVAIL9, BANDAVAIL10, MAXAVAIL, ROCUP, ROCDOWN, PASAAVAILABILITY
    """
    volume_bids = dynamic_data_compiler(
        start_time=start_time,
        end_time=end_time,
        table_name="BIDPEROFFER_D",
        raw_data_location=raw_data_cache,
        keep_csv=False,
        fformat="parquet",
        select_columns=[
            "INTERVAL_DATETIME",
            "SETTLEMENTDATE",
            "DUID",
            "BIDTYPE",
            "BANDAVAIL1",
            "BANDAVAIL2",
            "BANDAVAIL3",
            "BANDAVAIL4",
            "BANDAVAIL5",
            "BANDAVAIL6",
            "BANDAVAIL7",
            "BANDAVAIL8",
            "BANDAVAIL9",
            "BANDAVAIL10",
            "MAXAVAIL",
            "ROCUP",
            "ROCDOWN",
            "PASAAVAILABILITY",
        ],
    )
    return volume_bids


def get_price_bids(start_time: str, end_time: str, raw_data_cache: str):
    """
    Fetch unit price bid data using `NEMOSIS <https://github.com/UNSW-CEEM/NEMOSIS>`_. Data source from AEMO monthly
    archive tables BIDPDAYOFFER_D and current tables BIDMOVE_COMPETE. This should allow all historical AEMO
    data to fetched including data from the previous day.

    Examples:

    >>> get_price_bids(
    ... '2020/01/01 00:00:00',
    ... '2020/01/01 00:05:00',
    ... 'D:/nemosis_data_cache')
          SETTLEMENTDATE      DUID     BIDTYPE  PRICEBAND1  PRICEBAND2  PRICEBAND3  PRICEBAND4  PRICEBAND5  PRICEBAND6  PRICEBAND7  PRICEBAND8  PRICEBAND9  PRICEBAND10
    3936      2019-12-31    ASSEL1   LOWER5MIN        0.00        1.00        2.00        3.00        4.00        5.00        6.00        8.00       10.00     13000.00
    3937      2019-12-31    BALBL1      ENERGY      -48.06       16.60       52.20       65.11       72.81       82.70       92.53      116.11      297.18       942.29
    3938      2019-12-31    BNGSF2      ENERGY     -971.70        1.94        2.92        3.89        4.86        5.83        6.80        7.77        8.75         9.72
    3939      2019-12-31  BRAEMAR2   LOWER5MIN        0.00        1.00        2.00        4.00        8.00       16.00       32.00       64.00      128.00       256.00
    3940      2019-12-31  BRAEMAR2    LOWERREG        0.00        1.00        2.00        4.00        8.00       16.00       32.00       64.00      128.00       256.00
    ...              ...       ...         ...         ...         ...         ...         ...         ...         ...         ...         ...         ...          ...
    33328     2019-12-31   POAT110  LOWER60SEC        0.01        0.03        0.05        0.16        0.44        1.70       48.00      400.00     4300.00     14500.00
    33329     2019-12-31   POAT110    RAISEREG        0.01        8.00       17.50       33.00       40.00       90.00      250.00      500.00     1000.00     14500.00
    33330     2019-12-31   POAT220    LOWERREG        0.01        3.80        8.50       14.00       23.00       33.00      101.00      930.00     4300.00     14500.00
    33331     2019-12-31    REECE2   LOWER6SEC        0.01        0.02        0.03        0.04        0.05        0.90       48.00      400.00     4300.00     14500.00
    33332     2019-12-31   TRIBUTE    RAISEREG        0.01        8.00       17.50       33.00       40.00       90.00      250.00      500.00     1000.00     14500.00
    <BLANKLINE>
    [1076 rows x 13 columns]

    Args:
       start_time: str formatted "DD/MM/YYYY HH:MM:SS", data with date times greater than start_time are returned
       end_time: str formatted identical to start_time, data with date times less than or equal to end_time are returned
       raw_data_cache: Filepath to directory for caching files downloaded from AEMO

    Returns:
        pandas dataframe with columns INTERVAL_DATETIME, SETTLEMENTDATE, DUID, BIDTYPE, PRICEBAND1,
        PRICEBAND2, PRICEBAND3, PRICEBAND4, PRICEBAND5, PRICEBAND6, PRICEBAND7, PRICEBAND8,
        PRICEBAND9, PRICEBAND10
    """
    volume_bids = dynamic_data_compiler(
        start_time=start_time,
        end_time=end_time,
        table_name="BIDDAYOFFER_D",
        raw_data_location=raw_data_cache,
        keep_csv=False,
        fformat="parquet",
        select_columns=[
            "SETTLEMENTDATE",
            "DUID",
            "BIDTYPE",
            "PRICEBAND1",
            "PRICEBAND2",
            "PRICEBAND3",
            "PRICEBAND4",
            "PRICEBAND5",
            "PRICEBAND6",
            "PRICEBAND7",
            "PRICEBAND8",
            "PRICEBAND9",
            "PRICEBAND10",
        ],
    )
    return volume_bids


if __name__ == "__main__":
    raw_data_cache = "D:/nemosis_cache"
    # duid_data = get_duid_data(raw_data_cache)
    # region_data = get_region_data(
    #     "2022/11/01 00:00:00", "2022/11/02 00:00:00", raw_data_cache
    # )
    # print(duid_data)
    # x = 1
    t0 = time.time()
    get_volume_bids("2020/01/23 00:00:00", "2020/01/24 00:00:00", raw_data_cache)
    print(t0 - time.time())
