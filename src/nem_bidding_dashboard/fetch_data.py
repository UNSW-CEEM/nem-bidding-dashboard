import time

import nemosis.custom_errors
import pandas as pd
from nemosis import defaults, dynamic_data_compiler, static_table

from nem_bidding_dashboard.input_validation import validate_start_end_and_cache_location

defaults.table_columns["BIDPEROFFER_D"] += ["PASAAVAILABILITY", "ROCDOWN", "ROCUP"]

pd.set_option("display.width", None)


def region_data(start_time, end_time, raw_data_cache):
    """
    Fetch electricity price and demand data using `NEMOSIS <https://github.com/UNSW-CEEM/NEMOSIS>`_. Attempts to
    pull data from AEMO monthly archive tables DISPATCHPRICE and DISPATCHREGIONSUM, if all the required data cannot
    be fetched from these tables then AEMO current table PUBLIC_DAILY is also queried. This should allow all historical
    AEMO data to fetched including data from the previous day.

    Examples:

    >>> region_data(
    ... '2022/01/01 00:00:00',
    ... '2022/01/01 00:05:00',
    ... 'D:/nemosis_data_cache')
      REGIONID      SETTLEMENTDATE  TOTALDEMAND        RRP
    0     NSW1 2022-01-01 00:05:00      7206.03  124.85631
    1     QLD1 2022-01-01 00:05:00      5982.85  118.73008
    2      SA1 2022-01-01 00:05:00      1728.03  133.94970
    3     TAS1 2022-01-01 00:05:00      1148.93   40.34000
    4     VIC1 2022-01-01 00:05:00      5005.34  114.80312

    Args:
       start_time: str formatted "DD/MM/YYYY HH:MM:SS", data with date times greater than start_time are returned
       end_time: str formatted identical to start_time, data with date times less than or equal to end_time are returned
       raw_data_cache: Filepath to directory for caching files downloaded from AEMO

    Returns: pandas dataframe with columns SETTLEMENTDATE, REGIONID, TOTALDEMAND (the operational demand AEMO dispatches
             generation to meet), and RRP (the regional reference price for energy).
    """
    validate_start_end_and_cache_location(start_time, end_time, raw_data_cache)
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
    price_and_demand_data = price_and_demand_data.loc[
        :, ["SETTLEMENTDATE", "REGIONID", "TOTALDEMAND", "RRP"]
    ]
    return price_and_demand_data


def duid_availability_data(start_time, end_time, raw_data_cache):
    """
    Fetch unit availability and other dispatch values using `NEMOSIS <https://github.com/UNSW-CEEM/NEMOSIS>`_.
    Attempts to pull data from AEMO monthly archive table DISPATCHLOAD, if all the required data cannot be fetched from
    this table then the AEMO current table PUBLIC_NEXT_DAY_DISPATCH is also queried. This should allow all historical
    AEMO data to fetched including data from the previous day. Data is filtered for INTERVENTION values equal to 1 where
    an intervention dispatch run is present, such that the values returned are those assocaited with the dispatch run
    used to set unit dispatch targets.

    Examples:

    >>> duid_availability_data(
    ... '2022/01/01 00:00:00',
    ... '2022/01/01 00:05:00',
    ... 'D:/nemosis_data_cache')
             SETTLEMENTDATE     DUID  AVAILABILITY  TOTALCLEARED  INITIALMW  RAMPDOWNRATE  RAMPUPRATE
    0   2022-01-01 00:05:00  ADPBA1G         0.000         0.000    0.00000         93.00       93.00
    1   2022-01-01 00:05:00  ADPBA1L         0.000         0.000    0.00000         93.00       93.00
    2   2022-01-01 00:05:00   ADPPV1         0.000         0.000    0.00000        120.00      120.00
    3   2022-01-01 00:05:00   AGLHAL       153.000         0.000   -0.00763        720.00      720.00
    4   2022-01-01 00:05:00   AGLSOM       160.000         0.000    0.00000        480.00      480.00
    ..                  ...      ...           ...           ...        ...           ...         ...
    385 2022-01-01 00:05:00  YENDWF1        17.201        17.201   16.66000       1680.00     1680.00
    386 2022-01-01 00:05:00    YWPS1       380.000       380.000  376.69113        178.88      178.88
    387 2022-01-01 00:05:00    YWPS2         0.000         0.000    0.00000        180.00      180.00
    388 2022-01-01 00:05:00    YWPS3       350.000       350.000  348.94431        180.00      180.00
    389 2022-01-01 00:05:00    YWPS4       370.000       370.000  368.98608        180.00      180.00
    <BLANKLINE>
    [390 rows x 7 columns]

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
        or availability_data["SETTLEMENTDATE"].max().strftime("%Y/%m/%d %X") < end_time
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
    availability_data = availability_data.sort_values(
        ["SETTLEMENTDATE", "INTERVENTION"]
    )
    availability_data = availability_data.drop_duplicates(
        keep="last", subset=["SETTLEMENTDATE", "DUID"]
    )
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


def duid_data(raw_data_cache):
    """
    Fetch unit data using `NEMOSIS <https://github.com/UNSW-CEEM/NEMOSIS>`_. Data is sourced from AEMO's
    NEM Registration and Exemption List workbook (Generators and Scheduled Loads tab). This only includes currently
    registered generator, so if historical analysis is being conducted care needs to taken that data for some generators
    is not missing.

    Examples:

    >>> duid_data('D:/nemosis_data_cache')
                           STATION NAME REGION DISPATCH TYPE FUEL SOURCE - DESCRIPTOR       TECHNOLOGY TYPE - DESCRIPTOR      DUID
    0       Adelaide Desalination Plant    SA1     Generator                     Grid               Battery and Inverter   ADPBA1G
    1       Adelaide Desalination Plant    SA1          Load                     Grid               Battery and Inverter   ADPBA1L
    4       Adelaide Desalination Plant    SA1     Generator                    Water                       Run of River    ADPMH1
    5       Adelaide Desalination Plant    SA1     Generator                    Solar            Photovoltaic Flat panel    ADPPV3
    6       Adelaide Desalination Plant    SA1     Generator                    Solar            Photovoltaic Flat panel    ADPPV2
    ..                              ...    ...           ...                      ...                                ...       ...
    576  Yarrawonga Hydro Power Station   VIC1     Generator                    Water                    Hydro - Gravity  YWNGAHYD
    577            Yarwun Power Station   QLD1     Generator              Natural Gas  Combined Cycle Gas Turbine (CCGT)  YARWUN_1
    578              Yatpool Solar Farm   VIC1     Generator                    Solar   Photovoltaic Tracking Flat panel    YATSF1
    579                Yawong Wind Farm   VIC1     Generator                     Wind                     Wind - Onshore    YAWWF1
    580                Yendon Wind Farm   VIC1     Generator                     Wind                     Wind - Onshore   YENDWF1
    <BLANKLINE>
    [488 rows x 6 columns]


    Args:
        raw_data_cache: Filepath to directory for caching files downloaded from AEMO

    Returns:
        pandas dataframe with columns DUID, REGIONID, "FUEL SOURCE - DESCRIPTOR", "DISPATCH TYPE",
        "TECHNOLOGY TYPE - DESCRIPTOR", "STATION NAME"

    """
    duid_data = static_table(
        "Generators and Scheduled Loads",
        raw_data_cache,
        update_static_file=False,
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


def volume_bids(start_time, end_time, raw_data_cache):
    """
    Fetch unit volume bid data using `NEMOSIS <https://github.com/UNSW-CEEM/NEMOSIS>`_. Data source from AEMO monthly
    archive tables BIDPEROFFER_D and current tables BIDMOVE_COMPETE. This should allow all historical AEMO
    data to fetched including data from the previous day.

    Examples:

    >>> volume_bids(
    ... '2022/01/01 00:00:00',
    ... '2022/01/01 01:00:00',
    ... 'D:/nemosis_data_cache')
           SETTLEMENTDATE     DUID     BIDTYPE  MAXAVAIL  ROCUP  ROCDOWN  BANDAVAIL1  BANDAVAIL2  BANDAVAIL3  BANDAVAIL4  BANDAVAIL5  BANDAVAIL6  BANDAVAIL7  BANDAVAIL8  BANDAVAIL9  BANDAVAIL10  PASAAVAILABILITY   INTERVAL_DATETIME
    309360     2021-12-31  ADPBA1G      ENERGY         0    2.0      2.0           0           0           0           0           0           0           0           6           0            0               6.0 2022-01-01 00:05:00
    309361     2021-12-31  ADPBA1G    LOWERREG         0    NaN      NaN           0           0           0           0           0           0           0           6           0            0               NaN 2022-01-01 00:05:00
    309362     2021-12-31  ADPBA1G   RAISE5MIN         0    NaN      NaN           0           0           0           0           0           0           0           0           0            2               NaN 2022-01-01 00:05:00
    309363     2021-12-31  ADPBA1G  RAISE60SEC         0    NaN      NaN           0           0           0           0           0           0           0           0           0            2               NaN 2022-01-01 00:05:00
    309364     2021-12-31  ADPBA1G   RAISE6SEC         0    NaN      NaN           0           0           0           0           0           0           0           0           0            2               NaN 2022-01-01 00:05:00
    ...               ...      ...         ...       ...    ...      ...         ...         ...         ...         ...         ...         ...         ...         ...         ...          ...               ...                 ...
    324823     2021-12-31    YWPS4    LOWERREG        20    NaN      NaN           0           0           0           0           0           0           0          20           0            0               NaN 2022-01-01 01:00:00
    324824     2021-12-31    YWPS4   RAISE5MIN         0    NaN      NaN           0           0           0           5           5           0           0           0           0           10               NaN 2022-01-01 01:00:00
    324825     2021-12-31    YWPS4  RAISE60SEC         5    NaN      NaN           0           0           0           0           0           0           5          10           0            5               NaN 2022-01-01 01:00:00
    324826     2021-12-31    YWPS4   RAISE6SEC         5    NaN      NaN           0           0           0           0           0           5          10           0           0           10               NaN 2022-01-01 01:00:00
    324827     2021-12-31    YWPS4    RAISEREG        15    NaN      NaN           0           0           0           0           0           0           5          10           0            5               NaN 2022-01-01 01:00:00
    <BLANKLINE>
    [15468 rows x 18 columns]

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


def price_bids(start_time: str, end_time: str, raw_data_cache: str):
    """
    Fetch unit price bid data using `NEMOSIS <https://github.com/UNSW-CEEM/NEMOSIS>`_. Data source from AEMO monthly
    archive tables BIDPDAYOFFER_D and current tables BIDMOVE_COMPETE. This should allow all historical AEMO
    data to fetched including data from the previous day.

    Examples:

    >>> price_bids(
    ... '2022/01/01 00:00:00',
    ... '2022/01/01 00:05:00',
    ... 'D:/nemosis_data_cache')
         SETTLEMENTDATE     DUID     BIDTYPE  PRICEBAND1  PRICEBAND2  PRICEBAND3  PRICEBAND4  PRICEBAND5  PRICEBAND6  PRICEBAND7  PRICEBAND8  PRICEBAND9  PRICEBAND10
    0        2021-12-31  ADPBA1L    RAISEREG        5.00        8.00       12.00       18.00       24.00       47.00       98.00      268.00       498.0     12000.00
    1        2021-12-31    ARWF1      ENERGY     -898.70     -224.68     -179.74     -157.27     -145.59     -125.82      -44.94      224.68      2696.1     13570.37
    2        2021-12-31  ASNENC1   RAISE6SEC        0.03        0.30        0.73        0.99        1.98        5.00        9.90       17.70       100.0     10000.00
    3        2021-12-31   ASSEL1  LOWER60SEC        1.00        2.00        3.00        4.00        5.00        6.00        7.00        8.00         9.0     13000.00
    4        2021-12-31  ASSENC1  RAISE60SEC        0.00        1.00        2.00        4.00        8.00       16.00       32.00       64.00       128.0       256.00
    ...             ...      ...         ...         ...         ...         ...         ...         ...         ...         ...         ...         ...          ...
    1284     2021-12-31    YWPS3   LOWER5MIN        0.08        0.17        0.79        1.19        4.40        9.99       29.99       99.99       249.9      8999.99
    1285     2021-12-31    YWPS3   RAISE6SEC        0.48        1.75        4.90       20.70       33.33       99.90      630.00     1999.00      6000.0     12299.00
    1286     2021-12-31    YWPS4   LOWER6SEC        0.03        0.05        0.16        0.30        1.90       25.04       30.04       99.00      4600.0      9899.00
    1287     2021-12-31    YWPS4    LOWERREG        0.05        1.90        4.78        9.40       14.00       29.00       64.90      240.90     11990.0     14600.00
    1288     2021-12-31    YWPS4  RAISE60SEC        0.17        1.80        4.80       10.01       21.00       39.00       52.00      102.00      4400.0     11999.00
    <BLANKLINE>
    [1289 rows x 13 columns]

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
    volume_bids("2020/01/23 00:00:00", "2020/01/24 00:00:00", raw_data_cache)
    print(t0 - time.time())
