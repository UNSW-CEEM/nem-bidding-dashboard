import pandas as pd

from nem_bidding_dashboard import fetch_data, preprocessing
from nem_bidding_dashboard.input_validation import (
    data_cache_exits,
    validate_start_end_and_cache_location,
)

pd.set_option("display.width", None)


def region_data(start_time, end_time, raw_data_cache):
    """
    Wrapper for fetching and preprocessing regional demand and price data. Calls
    :py:func:`nem_bidding_dashboard.fetch_data.get_region_data` to get regional data, re-formats REGIONID by removing
    the trailing '1' using :py:func:`nem_bidding_dashboard.preprocessing.remove_number_from_region_names`, and finally
    converting datetime columns to string format. Used for preparing data to load into dashboard backend PostgresSQL
    database, or can be used for compiling regional data directly if the user does not have database, but compling data
    using this function will be considerably slower than from a PostgresSQL database.

    Examples:

    >>> region_data(
    ... '2022/01/01 00:00:00',
    ... '2022/01/01 00:05:00',
    ... 'D:/nemosis_data_cache')
      REGIONID       SETTLEMENTDATE  TOTALDEMAND        RRP
    0      NSW  2022-01-01 00:05:00      7206.03  124.85631
    1      QLD  2022-01-01 00:05:00      5982.85  118.73008
    2       SA  2022-01-01 00:05:00      1728.03  133.94970
    3      TAS  2022-01-01 00:05:00      1148.93   40.34000
    4      VIC  2022-01-01 00:05:00      5005.34  114.80312

    Args:
       start_time: str formatted "DD/MM/YYYY HH:MM:SS", data with date times greater than start_time are returned
       end_time: str formatted identical to start_time, data with date times less than or equal to end_time are returned
       raw_data_cache: Filepath to directory for caching files downloaded from AEMO

    Returns:
        pandas dataframe with columns SETTLEMENTDATE, REGIONID, TOTALDEMAND (the operational demand AEMO dispatches
        generation to meet), and RRP (the regional reference price for energy).
    """
    validate_start_end_and_cache_location(start_time, end_time, raw_data_cache)
    regional_data = fetch_data.region_data(start_time, end_time, raw_data_cache)
    regional_data = preprocessing.remove_number_from_region_names(
        "REGIONID", regional_data
    )
    regional_data["SETTLEMENTDATE"] = regional_data["SETTLEMENTDATE"].dt.strftime(
        "%Y-%m-%d %X"
    )
    regional_data = regional_data.sort_values("SETTLEMENTDATE")
    return regional_data


def bid_data(start_time, end_time, raw_data_cache):
    """
    Wrapper for fetching and preprocessing bid data.

    - Calls :py:func:`nem_bidding_dashboard.fetch_data.get_volume_bids`,
      :py:func:`nem_bidding_dashboard.fetch_data.get_price_bids`, and
      :py:func:`nem_bidding_dashboard.fetch_data.get_duid_availability_data` to get raw bidding and availabilty data
    - Volume and price bids are filtered to get only bids for the energy spot market
    - Volume and price bids are combined using :py:func:`nem_bidding_dashboard.preprocessing.stack_unit_bids`
    - Bids are filtered to remove those with zero volume.
    - The function :py:func:`nem_bidding_dashboardpreprocessing.adjust_bids_for_availability` to calculate the bid
      volume adjusted so that the bid volume does not exceed the unit availablity
    - Finally datetime columns are converted to string format.

    Used for preparing data to load into dashboard backend PostgresSQL database, or can be used for
    compiling regional data directly if the user does not have a database, but compling data using this function will be
    considerably slower than from a PostgresSQL database.

    Examples:

    >>> bid_data(
    ... '2022/01/01 00:00:00',
    ... '2022/01/01 00:05:00',
    ... 'D:/nemosis_data_cache')
           INTERVAL_DATETIME     DUID  BIDBAND  BIDVOLUME  BIDVOLUMEADJUSTED  BIDPRICE  ONHOUR
    0    2022-01-01 00:05:00  ADPBA1G        8          6              0.000    998.00   False
    462  2022-01-01 00:05:00   REECE1        2         45             45.000    -55.03   False
    463  2022-01-01 00:05:00   REECE1        4         74             74.000     -0.85   False
    464  2022-01-01 00:05:00   REECE2        2         35             35.000    -54.77   False
    465  2022-01-01 00:05:00   REECE2        4         84             84.000     -0.86   False
    ..                   ...      ...      ...        ...                ...       ...     ...
    235  2022-01-01 00:05:00  GUTHEGA       10         80             68.000  13599.06   False
    236  2022-01-01 00:05:00  HALLWF1        1         95             10.840   -963.00   False
    237  2022-01-01 00:05:00  HALLWF2        1         71              4.408   -960.60   False
    229  2022-01-01 00:05:00  GSTONE6        8          5              5.000   4806.84   False
    700  2022-01-01 00:05:00    YWPS4       10          9              0.000  14489.96   False
    <BLANKLINE>
    [701 rows x 7 columns]

    Args:
       start_time: str formatted "DD/MM/YYYY HH:MM:SS", data with date times greater than start_time are returned
       end_time: str formatted identical to start_time, data with date times less than or equal to end_time are returned
       raw_data_cache: Filepath to directory for caching files downloaded from AEMO

    Returns:
        pandas dataframe with columns INTERVAL_DATETIME, DUID, BIDPRICE ($/MWh), BIDVOLUME (MW), BIDVOLUMEADJUSTED (MW)
    """
    validate_start_end_and_cache_location(start_time, end_time, raw_data_cache)
    volume_bids = fetch_data.volume_bids(start_time, end_time, raw_data_cache)
    volume_bids = volume_bids[volume_bids["BIDTYPE"] == "ENERGY"].drop(
        columns=["BIDTYPE"]
    )
    price_bids = fetch_data.price_bids(start_time, end_time, raw_data_cache)
    price_bids = price_bids[price_bids["BIDTYPE"] == "ENERGY"].drop(columns=["BIDTYPE"])
    availability = fetch_data.duid_availability_data(
        start_time, end_time, raw_data_cache
    )
    combined_bids = preprocessing.stack_unit_bids(volume_bids, price_bids)
    combined_bids = combined_bids[combined_bids["BIDVOLUME"] > 0.0].copy()
    combined_bids = preprocessing.adjust_bids_for_availability(
        combined_bids, availability
    )
    combined_bids = preprocessing.add_on_hour_column(combined_bids)
    combined_bids["INTERVAL_DATETIME"] = combined_bids["INTERVAL_DATETIME"].dt.strftime(
        "%Y-%m-%d %X"
    )
    combined_bids = combined_bids.sort_values("INTERVAL_DATETIME")
    return combined_bids


def duid_info(raw_data_cache):
    """
    Wrapper for fetching and preprocessing duid summary data.

    - Calls :py:func:`nem_bidding_dashboard.fetch_data.get_duid_data` to get raw duid info
    - Calls :py:func:`nem_bidding_dashboard.preprocessing.hard_code_fix_fuel_source_and_tech_errors` to replace NA
      values with '-'
    - Calls :py:func:`nem_bidding_dashboard.preprocessing.remove_number_from_region_names` to remove trailing '1' from
      REGIONIDs
    - Calls :py:func:`nem_bidding_dashboard.preprocessing.tech_namer` to determine a more concise and consistent
      technology type for units. This value is stored in the column "UNIT TYPE".

    Used for preparing data to load into dashboard backend PostgresSQL database, or can be used for
    compiling regional data directly if the user does not have a database.

    Examples:

    >>> duid_info('D:/nemosis_data_cache')
                           STATION NAME REGION DISPATCH TYPE FUEL SOURCE - DESCRIPTOR       TECHNOLOGY TYPE - DESCRIPTOR      DUID           UNIT TYPE
    0       Adelaide Desalination Plant     SA     Generator                     Grid               Battery and Inverter   ADPBA1G   Battery Discharge
    1       Adelaide Desalination Plant     SA          Load                     Grid               Battery and Inverter   ADPBA1L      Battery Charge
    4       Adelaide Desalination Plant     SA     Generator                    Water                       Run of River    ADPMH1  Run of River Hydro
    5       Adelaide Desalination Plant     SA     Generator                    Solar            Photovoltaic Flat panel    ADPPV3               Solar
    6       Adelaide Desalination Plant     SA     Generator                    Solar            Photovoltaic Flat panel    ADPPV2               Solar
    ..                              ...    ...           ...                      ...                                ...       ...                 ...
    576  Yarrawonga Hydro Power Station    VIC     Generator                    Water                    Hydro - Gravity  YWNGAHYD               Hydro
    577            Yarwun Power Station    QLD     Generator              Natural Gas  Combined Cycle Gas Turbine (CCGT)  YARWUN_1                CCGT
    578              Yatpool Solar Farm    VIC     Generator                    Solar   Photovoltaic Tracking Flat panel    YATSF1               Solar
    579                Yawong Wind Farm    VIC     Generator                     Wind                     Wind - Onshore    YAWWF1                Wind
    580                Yendon Wind Farm    VIC     Generator                     Wind                     Wind - Onshore   YENDWF1                Wind
    <BLANKLINE>
    [488 rows x 7 columns]


    Args:
       raw_data_cache: Filepath to directory for caching files downloaded from AEMO

    Returns:
        pandas dataframe with columns DUID, REGIONID, "FUEL SOURCE - DESCRIPTOR", "DISPATCH TYPE",
        "TECHNOLOGY TYPE - DESCRIPTOR", "UNIT TYPE", "STATION NAME"
    """
    data_cache_exits(raw_data_cache)
    duid_info = fetch_data.duid_data(raw_data_cache)
    duid_info = preprocessing.hard_code_fix_fuel_source_and_tech_errors(duid_info)
    duid_info = preprocessing.remove_number_from_region_names("REGION", duid_info)
    duid_info = preprocessing.tech_namer(duid_info)
    return duid_info


def unit_dispatch(start_time, end_time, raw_data_cache):
    """
    Wrapper for fetching and preprocessing unit dispatch data.

    - Calls :py:func:`nem_bidding_dashboard.fetch_data.get_volume_bids` and
      :py:func:`nem_bidding_dashboard.fetch_data.get_duid_availability_data` to get raw
      bidding and availabilty data
    - Volume are filtered to get only bids for the energy spot market
    - Calls :py:func:`nem_bidding_dashboard.preprocessing.calculate_unit_time_series_metrics`
    - Finally datetime columns are converted to string format.

    Used for preparing data to load into dashboard backend PostgresSQL database, or can be used for
    compiling regional data directly if the user does not have database, but compling data using this function will be
    considerably slower than from a PostgresSQL database.

    Examples:

    >>> unit_dispatch(
    ... '2022/01/01 00:55:00',
    ... '2022/01/01 01:05:00',
    ... 'D:/nemosis_data_cache')
           INTERVAL_DATETIME      DUID  AVAILABILITY  TOTALCLEARED    FINALMW  ASBIDRAMPUPMAXAVAIL  ASBIDRAMPDOWNMINAVAIL  RAMPUPMAXAVAIL  RAMPDOWNMINAVAIL  PASAAVAILABILITY  MAXAVAIL  ONHOUR
    0    2022-01-01 01:00:00   ADPBA1G         0.000         0.000    0.00000             10.00000              -10.00000         7.75000          -7.75000               6.0         0    True
    250  2022-01-01 01:00:00     RRSF1         0.000         0.000    0.00000           3480.00000            -3480.00000      3480.00000       -3480.00000             116.0       116    True
    249  2022-01-01 01:00:00    ROMA_8        34.000         0.000    0.00000             40.00000              -40.00000        40.00000         -40.00000              34.0        34    True
    248  2022-01-01 01:00:00    ROMA_7        34.000         0.000    0.00000             40.00000              -40.00000        40.00000         -40.00000              34.0        34    True
    247  2022-01-01 01:00:00    REECE2       119.000       118.000  117.53001            266.89001              -33.10999       266.89001         -33.10999             119.0       119    True
    ..                   ...       ...           ...           ...        ...                  ...                    ...             ...               ...               ...       ...     ...
    118  2022-01-01 01:00:00   GSTONE3       260.000       170.000  170.45000            193.87500              143.87500       193.35000         144.40000             260.0       260    True
    117  2022-01-01 01:00:00   GSTONE2         0.000         0.000    0.00000             25.00000              -25.00000        25.00000         -25.00000             270.0         0    True
    116  2022-01-01 01:00:00   GSTONE1         0.000         0.000    0.00000             15.00000              -15.00000        15.00000         -15.00000               0.0         0    True
    125  2022-01-01 01:00:00  GUNNING1        10.042        10.042    7.97946             25.02899               -4.97101        25.02899          -4.97101              47.0        47    True
    368  2022-01-01 01:00:00     YWPS4       370.000       370.000  370.11520            385.39746              355.39746       385.39746         355.39746             396.0       370    True
    <BLANKLINE>
    [369 rows x 12 columns]

    Args:
       start_time: str formatted "DD/MM/YYYY HH:MM:SS", data with date times greater than start_time are returned
       end_time: str formatted identical to start_time, data with date times less than or equal to end_time are returned
       raw_data_cache: Filepath to directory for caching files downloaded from AEMO

    Returns:
        pandas dataframe with columns INTERVAL_DATETIME, DUID, AVAILABILITY, TOTALCLEARED, FINALMW,
        ASBIDRAMPUPMAXAVAIL, ASBIDRAMPDOWNMINAVAIL, RAMPUPMAXAVAIL, RAMPDOWNMINAVAIL, PASAAVAILABILITY,
        MAXAVAIL (see unit_dispatch in Database Guide for column definitions)
    """
    validate_start_end_and_cache_location(start_time, end_time, raw_data_cache)
    as_bid_metrics = fetch_data.volume_bids(start_time, end_time, raw_data_cache)
    as_bid_metrics = as_bid_metrics[as_bid_metrics["BIDTYPE"] == "ENERGY"].drop(
        columns=["BIDTYPE"]
    )
    as_bid_metrics = as_bid_metrics.loc[
        :,
        [
            "INTERVAL_DATETIME",
            "DUID",
            "MAXAVAIL",
            "ROCUP",
            "ROCDOWN",
            "PASAAVAILABILITY",
        ],
    ]
    after_dispatch_metrics = fetch_data.duid_availability_data(
        start_time, end_time, raw_data_cache
    )
    unit_time_series_metrics = preprocessing.calculate_unit_time_series_metrics(
        as_bid_metrics, after_dispatch_metrics
    )
    unit_time_series_metrics = preprocessing.add_on_hour_column(
        unit_time_series_metrics
    )
    unit_time_series_metrics["INTERVAL_DATETIME"] = unit_time_series_metrics[
        "INTERVAL_DATETIME"
    ].dt.strftime("%Y-%m-%d %X")
    unit_time_series_metrics = unit_time_series_metrics.sort_values("INTERVAL_DATETIME")
    return unit_time_series_metrics


def define_and_return_price_bins():
    """
    Defines the bins for aggregating bidding data.

    Examples:

    >>> define_and_return_price_bins()
              BIN_NAME  LOWER_EDGE  UPPER_EDGE
    0    [-1000, -100)       -2000        -100
    1        [-100, 0)        -100           0
    2          [0, 50)           0          50
    3        [50, 100)          50         100
    4       [100, 200)         100         200
    5       [200, 300)         200         300
    6       [300, 500)         300         500
    7      [500, 1000)         500        1000
    8     [1000, 5000)        1000        5000
    9    [5000, 10000)        5000       10000
    10  [10000, 15500)       10000       16000

    Returns:
        pandas dataframe with column bin_name, lower_edge and upper_edge
    """
    price_bins = pd.DataFrame(
        {
            "BIN_NAME": [
                "[-2000, -100)",
                "[-100, 0)",
                "[0, 50)",
                "[50, 100)",
                "[100, 200)",
                "[200, 300)",
                "[300, 500)",
                "[500, 1000)",
                "[1000, 5000)",
                "[5000, 10000)",
                "[10000, 16500)",
            ],
            "LOWER_EDGE": [-2000, -100, 0, 50, 100, 200, 300, 500, 1000, 5000, 10000],
            "UPPER_EDGE": [-100, 0, 50, 100, 200, 300, 500, 1000, 5000, 10000, 16500],
        }
    )
    return price_bins
