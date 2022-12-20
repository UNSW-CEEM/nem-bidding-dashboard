import pandas as pd

from nem_bidding_dashboard import fetch_data, preprocessing

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
    ... '2020/01/01 00:00:00',
    ... '2020/01/01 00:05:00',
    ... 'D:/nemosis_data_cache')
      REGIONID       SETTLEMENTDATE  TOTALDEMAND       RRP
    0      NSW  2020-01-01 00:05:00      7245.31  49.00916
    1      QLD  2020-01-01 00:05:00      6095.75  50.81148
    2       SA  2020-01-01 00:05:00      1466.53  68.00000
    3      TAS  2020-01-01 00:05:00      1010.06  81.79115
    4      VIC  2020-01-01 00:05:00      4267.32  65.67826

    Args:
       start_time: str formatted "DD/MM/YYYY HH:MM:SS", data with date times greater than start_time are returned
       end_time: str formatted identical to start_time, data with date times less than or equal to end_time are returned
       raw_data_cache: Filepath to directory for caching files downloaded from AEMO

    Returns:
        pandas dataframe with columns SETTLEMENTDATE, REGIONID, TOTALDEMAND (the operational demand AEMO dispatches
        generation to meet), and RRP (the regional reference price for energy).
    """
    regional_data = fetch_data.get_region_data(start_time, end_time, raw_data_cache)
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
    ... '2020/01/01 00:00:00',
    ... '2020/01/01 00:05:00',
    ... 'D:/nemosis_data_cache')
           INTERVAL_DATETIME      DUID  BIDBAND  BIDVOLUME  BIDVOLUMEADJUSTED  BIDPRICE  ONHOUR
    0    2020-01-01 00:05:00    BALBL1        1         20                0.0    -48.06   False
    1    2020-01-01 00:05:00    RT_SA4        1       3000                0.0  -1000.00   False
    2    2020-01-01 00:05:00    RT_SA5        1       3000                0.0  -1000.00   False
    3    2020-01-01 00:05:00    RT_SA6        1       3000                0.0  -1000.00   False
    4    2020-01-01 00:05:00   RT_TAS1        1       3000                0.0  -1000.00   False
    ..                   ...       ...      ...        ...                ...       ...     ...
    523  2020-01-01 00:05:00   GSTONE6       10          5                0.0  13557.81   False
    524  2020-01-01 00:05:00   GUTHEGA       10         80               67.0  13253.52   False
    525  2020-01-01 00:05:00  JBUTTERS       10          4                0.0  12309.16   False
    526  2020-01-01 00:05:00   GSTONE1       10          5                0.0  13557.81   False
    527  2020-01-01 00:05:00     LBBG1       10         25               25.0  14585.34   False
    <BLANKLINE>
    [528 rows x 7 columns]

    Args:
       start_time: str formatted "DD/MM/YYYY HH:MM:SS", data with date times greater than start_time are returned
       end_time: str formatted identical to start_time, data with date times less than or equal to end_time are returned
       raw_data_cache: Filepath to directory for caching files downloaded from AEMO

    Returns:
        pandas dataframe with columns INTERVAL_DATETIME, DUID, BIDPRICE ($/MWh), BIDVOLUME (MW), BIDVOLUMEADJUSTED (MW)
    """
    volume_bids = fetch_data.get_volume_bids(start_time, end_time, raw_data_cache)
    volume_bids = volume_bids[volume_bids["BIDTYPE"] == "ENERGY"].drop(
        columns=["BIDTYPE"]
    )
    price_bids = fetch_data.get_price_bids(start_time, end_time, raw_data_cache)
    price_bids = price_bids[price_bids["BIDTYPE"] == "ENERGY"].drop(columns=["BIDTYPE"])
    availability = fetch_data.get_duid_availability_data(
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
    578  Yarrawonga Hydro Power Station    VIC     Generator                    Water                    Hydro - Gravity  YWNGAHYD               Hydro
    579            Yarwun Power Station    QLD     Generator              Natural Gas  Combined Cycle Gas Turbine (CCGT)  YARWUN_1                CCGT
    580              Yatpool Solar Farm    VIC     Generator                    Solar   Photovoltaic Tracking Flat panel    YATSF1               Solar
    581                Yawong Wind Farm    VIC     Generator                     Wind                     Wind - Onshore    YAWWF1                Wind
    582                Yendon Wind Farm    VIC     Generator                     Wind                     Wind - Onshore   YENDWF1                Wind
    <BLANKLINE>
    [490 rows x 7 columns]

    Args:
       raw_data_cache: Filepath to directory for caching files downloaded from AEMO

    Returns:
        pandas dataframe with columns DUID, REGIONID, "FUEL SOURCE - DESCRIPTOR", "DISPATCH TYPE",
        "TECHNOLOGY TYPE - DESCRIPTOR", "UNIT TYPE", "STATION NAME"
    """
    duid_info = fetch_data.get_duid_data(raw_data_cache)
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
    ... '2020/01/02 00:55:00',
    ... '2020/01/02 01:05:00',
    ... 'D:/nemosis_data_cache')
           INTERVAL_DATETIME      DUID  AVAILABILITY  TOTALCLEARED  FINALMW  ASBIDRAMPUPMAXAVAIL  ASBIDRAMPDOWNMINAVAIL  RAMPUPMAXAVAIL  RAMPDOWNMINAVAIL  PASAAVAILABILITY  MAXAVAIL  ONHOUR
    0    2020-01-02 01:00:00   DG_VIC1         0.000         0.000    0.000            500.00000             -500.00000       500.00000        -500.00000               0.0         0    True
    1    2020-01-02 01:00:00   RT_TAS1         0.000         0.000    0.000            500.00000             -500.00000       500.00000        -500.00000               0.0         0    True
    2    2020-01-02 01:00:00   DG_QLD1         0.000         0.000    0.000            500.00000             -500.00000       500.00000        -500.00000               0.0         0    True
    3    2020-01-02 01:00:00   DG_NSW1         0.000         0.000    0.000            500.00000             -500.00000       500.00000        -500.00000               0.0         0    True
    4    2020-01-02 01:00:00    DG_SA1         0.000         0.000    0.000            500.00000             -500.00000       500.00000        -500.00000               0.0         0    True
    ..                   ...       ...           ...           ...      ...                  ...                    ...             ...               ...               ...       ...     ...
    296  2020-01-02 01:00:00      QPS3        23.000         0.000    0.000             15.00000              -15.00000        15.00000         -15.00000              23.0        23    True
    297  2020-01-02 01:00:00  WOODLWN1        14.554        14.554   15.453             79.36401               -0.63599        79.36401          -0.63599              48.0        48    True
    298  2020-01-02 01:00:00   RT_NSW3         0.000         0.000    0.000            500.00000             -500.00000       500.00000        -500.00000               0.0         0    True
    299  2020-01-02 01:00:00   HAYMSF1         0.000         0.000    0.000            500.00000             -500.00000       500.00000        -500.00000              50.0        50    True
    300  2020-01-02 01:00:00    NBHWF1        83.310        83.310   81.000            130.50000               30.50000       130.50000          30.50000             132.0       132    True
    <BLANKLINE>
    [301 rows x 12 columns]

    Args:
       start_time: str formatted "DD/MM/YYYY HH:MM:SS", data with date times greater than start_time are returned
       end_time: str formatted identical to start_time, data with date times less than or equal to end_time are returned
       raw_data_cache: Filepath to directory for caching files downloaded from AEMO

    Returns:
        pandas dataframe with columns INTERVAL_DATETIME, DUID, AVAILABILITY, TOTALCLEARED, FINALMW,
        ASBIDRAMPUPMAXAVAIL, ASBIDRAMPDOWNMINAVAIL, RAMPUPMAXAVAIL, RAMPDOWNMINAVAIL, PASAAVAILABILITY,
        MAXAVAIL (see unit_dispatch in Database Guide for column definitions)
    """
    as_bid_metrics = fetch_data.get_volume_bids(start_time, end_time, raw_data_cache)
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
    after_dispatch_metrics = fetch_data.get_duid_availability_data(
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
              bin_name  lower_edge  upper_edge
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
                "[-1000, -100)",
                "[-100, 0)",
                "[0, 50)",
                "[50, 100)",
                "[100, 200)",
                "[200, 300)",
                "[300, 500)",
                "[500, 1000)",
                "[1000, 5000)",
                "[5000, 10000)",
                "[10000, 15500)",
            ],
            "LOWER_EDGE": [-2000, -100, 0, 50, 100, 200, 300, 500, 1000, 5000, 10000],
            "UPPER_EDGE": [-100, 0, 50, 100, 200, 300, 500, 1000, 5000, 10000, 16000],
        }
    )
    return price_bins
