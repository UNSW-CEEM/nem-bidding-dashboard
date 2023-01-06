import pandas as pd

from nem_bidding_dashboard import defaults
from nem_bidding_dashboard.postgres_helpers import run_query_return_dataframe

pd.set_option("display.width", None)


def region_demand(connection_string, regions, start_time, end_time):
    """
    Query demand and price data from a postgres database. To aggregate demand data is summed.

    Examples:

    >>> from nem_bidding_dashboard import postgres_helpers

    >>> con_string = postgres_helpers.build_connection_string(
    ... hostname='localhost',
    ... dbname='bidding_dashboard_db',
    ... username='bidding_dashboard_maintainer',
    ... password='1234abcd',
    ... port=5433)

    >>> region_demand(
    ... con_string,
    ... ['NSW'],
    ... "2022/01/02 00:00:00",
    ... "2022/01/02 00:30:00")
           SETTLEMENTDATE  TOTALDEMAND
    5 2022-01-02 00:05:00      6850.57
    1 2022-01-02 00:10:00      6774.01
    3 2022-01-02 00:15:00      6758.63
    4 2022-01-02 00:20:00      6732.82
    2 2022-01-02 00:25:00      6704.92
    0 2022-01-02 00:30:00      6672.90


    Arguments:
        connection_string: str for connecting to PostgresSQL database, the function :py:func:`nem_bidding_dashboard.postgres_helpers.build_connection_string`
            can be used to build a properly formated connection string, or alternative any string that matches the
            format allowed by `PostgresSQL <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING>`_
            can be used
        start_time: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS"
        end_time: Ending datetime, formatted identical to start_time
        raw_data_cache: Filepath to directory for caching files downloaded from AEMO

    Returns:
        pd.DataFrame with columns SETTLEMENTDATE, and TOTALDEMAND (demand to be meet by schedualed and
        semischedualed generators, not including schedualed loads).
    """
    regions = ['"{r}"'.format(r=r) for r in regions]
    regions = ", ".join(regions)
    query = """SELECT * FROM aggregate_demand(
                '{{{regions}}}',
                (timestamp '{start_time}'),
                (timestamp '{end_time}')
                )"""
    query = query.format(regions=regions, start_time=start_time, end_time=end_time)
    data = run_query_return_dataframe(connection_string, query)
    return data.sort_values("SETTLEMENTDATE")


def aggregate_bids(
    connection_string,
    regions,
    start_time,
    end_time,
    resolution,
    raw_adjusted,
    tech_types,
    dispatch_type,
):
    """
    Function to query and aggregate bidding data from postgres database. Data is filtered according to the regions,
    dispatch type, tech types and time window provided, it is then aggregated into a set of predefined bins.
    Data can be queried at hourly or 5 minute resolution. If an hourly resolution is chosen only bid for 5 minute
    interval ending on the hour are returned.

    Examples:

    >>> from nem_bidding_dashboard import postgres_helpers

    >>> con_string = postgres_helpers.build_connection_string(
    ... hostname='localhost',
    ... dbname='bidding_dashboard_db',
    ... username='bidding_dashboard_maintainer',
    ... password='1234abcd',
    ... port=5433)

    >>> aggregate_bids(
    ... con_string,
    ... ['QLD', 'NSW', 'SA'],
    ... "2022/01/02 00:00:00",
    ... "2022/01/02 01:00:00",
    ... 'hourly',
    ... 'adjusted',
    ... [],
    ... 'Generator')
         INTERVAL_DATETIME        BIN_NAME  BIDVOLUME
    9  2022-01-02 01:00:00   [-1000, -100)  9035.5950
    4  2022-01-02 01:00:00       [-100, 0)   198.3533
    7  2022-01-02 01:00:00         [0, 50)  1357.0000
    0  2022-01-02 01:00:00       [50, 100)  1358.0000
    5  2022-01-02 01:00:00      [100, 200)  1371.0000
    2  2022-01-02 01:00:00      [200, 300)  2133.0000
    8  2022-01-02 01:00:00      [300, 500)   957.0000
    3  2022-01-02 01:00:00     [500, 1000)   217.0000
    1  2022-01-02 01:00:00    [1000, 5000)   231.0000
    6  2022-01-02 01:00:00   [5000, 10000)    15.0000
    10 2022-01-02 01:00:00  [10000, 15500)  5543.0000


    >>> aggregate_bids(
    ... con_string,
    ... ['QLD', 'NSW', 'SA'],
    ... "2022/01/02 00:00:00",
    ... "2022/01/02 00:05:00",
    ... '5-min',
    ... 'adjusted',
    ... [],
    ... 'Generator')
         INTERVAL_DATETIME        BIN_NAME   BIDVOLUME
    1  2022-01-02 00:05:00   [-1000, -100)  9120.23200
    10 2022-01-02 00:05:00       [-100, 0)   252.16272
    9  2022-01-02 00:05:00         [0, 50)  1387.00000
    5  2022-01-02 00:05:00       [50, 100)  1798.00000
    3  2022-01-02 00:05:00      [100, 200)  1371.00000
    6  2022-01-02 00:05:00      [200, 300)  1957.00000
    0  2022-01-02 00:05:00      [300, 500)   935.00000
    2  2022-01-02 00:05:00     [500, 1000)   217.00000
    4  2022-01-02 00:05:00    [1000, 5000)   231.00000
    7  2022-01-02 00:05:00   [5000, 10000)    15.00000
    8  2022-01-02 00:05:00  [10000, 15500)  5367.00000

    Arguments:
        connection_string: str for connecting to PostgresSQL database, the function :py:func:`nem_bidding_dashboard.postgres_helpers.build_connection_string`
            can be used to build a properly formated connection string, or alternative any string that matches the
            format allowed by `PostgresSQL <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING>`_
            can be used
        regions: list[str] regions to aggregate should only be QLD, NSW, VIC, SA or TAS.
        start_time: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS"
        end_time: Ending datetime, formatted identical to start_time
        resolution: str 'hourly' or '5-min'
        dispatch_type: str 'Generator' or 'Load'
        raw_adjusted: str which bid data to use aggregate 'raw' or 'adjusted'. Adjusted bid data has been
            adjusted down so the total bid does not exceed the unit availability.
        tech_types: list[str] the technology types to filter for e.g. Solar, Black Coal, CCGT. An empty list
            will result in no filtering by technology

    Returns:
        pd.DataFrame with columns INTERVAL_DATETIME, BIN_NAME (upper and lower limits of price bin) and
        BIDVOLUME (total volume bid by units within price bin).
    """
    regions = ['"{r}"'.format(r=r) for r in regions]
    regions = ", ".join(regions)
    tech_types = ['"{t}"'.format(t=t) for t in tech_types]
    tech_types = ", ".join(tech_types)
    query = f"""SELECT * FROM aggregate_bids_v2(
                '{{{regions}}}',
                (timestamp '{start_time}'),
                (timestamp '{end_time}'),
                '{resolution}',
                '{dispatch_type}',
                '{raw_adjusted}',
                '{{{tech_types}}}'
                )"""
    data = run_query_return_dataframe(connection_string, query)
    data["BIN_NAME"] = data["BIN_NAME"].astype("category")
    data["BIN_NAME"] = data["BIN_NAME"].cat.set_categories(defaults.bid_order)
    return data.sort_values(["INTERVAL_DATETIME", "BIN_NAME"])


def duid_bids(connection_string, duids, start_time, end_time, resolution, adjusted):
    """
    Function to query bidding data from a postgres database. Data is filter according to the DUID list and time window
    provided, and returned on a duid basis. Data can queryed at hourly or 5 minute resolution. If an hourly resolution
    is chosen only bid for 5 minute interval ending on the hour are returned.

    Examples:

    >>> from nem_bidding_dashboard import postgres_helpers

    >>> con_string = postgres_helpers.build_connection_string(
    ... hostname='localhost',
    ... dbname='bidding_dashboard_db',
    ... username='bidding_dashboard_maintainer',
    ... password='1234abcd',
    ... port=5433)

    >>> duid_bids(
    ... con_string,
    ... ['AGLHAL', 'BASTYAN'],
    ... "2022/01/02 00:00:00",
    ... "2022/01/02 01:00:00",
    ... 'hourly',
    ... 'adjusted')
        INTERVAL_DATETIME     DUID  BIDBAND  BIDVOLUME  BIDPRICE
    0 2022-01-02 01:00:00   AGLHAL        7       32.0    557.39
    1 2022-01-02 01:00:00   AGLHAL       10      121.0  14541.30
    2 2022-01-02 01:00:00  BASTYAN        2       53.0    -55.64
    3 2022-01-02 01:00:00  BASTYAN        4       28.0     -0.91
    4 2022-01-02 01:00:00  BASTYAN       10        0.0  14021.86


    >>> duid_bids(
    ... con_string,
    ... ['AGLHAL', 'BASTYAN'],
    ... "2022/01/02 00:00:00",
    ... "2022/01/02 01:00:00",
    ... 'hourly',
    ... 'adjusted')
        INTERVAL_DATETIME     DUID  BIDBAND  BIDVOLUME  BIDPRICE
    0 2022-01-02 01:00:00   AGLHAL        7       32.0    557.39
    1 2022-01-02 01:00:00   AGLHAL       10      121.0  14541.30
    2 2022-01-02 01:00:00  BASTYAN        2       53.0    -55.64
    3 2022-01-02 01:00:00  BASTYAN        4       28.0     -0.91
    4 2022-01-02 01:00:00  BASTYAN       10        0.0  14021.86

    Arguments:
        connection_string: str for connecting to PostgresSQL database, the function :py:func:`nem_bidding_dashboard.postgres_helpers.build_connection_string`
            can be used to build a properly formated connection string, or alternative any string that matches the
            format allowed by `PostgresSQL <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING>`_
            can be used
        duids: list[str] of duids to return in result.
        start_time: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS"
        end_time: Ending datetime, formatted identical to start_time
        resolution: str 'hourly' or '5-min'
        adjusted: str which bid data to use aggregate 'raw' or 'adjusted'. Adjusted bid data has been
            adjusted down so the total bid does not exceed the unit availability.

    Returns:
        pd.DataFrame with columns INTERVAL_DATETIME, DUID, BIDBAND, BIDVOLUME, and BIDPRICE
    """
    duids = ['"{d}"'.format(d=d) for d in duids]
    duids = ", ".join(duids)
    query = """SELECT * FROM get_bids_by_unit_v2(
                '{{{duids}}}',
                (timestamp '{start_time}'),
                (timestamp '{end_time}'),
                '{resolution}',
                '{adjusted}')"""
    query = query.format(
        duids=duids,
        start_time=start_time,
        end_time=end_time,
        resolution=resolution,
        adjusted=adjusted,
    )
    data = run_query_return_dataframe(connection_string, query)
    return data.sort_values(["INTERVAL_DATETIME", "DUID", "BIDBAND"])


def stations_and_duids_in_regions_and_time_window(
    connection_string, regions, start_time, end_time, dispatch_type, tech_types
):
    """
    Function to query units from given regions with bids available in the given time window, with the the given dispatch
    and technology type. Data returned is DUIDs and corresponding Station Names.

    Examples:

    >>> from nem_bidding_dashboard import postgres_helpers

    >>> con_string = postgres_helpers.build_connection_string(
    ... hostname='localhost',
    ... dbname='bidding_dashboard_db',
    ... username='bidding_dashboard_maintainer',
    ... password='1234abcd',
    ... port=5433)

    >>> stations_and_duids_in_regions_and_time_window(
    ... con_string,
    ... ['NSW'],
    ... "2022/01/02 00:00:00",
    ... "2022/01/02 01:00:00",
    ... "Generator",
    ... [])
            DUID             STATION NAME
    0   BANGOWF1      Bango 973 Wind Farm
    1   BANGOWF2      Bango 999 Wind Farm
    6   BERYLSF1         Beryl Solar Farm
    7   BLOWERNG  Blowering Power Station
    8   BOCORWF1      Boco Rock Wind Farm
    ..       ...                      ...
    69  WALGRVG1         Wallgrove BESS 1
    70   WELLSF1    Wellington Solar Farm
    73  WOODLWN1       Woodlawn Wind Farm
    71     WRSF1    White Rock Solar Farm
    72     WRWF1     White Rock Wind Farm
    <BLANKLINE>
    [74 rows x 2 columns]

    Arguments:
        connection_string: str for connecting to PostgresSQL database, the function :py:func:`nem_bidding_dashboard.postgres_helpers.build_connection_string`
            can be used to build a properly formated connection string, or alternative any string that matches the
            format allowed by `PostgresSQL <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING>`_
            can be used
        regions: list[str] regions to filter, should only be QLD, NSW, VIC, SA or TAS.
        start_time: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS"
        end_time: Ending datetime, formatted identical to start_time
        dispatch_type: str 'Generator' or 'Load'
        tech_types: list[str] the technology types to filter for e.g. Solar, Black Coal, CCGT. An empty list
            will result in no filtering by technology

    Returns:
        pd.DataFrame with columns DUID and STATION NAME
    """
    regions = ['"{r}"'.format(r=r) for r in regions]
    regions = ", ".join(regions)
    tech_types = ['"{t}"'.format(t=t) for t in tech_types]
    tech_types = ", ".join(tech_types)
    query = f"""SELECT * FROM get_duids_and_staions_in_regions_and_time_window_v2(
                '{{{regions}}}',
                (timestamp '{start_time}'),
                (timestamp '{end_time}'),
                '{dispatch_type}',
                '{{{tech_types}}}'
                )"""
    data = run_query_return_dataframe(connection_string, query)
    return data.sort_values("DUID")


def get_aggregated_dispatch_data(
    connection_string,
    column_name,
    regions,
    start_time,
    end_time,
    resolution,
    dispatch_type,
    tech_types,
):
    """
    Function to query dispatch and aggregate data from a postgres database. Data is filter according to the regions,
    time window, dispatch type, and technology type  provided. Data can queryed at hourly or 5 minute resolution.
    If an hourly resolution is chosen only data for 5 minute interval ending on the hour are returned.

    Examples:

    >>> from nem_bidding_dashboard import postgres_helpers

    >>> con_string = postgres_helpers.build_connection_string(
    ... hostname='localhost',
    ... dbname='bidding_dashboard_db',
    ... username='bidding_dashboard_maintainer',
    ... password='1234abcd',
    ... port=5433)

    >>> get_aggregated_dispatch_data(
    ... con_string,
    ... 'AVAILABILITY',
    ... ['NSW'],
    ... "2022/01/02 00:00:00",
    ... "2022/01/02 01:00:00",
    ... 'hourly',
    ... 'Generator',
    ... [])
        INTERVAL_DATETIME  COLUMNVALUES
    0 2022-01-02 01:00:00     10606.271


    >>> get_aggregated_dispatch_data(
    ... con_string,
    ... 'AVAILABILITY',
    ... ['NSW'],
    ... "2022/01/02 00:00:00",
    ... "2022/01/02 00:05:00",
    ... '5-min',
    ... 'Generator',
    ... [])
        INTERVAL_DATETIME  COLUMNVALUES
    0 2022-01-02 00:05:00     10686.938

    Arguments:
        connection_string: str for connecting to PostgresSQL database, the function :py:func:`nem_bidding_dashboard.postgres_helpers.build_connection_string`
            can be used to build a properly formated connection string, or alternative any string that matches the
            format allowed by `PostgresSQL <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING>`_
            can be used
        column_name: str, which column of dispatch data to aggregate and return. Should be one of NTERVAL_DATETIME,
            ASBIDRAMPUPMAXAVAIL (upper dispatch limit based on as bid ramp rate, when aggregated unit contribution cannot
            exceed MAXAVAIL), ASBIDRAMPDOWNMINAVAIL (lower dispatch limit based on as bid ramp rate, when aggregated unit
            contribution cannot be less than zero), RAMPUPMAXAVAIL (upper dispatch limit based lesser of as bid and
            telemetry ramp rates, when aggregated unit contribution cannot exceed AVAILABILITY), RAMPDOWNMINAVAIL (lower
            dispatch limit based lesser of as bid and telemetry ramp rates, when aggregated unit contribution cannot be less
            than zero), AVAILABILITY, TOTALCLEARED (as for after_dispatch_metrics), PASAAVAILABILITY, MAXAVAIL (as for
            as_bid_metrics), and FINALMW (the unit operating level at the end of the dispatch interval).
        regions: list[str] regions to aggregate should only be QLD, NSW, VIC, SA or TAS.
        start_time: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS"
        end_time: Ending datetime, formatted identical to start_time
        resolution: str 'hourly' or '5-min'
        dispatch_type: str 'Generator' or 'Load'
        tech_types: list[str] the technology types to filter for e.g. Solar, Black Coal, CCGT. An empty list
            will result in no filtering by technology

    Returns:
        pd.DataFrame containing columns INTERVAL_DATETIME, COLUMNVALUES (aggregate of column specified in input)
    """
    regions = ['"{r}"'.format(r=r) for r in regions]
    regions = ", ".join(regions)
    tech_types = ['"{t}"'.format(t=t) for t in tech_types]
    tech_types = ", ".join(tech_types)
    query = f"""SELECT * FROM aggregate_dispatch_data_v2(
                '{column_name}',
                '{{{regions}}}',
                (timestamp '{start_time}'),
                (timestamp '{end_time}'),
                '{resolution}',
                '{dispatch_type}',
                '{{{tech_types}}}'
                )"""
    data = run_query_return_dataframe(connection_string, query)
    return data.sort_values("INTERVAL_DATETIME")


def get_aggregated_dispatch_data_by_duids(
    connection_string, column_name, duids, start_time, end_time, resolution
):
    """
    Function to query dispatch and aggregate data from a postgres database. Data is filter according to the DUIDs,
    and time window. Data can queryed at hourly or 5 minute resolution. If an hourly resolution is chosen only data for
    5 minute interval ending on the hour are returned.

    Examples:

    >>> from nem_bidding_dashboard import postgres_helpers

    >>> con_string = postgres_helpers.build_connection_string(
    ... hostname='localhost',
    ... dbname='bidding_dashboard_db',
    ... username='bidding_dashboard_maintainer',
    ... password='1234abcd',
    ... port=5433)

    >>> get_aggregated_dispatch_data_by_duids(
    ... con_string,
    ... 'AVAILABILITY',
    ... ['AGLHAL', 'BASTYAN'],
    ... "2022/01/02 00:00:00",
    ... "2022/01/02 01:00:00",
    ... 'hourly')
        INTERVAL_DATETIME  COLUMNVALUES
    0 2022-01-02 01:00:00         234.0


    >>> get_aggregated_dispatch_data_by_duids(
    ... con_string,
    ... 'AVAILABILITY',
    ... ['AGLHAL', 'BASTYAN'],
    ... "2022/01/02 00:00:00",
    ... "2022/01/02 00:05:00",
    ... '5-min')
        INTERVAL_DATETIME  COLUMNVALUES
    0 2022-01-02 00:05:00         234.0

    Arguments:
        connection_string: str for connecting to PostgresSQL database, the function :py:func:`nem_bidding_dashboard.postgres_helpers.build_connection_string`
            can be used to build a properly formated connection string, or alternative any string that matches the
            format allowed by `PostgresSQL <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING>`_
            can be used
        column_name: str, which column of dispatch data to aggregate and return. Should be one of NTERVAL_DATETIME,
            ASBIDRAMPUPMAXAVAIL (upper dispatch limit based on as bid ramp rate, when aggregated unit contribution cannot
            exceed MAXAVAIL), ASBIDRAMPDOWNMINAVAIL (lower dispatch limit based on as bid ramp rate, when aggregated unit
            contribution cannot be less than zero), RAMPUPMAXAVAIL (upper dispatch limit based lesser of as bid and
            telemetry ramp rates, when aggregated unit contribution cannot exceed AVAILABILITY), RAMPDOWNMINAVAIL (lower
            dispatch limit based lesser of as bid and telemetry ramp rates, when aggregated unit contribution cannot be less
            than zero), AVAILABILITY, TOTALCLEARED (as for after_dispatch_metrics), PASAAVAILABILITY, MAXAVAIL (as for
            as_bid_metrics), and FINALMW (the unit operating level at the end of the dispatch interval).
        duids: list[str] of duids to return in result.
        start_time: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS"
        end_time: Ending datetime, formatted identical to start_time
        resolution: str 'hourly' or '5-min'

    Returns:
        pd.DataFrame containing columns INTERVAL_DATETIME, COLUMNVALUES (aggregate of column specified in input)
    """
    duids = ['"{d}"'.format(d=d) for d in duids]
    duids = ", ".join(duids)
    query = """SELECT * FROM aggregate_dispatch_data_duids_v2(
                '{column_name}',
                '{{{duids}}}',
                (timestamp '{start_time}'),
                (timestamp '{end_time}'),
                '{resolution}'
                )"""
    query = query.format(
        column_name=column_name.lower(),
        duids=duids,
        start_time=start_time,
        end_time=end_time,
        resolution=resolution,
    )
    data = run_query_return_dataframe(connection_string, query)
    return data.sort_values(["INTERVAL_DATETIME"])


def get_aggregated_vwap(connection_string, regions, start_time, end_time):
    """
    Function to query aggregated Volume Weighted Average Price from supabase. Data is filter according to the regions
    and time window provided. Data can queryed at hourly or 5 minute resolution. Prices are weighted by demand in each
    region selected.

    Examples:

    >>> from nem_bidding_dashboard import postgres_helpers

    >>> con_string = postgres_helpers.build_connection_string(
    ... hostname='localhost',
    ... dbname='bidding_dashboard_db',
    ... username='bidding_dashboard_maintainer',
    ... password='1234abcd',
    ... port=5433)

    >>> get_aggregated_vwap(
    ... con_string,
    ... ['NSW'],
    ... "2022/01/02 00:00:00",
    ... "2022/01/02 01:00:00")
            SETTLEMENTDATE      PRICE
    10 2022-01-02 00:05:00  110.22005
    3  2022-01-02 00:10:00  104.30393
    6  2022-01-02 00:15:00   85.50552
    8  2022-01-02 00:20:00   78.07000
    4  2022-01-02 00:25:00   85.00000
    1  2022-01-02 00:30:00   85.00000
    7  2022-01-02 00:35:00  103.51609
    0  2022-01-02 00:40:00   94.31247
    11 2022-01-02 00:45:00  103.13011
    9  2022-01-02 00:50:00   96.08903
    2  2022-01-02 00:55:00   86.38491
    5  2022-01-02 01:00:00   87.05018


    Arguments:
        connection_string: str for connecting to PostgresSQL database, the function :py:func:`nem_bidding_dashboard.postgres_helpers.build_connection_string`
            can be used to build a properly formated connection string, or alternative any string that matches the
            format allowed by `PostgresSQL <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING>`_
            can be used
        regions: list[str] of region to aggregate.
        start_time: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS"
        end_time: Ending datetime, formatted identical to start_time

    Returns:
        pd.DataFrame with columns SETTLEMENTDATE, TOTALDEMAND and RRP (volume weighted avergae of energy price at
        regional reference nodes).
    """
    regions = ['"{r}"'.format(r=r) for r in regions]
    regions = ", ".join(regions)
    query = """SELECT * FROM aggregate_prices(
                '{{{regions}}}',
                (timestamp '{start_time}'),
                (timestamp '{end_time}')
                )"""
    query = query.format(regions=regions, start_time=start_time, end_time=end_time)
    data = run_query_return_dataframe(connection_string, query)
    return data.sort_values("SETTLEMENTDATE")


def unit_types(connection_string, dispatch_type, regions):
    """
    Function to query distinct unit types from postgres database.

    Examples:

    >>> from nem_bidding_dashboard import postgres_helpers

    >>> con_string = postgres_helpers.build_connection_string(
    ... hostname='localhost',
    ... dbname='bidding_dashboard_db',
    ... username='bidding_dashboard_maintainer',
    ... password='1234abcd',
    ... port=5433)

    >>> unit_types(
    ... con_string,
    ... 'Generator',
    ... ['NSW'])
                UNIT TYPE
    7             Bagasse
    4   Battery Discharge
    2          Black Coal
    0                CCGT
    5              Engine
    8               Hydro
    6                OCGT
    9  Run of River Hydro
    1               Solar
    3                Wind

    Args:
        connection_string: str for connecting to PostgresSQL database, the function :py:func:`nem_bidding_dashboard.postgres_helpers.build_connection_string`
            can be used to build a properly formated connection string, or alternative any string that matches the
            format allowed by `PostgresSQL <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING>`_
            can be used
        dispatch_type: str 'Generator' or 'Load'
        regions: list[str] regions to aggregate should only be QLD, NSW, VIC, SA or TAS.

    Returns:
        pd.DataFrame column UNIT TYPE (this is the unit type as determined by the function
        :py:func:`nem_bidding_dashboard.preprocessing.tech_namer_by_row`)
    """
    regions = ['"{r}"'.format(r=r) for r in regions]
    regions = ", ".join(regions)
    query = """SELECT * FROM distinct_unit_types_v3(
                '{dispatch_type}',
                '{{{regions}}}'
                )"""
    query = query.format(dispatch_type=dispatch_type, regions=regions)
    data = run_query_return_dataframe(connection_string, query)
    return data.sort_values("UNIT TYPE")
