import pandas as pd

from nem_bidding_dashboard import defaults, input_validation
from nem_bidding_dashboard.postgres_helpers import run_query_return_dataframe

pd.set_option("display.width", None)


def region_demand(connection_string, start_time, end_time, regions):
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
    ... "2022/01/01 01:00:00",
    ... "2022/01/01 01:30:00",
    ... ['NSW'])
            SETTLEMENTDATE  TOTALDEMAND
    0  2022-01-01 01:05:00      6631.21
    1  2022-01-01 01:10:00      6655.52
    2  2022-01-01 01:15:00      6496.85
    3  2022-01-01 01:20:00      6520.86
    4  2022-01-01 01:25:00      6439.22
    5  2022-01-01 01:30:00      6429.13


    Arguments:
        connection_string: str for connecting to PostgresSQL database, the function :py:func:`nem_bidding_dashboard.postgres_helpers.build_connection_string`
            can be used to build a properly formated connection string, or alternative any string that matches the
            format allowed by `PostgresSQL <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING>`_
            can be used
        start_time: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS"
        end_time: Ending datetime, formatted identical to start_time
        regions: list[str] regions to aggregate should only be QLD, NSW, VIC, SA or TAS.

    Returns:
        pd.DataFrame with columns SETTLEMENTDATE, and TOTALDEMAND (demand to be meet by schedualed and
        semischedualed generators, not including schedualed loads).
    """
    input_validation.validate_region_demand_args(start_time, end_time, regions)
    regions = ['"{r}"'.format(r=r) for r in regions]
    regions = ", ".join(regions)
    query = """SELECT * FROM aggregate_demand(
                '{{{regions}}}',
                (timestamp '{start_time}'),
                (timestamp '{end_time}')
                )"""
    query = query.format(regions=regions, start_time=start_time, end_time=end_time)
    data = run_query_return_dataframe(connection_string, query)
    if data.empty:
        data = pd.DataFrame(
            {
                "SETTLEMENTDATE": pd.Series(dtype="datetime64[ns]"),
                "TOTALDEMAND": pd.Series(dtype="float"),
            }
        )
    data["SETTLEMENTDATE"] = data["SETTLEMENTDATE"].dt.strftime("%Y-%m-%d %X")
    return data.sort_values("SETTLEMENTDATE").reset_index(drop=True)


def aggregate_bids(
    connection_string,
    start_time,
    end_time,
    regions,
    dispatch_type,
    tech_types,
    resolution,
    adjusted,
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
    ... "2022/01/01 01:00:00",
    ... "2022/01/01 02:00:00",
    ... ['QLD', 'NSW', 'SA'],
    ... 'Generator',
    ... [],
    ... 'hourly',
    ... 'adjusted')
          INTERVAL_DATETIME        BIN_NAME   BIDVOLUME
    0   2022-01-01 02:00:00   [-1000, -100)  9158.02700
    1   2022-01-01 02:00:00       [-100, 0)   299.74405
    2   2022-01-01 02:00:00         [0, 50)  1142.00000
    3   2022-01-01 02:00:00       [50, 100)  1141.00000
    4   2022-01-01 02:00:00      [100, 200)   918.00000
    5   2022-01-01 02:00:00      [200, 300)  1138.00000
    6   2022-01-01 02:00:00      [300, 500)   920.00000
    7   2022-01-01 02:00:00     [500, 1000)   273.00000
    8   2022-01-01 02:00:00    [1000, 5000)   210.00000
    9   2022-01-01 02:00:00   [5000, 10000)   125.00000
    10  2022-01-01 02:00:00  [10000, 15500)  7009.00000


    >>> aggregate_bids(
    ... con_string,
    ... "2022/01/01 01:00:00",
    ... "2022/01/01 01:05:00",
    ... ['QLD', 'NSW', 'SA'],
    ... 'Generator',
    ... [],
    ... '5-min',
    ... 'adjusted')
          INTERVAL_DATETIME        BIN_NAME   BIDVOLUME
    0   2022-01-01 01:05:00   [-1000, -100)  9642.26100
    1   2022-01-01 01:05:00       [-100, 0)   361.94458
    2   2022-01-01 01:05:00         [0, 50)  1348.00000
    3   2022-01-01 01:05:00       [50, 100)  1415.00000
    4   2022-01-01 01:05:00      [100, 200)   912.00000
    5   2022-01-01 01:05:00      [200, 300)  1188.00000
    6   2022-01-01 01:05:00      [300, 500)   903.00000
    7   2022-01-01 01:05:00     [500, 1000)   272.00000
    8   2022-01-01 01:05:00    [1000, 5000)   210.00000
    9   2022-01-01 01:05:00   [5000, 10000)   125.00000
    10  2022-01-01 01:05:00  [10000, 15500)  6853.00000

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
        adjusted: str which bid data to use aggregate 'raw' or 'adjusted'. Adjusted bid data has been
            adjusted down so the total bid does not exceed the unit availability.
        tech_types: list[str] the technology types to filter for e.g. Solar, Black Coal, CCGT. An empty list
            will result in no filtering by technology

    Returns:
        pd.DataFrame with columns INTERVAL_DATETIME, BIN_NAME (upper and lower limits of price bin) and
        BIDVOLUME (total volume bid by units within price bin).
    """
    input_validation.validate_aggregate_bids_args(
        regions, start_time, end_time, resolution, adjusted, tech_types, dispatch_type
    )
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
                '{adjusted}',
                '{{{tech_types}}}'
                )"""
    data = run_query_return_dataframe(connection_string, query)
    if data.empty:
        data = pd.DataFrame(
            {
                "INTERVAL_DATETIME": pd.Series(dtype="datetime64[ns]"),
                "BIN_NAME": pd.Series(dtype="str"),
                "BIDVOLUME": pd.Series(dtype="float"),
            }
        )
    data["BIN_NAME"] = data["BIN_NAME"].astype("category")
    data["BIN_NAME"] = data["BIN_NAME"].cat.set_categories(defaults.bid_order)
    data = data.sort_values(["INTERVAL_DATETIME", "BIN_NAME"]).reset_index(drop=True)
    data["BIN_NAME"] = data["BIN_NAME"].astype(str)
    data["INTERVAL_DATETIME"] = data["INTERVAL_DATETIME"].dt.strftime("%Y-%m-%d %X")
    data["BIDVOLUME"] = data["BIDVOLUME"].astype(float)
    return data


def duid_bids(connection_string, start_time, end_time, duids, resolution, adjusted):
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
    ... "2022/01/01 01:00:00",
    ... "2022/01/01 02:00:00",
    ... ['AGLHAL', 'BASTYAN'],
    ... 'hourly',
    ... 'adjusted')
         INTERVAL_DATETIME     DUID  BIDBAND  BIDVOLUME  BIDPRICE
    0  2022-01-01 02:00:00   AGLHAL        7       32.0    557.39
    1  2022-01-01 02:00:00   AGLHAL       10      121.0  14541.30
    2  2022-01-01 02:00:00  BASTYAN        2       53.0    -55.64
    3  2022-01-01 02:00:00  BASTYAN        4       28.0     -0.91
    4  2022-01-01 02:00:00  BASTYAN       10        0.0  14021.86


    >>> duid_bids(
    ... con_string,
    ... "2022/01/01 01:00:00",
    ... "2022/01/01 02:00:00",
    ... ['AGLHAL', 'BASTYAN'],
    ... 'hourly',
    ... 'adjusted')
         INTERVAL_DATETIME     DUID  BIDBAND  BIDVOLUME  BIDPRICE
    0  2022-01-01 02:00:00   AGLHAL        7       32.0    557.39
    1  2022-01-01 02:00:00   AGLHAL       10      121.0  14541.30
    2  2022-01-01 02:00:00  BASTYAN        2       53.0    -55.64
    3  2022-01-01 02:00:00  BASTYAN        4       28.0     -0.91
    4  2022-01-01 02:00:00  BASTYAN       10        0.0  14021.86

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
    input_validation.validate_duid_bids_args(
        duids, start_time, end_time, resolution, adjusted
    )
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
    if data.empty:
        data = pd.DataFrame(
            {
                "INTERVAL_DATETIME": pd.Series(dtype="datetime64[ns]"),
                "DUID": pd.Series(dtype="str"),
                "BIDBAND": pd.Series(dtype="int64"),
                "BIDVOLUME": pd.Series(dtype="float"),
                "BIDPRICE": pd.Series(dtype="float"),
            }
        )
    data["INTERVAL_DATETIME"] = data["INTERVAL_DATETIME"].dt.strftime("%Y-%m-%d %X")
    data["BIDVOLUME"] = data["BIDVOLUME"].astype(float)
    return data.sort_values(["INTERVAL_DATETIME", "DUID", "BIDBAND"]).reset_index(
        drop=True
    )


def stations_and_duids_in_regions_and_time_window(
    connection_string, start_time, end_time, regions, dispatch_type, tech_types
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
    ... "2022/01/01 01:00:00",
    ... "2022/01/01 02:00:00",
    ... ['NSW'],
    ... "Generator",
    ... [])
            DUID             STATION NAME
    0   BANGOWF1      Bango 973 Wind Farm
    1   BANGOWF2      Bango 999 Wind Farm
    2   BERYLSF1         Beryl Solar Farm
    3   BLOWERNG  Blowering Power Station
    4   BOCORWF1      Boco Rock Wind Farm
    ..       ...                      ...
    69  WALGRVG1         Wallgrove BESS 1
    70   WELLSF1    Wellington Solar Farm
    71  WOODLWN1       Woodlawn Wind Farm
    72     WRSF1    White Rock Solar Farm
    73     WRWF1     White Rock Wind Farm
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
    input_validation.validate_stations_and_duids_in_regions_and_time_window_args(
        regions, start_time, end_time, dispatch_type, tech_types
    )
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
    if data.empty:
        data = pd.DataFrame(columns=["DUID", "STATION NAME"])
    return data.sort_values("DUID").reset_index(drop=True)


def aggregated_dispatch_data(
    connection_string,
    column_name,
    start_time,
    end_time,
    regions,
    dispatch_type,
    tech_types,
    resolution,
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

    >>> aggregated_dispatch_data(
    ... con_string,
    ... 'AVAILABILITY',
    ... "2022/01/01 01:00:00",
    ... "2022/01/01 02:00:00",
    ... ['NSW'],
    ... 'Generator',
    ... [],
    ... 'hourly')
         INTERVAL_DATETIME  COLUMNVALUES
    0  2022-01-01 02:00:00     10402.473


    >>> aggregated_dispatch_data(
    ... con_string,
    ... 'AVAILABILITY',
    ... "2022/01/01 01:00:00",
    ... "2022/01/01 01:05:00",
    ... ['NSW'],
    ... 'Generator',
    ... [],
    ... '5-min')
         INTERVAL_DATETIME  COLUMNVALUES
    0  2022-01-01 01:05:00     10440.107

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
    input_validation.validate_get_aggregated_dispatch_data_args(
        column_name,
        regions,
        start_time,
        end_time,
        resolution,
        dispatch_type,
        tech_types,
    )
    regions = ['"{r}"'.format(r=r) for r in regions]
    regions = ", ".join(regions)
    tech_types = ['"{t}"'.format(t=t) for t in tech_types]
    tech_types = ", ".join(tech_types)
    query = f"""SELECT * FROM aggregate_dispatch_data_v2(
                '{column_name.lower()}',
                '{{{regions}}}',
                (timestamp '{start_time}'),
                (timestamp '{end_time}'),
                '{resolution}',
                '{dispatch_type}',
                '{{{tech_types}}}'
                )"""
    data = run_query_return_dataframe(connection_string, query)
    if not data.empty:
        data.columns = data.columns.str.upper()
        data["INTERVAL_DATETIME"] = data["INTERVAL_DATETIME"].dt.strftime("%Y-%m-%d %X")
        data["COLUMNVALUES"] = data["COLUMNVALUES"].astype(float)
        data = data.sort_values("INTERVAL_DATETIME").reset_index(drop=True)
    else:
        data = pd.DataFrame(columns=["INTERVAL_DATETIME", "COLUMNVALUES"])
    data["COLUMNVALUES"] = data["COLUMNVALUES"].astype(float)
    data = data.sort_values("INTERVAL_DATETIME").reset_index(drop=True)
    return data


def aggregated_dispatch_data_by_duids(
    connection_string, column_name, start_time, end_time, duids, resolution
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

    >>> aggregated_dispatch_data_by_duids(
    ... con_string,
    ... 'AVAILABILITY',
    ... "2022/01/01 01:00:00",
    ... "2022/01/01 02:00:00",
    ... ['AGLHAL', 'BASTYAN'],
    ... 'hourly')
         INTERVAL_DATETIME  COLUMNVALUES
    0  2022-01-01 02:00:00         234.0


    >>> aggregated_dispatch_data_by_duids(
    ... con_string,
    ... 'AVAILABILITY',
    ... "2022/01/01 01:00:00",
    ... "2022/01/01 01:05:00",
    ... ['AGLHAL', 'BASTYAN'],
    ... '5-min')
         INTERVAL_DATETIME  COLUMNVALUES
    0  2022-01-01 01:05:00         234.0

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
    input_validation.validate_get_aggregated_dispatch_data_by_duids_args(
        column_name, duids, start_time, end_time, resolution
    )
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
    if data.empty:
        data = pd.DataFrame(
            {
                "INTERVAL_DATETIME": pd.Series(dtype="datetime64[ns]"),
                "COLUMNVALUES": pd.Series(dtype="float"),
            }
        )
    data["INTERVAL_DATETIME"] = data["INTERVAL_DATETIME"].dt.strftime("%Y-%m-%d %X")
    data["COLUMNVALUES"] = data["COLUMNVALUES"].astype(float)
    return data.sort_values(["INTERVAL_DATETIME"]).reset_index(drop=True)


def aggregated_vwap(connection_string, start_time, end_time, regions):
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

    >>> aggregated_vwap(
    ... con_string,
    ... "2022/01/01 01:00:00",
    ... "2022/01/01 02:00:00",
    ... ['NSW'])
             SETTLEMENTDATE      PRICE
    0   2022-01-01 01:05:00  107.80005
    1   2022-01-01 01:10:00  107.80005
    2   2022-01-01 01:15:00   91.92056
    3   2022-01-01 01:20:00  107.80005
    4   2022-01-01 01:25:00   91.37289
    5   2022-01-01 01:30:00   91.38851
    6   2022-01-01 01:35:00   92.14760
    7   2022-01-01 01:40:00  100.27929
    8   2022-01-01 01:45:00   91.90742
    9   2022-01-01 01:50:00  100.30000
    10  2022-01-01 01:55:00   85.00000
    11  2022-01-01 02:00:00   85.00005


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
    input_validation.validate_region_demand_args(start_time, end_time, regions)
    regions = ['"{r}"'.format(r=r) for r in regions]
    regions = ", ".join(regions)
    query = """SELECT * FROM aggregate_prices(
                '{{{regions}}}',
                (timestamp '{start_time}'),
                (timestamp '{end_time}')
                )"""
    query = query.format(regions=regions, start_time=start_time, end_time=end_time)
    data = run_query_return_dataframe(connection_string, query)
    if data.empty:
        data = pd.DataFrame(
            {
                "SETTLEMENTDATE": pd.Series(dtype="datetime64[ns]"),
                "PRICE": pd.Series(dtype="float"),
            }
        )
    data["SETTLEMENTDATE"] = data["SETTLEMENTDATE"].dt.strftime("%Y-%m-%d %X")
    return data.sort_values("SETTLEMENTDATE").reset_index(drop=True)


def unit_types(connection_string, regions, dispatch_type):
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

    >>> unit_types(con_string,
    ... ['NSW'],
    ... 'Generator')
                UNIT TYPE
    0             Bagasse
    1   Battery Discharge
    2          Black Coal
    3                CCGT
    4              Engine
    5               Hydro
    6                OCGT
    7  Run of River Hydro
    8               Solar
    9                Wind

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
    input_validation.validate_unit_types_args(dispatch_type, regions)
    regions = ['"{r}"'.format(r=r) for r in regions]
    regions = ", ".join(regions)
    query = """SELECT * FROM distinct_unit_types_v3(
                '{dispatch_type}',
                '{{{regions}}}'
                )"""
    query = query.format(dispatch_type=dispatch_type, regions=regions)
    data = run_query_return_dataframe(connection_string, query)
    if data.empty:
        data = pd.DataFrame(columns=["UNIT TYPE"])
    return data.sort_values("UNIT TYPE").reset_index(drop=True)
