from datetime import datetime, timedelta

import pytz

from nem_bidding_dashboard import (
    fetch_and_preprocess,
    input_validation,
    postgres_helpers,
)
from nem_bidding_dashboard.postgres_helpers import insert_data_into_postgres

"""This module is used for populating the database used by the dashboard. The functions it contains co-ordinate
 fetching historical AEMO data, pre-processing to limit the work done by the dashboard (to improve responsiveness),
 and loading the processed data into the database. It will contain functions for both populating the production
 version for the hosted version of the dashboard and functions for populating an sqlite database for use by user
 on their local machine."""


def region_data(connection_string, raw_data_cache, start_time, end_time):
    """
    Function to populate database table containing electricity demand and price data by region. Data is prepped for
    loading by the function :py:func:`nem_bidding_dashboard.fetch_and_preprocess.define_and_return_price_bins`.

    Examples:

    >>> from nem_bidding_dashboard import postgres_helpers

    >>> con_string = postgres_helpers.build_connection_string(
    ... hostname='localhost',
    ... dbname='bidding_dashboard_db',
    ... username='bidding_dashboard_maintainer',
    ... password='1234abcd',
    ... port=5433)

    >>> region_data(
    ... con_string,
    ... "D:/nemosis_cache",
    ... "2020/01/01 00:00:00",
    ... "2020/01/02 00:00:00")

    Arguments:
        connection_string: str for connecting to PostgresSQL database, the function :py:func:`nem_bidding_dashboard.postgres_helpers.build_connection_string`
            can be used to build a properly formated connection string, or alternative any string that matches the
            format allowed by `PostgresSQL <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING>`_
            can be used
        start_time: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS"
        end_time: Ending datetime, formatted identical to start_time
        raw_data_cache: Filepath to directory for storing data that is fetched
    """
    input_validation.validate_start_end_and_cache_location(
        start_time, end_time, raw_data_cache
    )
    regional_data = fetch_and_preprocess.region_data(
        start_time, end_time, raw_data_cache
    )
    insert_data_into_postgres(connection_string, "demand_data", regional_data)


def bid_data(connection_string, raw_data_cache, start_time, end_time):
    """
    Function to populate database table containing bidding data by unit. Data is prepped for loading by the
    function :py:func:`nem_bidding_dashboard.fetch_and_preprocess.bid_data`.

    Examples:

    >>> from nem_bidding_dashboard import postgres_helpers

    >>> con_string = postgres_helpers.build_connection_string(
    ... hostname='localhost',
    ... dbname='bidding_dashboard_db',
    ... username='bidding_dashboard_maintainer',
    ... password='1234abcd',
    ... port=5433)

    >>> bid_data(
    ... con_string,
    ... "D:/nemosis_cache",
    ... "2020/01/01 00:00:00",
    ... "2020/01/02 00:00:00")

    Arguments:
        connection_string: str for connecting to PostgresSQL database, the function :py:func:`nem_bidding_dashboard.postgres_helpers.build_connection_string`
            can be used to build a properly formated connection string, or alternative any string that matches the
            format allowed by `PostgresSQL <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING>`_
            can be used
        start_time: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS"
        end_time: Ending datetime, formatted identical to start_time
        raw_data_cache: Filepath to directory for storing data that is fetched

    """
    input_validation.validate_start_end_and_cache_location(
        start_time, end_time, raw_data_cache
    )
    combined_bids = fetch_and_preprocess.bid_data(start_time, end_time, raw_data_cache)
    insert_data_into_postgres(connection_string, "bidding_data", combined_bids)


def duid_info(connection_string, raw_data_cache):
    """
    Function to populate database table containing bidding data by unit. Data is prepped for loading by the
    function :py:func:`nem_bidding_dashboard.fetch_and_preprocess.duid_info`.

    Examples:

    >>> from nem_bidding_dashboard import postgres_helpers

    >>> con_string = postgres_helpers.build_connection_string(
    ... hostname='localhost',
    ... dbname='bidding_dashboard_db',
    ... username='bidding_dashboard_maintainer',
    ... password='1234abcd',
    ... port=5433)

    >>> duid_info(
    ...  con_string,
    ... "D:/nemosis_data_cache",)

    Arguments:
        connection_string: str for connecting to PostgresSQL database, the function :py:func:`nem_bidding_dashboard.postgres_helpers.build_connection_string`
            can be used to build a properly formated connection string, or alternative any string that matches the
            format allowed by `PostgresSQL <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING>`_
            can be used
        start_time: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS"
        end_time: Ending datetime, formatted identical to start_time
        raw_data_cache: Filepath to directory for storing data that is fetched

    """
    input_validation.data_cache_exits(raw_data_cache)
    duid_info = fetch_and_preprocess.duid_info(raw_data_cache)
    insert_data_into_postgres(connection_string, "duid_info", duid_info)


def unit_dispatch(connection_string, raw_data_cache, start_time, end_time):
    """
    Function to populate database table containing unit time series metrics. Data is prepped for loading by the
    function :py:func:`nem_bidding_dashboard.fetch_and_preprocess.unit_dispatch`.

    Examples:

    >>> from nem_bidding_dashboard import postgres_helpers

    >>> con_string = postgres_helpers.build_connection_string(
    ... hostname='localhost',
    ... dbname='bidding_dashboard_db',
    ... username='bidding_dashboard_maintainer',
    ... password='1234abcd',
    ... port=5433)

    >>> unit_dispatch(
    ... con_string,
    ... "D:/nemosis_cache",
    ... "2020/01/01 00:00:00",
    ... "2020/01/02 00:00:00")

    Arguments:
        connection_string: str for connecting to PostgresSQL database, the function :py:func:`nem_bidding_dashboard.postgres_helpers.build_connection_string`
            can be used to build a properly formated connection string, or alternative any string that matches the
            format allowed by `PostgresSQL <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING>`_
            can be used
        start_time: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS"
        end_time: Ending datetime, formatted identical to start_time
        raw_data_cache: Filepath to directory for storing data that is fetched
    """
    input_validation.validate_start_end_and_cache_location(
        start_time, end_time, raw_data_cache
    )
    unit_time_series_metrics = fetch_and_preprocess.unit_dispatch(
        start_time, end_time, raw_data_cache
    )
    insert_data_into_postgres(
        connection_string, "unit_dispatch", unit_time_series_metrics
    )


def price_bin_edges_table(connection_string):
    """
    Function to populate database table containing bin definitions for aggregating bids. Data is prepped for loading by
    the function :py:func:`nem_bidding_dashboard.fetch_and_preprocess.region_data`.

    Examples:

    >>> from nem_bidding_dashboard import postgres_helpers

    >>> con_string = postgres_helpers.build_connection_string(
    ... hostname='localhost',
    ... dbname='bidding_dashboard_db',
    ... username='bidding_dashboard_maintainer',
    ... password='1234abcd',
    ... port=5433)

    >>> price_bin_edges_table(
    ... con_string)

    Arguments:
        connection_string: str for connecting to PostgresSQL database, the function :py:func:`nem_bidding_dashboard.postgres_helpers.build_connection_string`
            can be used to build a properly formated connection string, or alternative any string that matches the
            format allowed by `PostgresSQL <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING>`_
            can be used
    """
    price_bins = fetch_and_preprocess.define_and_return_price_bins()
    insert_data_into_postgres(connection_string, "price_bins", price_bins)


def all_tables_two_most_recent_market_days(connection_string, cache):
    """
    Load data to postgres database for a window starting at 4 am of the current day and going back 48 hrs. Loading is
    performed for all tables except price_bin_edges.

    Examples:

    >>> from nem_bidding_dashboard import postgres_helpers

    >>> con_string = postgres_helpers.build_connection_string(
    ... hostname='localhost',
    ... dbname='bidding_dashboard_db',
    ... username='bidding_dashboard_maintainer',
    ... password='1234abcd',
    ... port=5433)

    >>> all_tables_two_most_recent_market_days(
    ...  con_string
    ... "D:/nemosis_data_cache")

    Arguments:
        connection_string: str for connecting to PostgresSQL database, the function :py:func:`nem_bidding_dashboard.postgres_helpers.build_connection_string`
            can be used to build a properly formated connection string, or alternative any string that matches the
            format allowed by `PostgresSQL <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING>`_
            can be used
        cache: str the directory to use for caching data prior to upload.
    """
    current_time = datetime.now(pytz.timezone("Etc/GMT-10"))
    start_today = datetime(
        year=current_time.year,
        month=current_time.month,
        day=current_time.day,
        hour=4,
    )
    two_days_before_today = start_today - timedelta(days=2)
    start_today = start_today.isoformat().replace("T", " ").replace("-", "/")
    two_days_before_today = (
        two_days_before_today.isoformat().replace("T", " ").replace("-", "/")
    )
    region_data(
        connection_string=connection_string,
        raw_data_cache=cache,
        start_time=two_days_before_today,
        end_time=start_today,
    )
    bid_data(
        connection_string=connection_string,
        raw_data_cache=cache,
        start_time=two_days_before_today,
        end_time=start_today,
    )
    duid_info(connection_string=connection_string, raw_data_cache=cache)
    unit_dispatch(
        connection_string=connection_string,
        raw_data_cache=cache,
        start_time=two_days_before_today,
        end_time=start_today,
    )


if __name__ == "__main__":
    raw_data_cache = "D:/nemosis_data_cache"
    con_string = postgres_helpers.build_connection_string(
        hostname="localhost",
        dbname="bidding_dashboard_db",
        username="bidding_dashboard_maintainer",
        password="1234abcd",
        port=5433,
    )
    for m in [2, 3, 4, 5, 6, 7, 8, 9, 10, 11]:
        start = "2022/{}/01 00:00:00".format((str(m)).zfill(2))
        end = "2022/{}/01 00:00:00".format((str(m + 1)).zfill(2))
        print(start)
        print(end)
        duid_info(con_string, raw_data_cache)
        bid_data(con_string, raw_data_cache, start, end)
        region_data(con_string, raw_data_cache, start, end)
        unit_dispatch(con_string, raw_data_cache, start, end)
    price_bin_edges_table(con_string)
