import math
import os
from datetime import datetime, timedelta

import numpy as np
import postgrest
import pytz

postgrest.constants.DEFAULT_POSTGREST_CLIENT_TIMEOUT = (
    300000  # Change supabase client timeout
)

from supabase import create_client

from nem_bidding_dashboard import fetch_and_preprocess

"""This module is used for populating the database used by the dashboard. The functions it contains co-ordinate
 fetching historical AEMO data, pre-processing to limit the work done by the dashboard (to improve responsiveness),
 and loading the processed data into the database. It will contain functions for both populating the production
 version for the hosted version of the dashboard and functions for populating an sqlite database for use by user
 on their local machine."""


def insert_data_into_supabase(table_name, data):
    """Insert data into the supabase database. For this function to run the supabase url and key need to be configured
    as environment variables labeled SUPABASE_BIDDING_DASHBOARD_URL and SUPABASE_BIDDING_DASHBOARD_WRITE_KEY
    respectively.

    Arguments:
        table_name: str which is the name of the table in the supabase database
        data: pd dataframe of data to be uploaded
    """

    url = os.environ.get("SUPABASE_BIDDING_DASHBOARD_URL")
    key = os.environ.get("SUPABASE_BIDDING_DASHBOARD_WRITE_KEY")
    supabase = create_client(url, key)
    rows_per_chunk = 5000
    data.columns = data.columns.str.lower()
    number_of_chunks = math.ceil(data.shape[0] / rows_per_chunk)
    chunked_data = np.array_split(data, number_of_chunks)
    for chunk in chunked_data:
        _ = supabase.table(table_name).upsert(chunk.to_dict("records")).execute()


def populate_supabase_region_data(start_time, end_time, raw_data_cache):
    """
    Function to populate database table containing electricity demand and price data by region. For this function to run
    the supabase url and key need to be configured as environment variables labeled SUPABASE_BIDDING_DASHBOARD_URL and
    SUPABASE_BIDDING_DASHBOARD_WRITE_KEY respectively. Data is preped for loading by the function
    :py:func:`nem_bidding_dashboard.fetch_and_preprocess.region_data`.

    Examples:

    >>> populate_supabase_region_data(
    ... "2020/01/01 00:00:00",
    ... "2020/01/02 00:00:00",
    ... "D:/nemosis_cache")

    Arguments:
        start_time: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS"
        end_time: Ending datetime, formatted identical to start_time
        raw_data_cache: Filepath to directory for storing data that is fetched
    """
    regional_data = fetch_and_preprocess.region_data(
        start_time, end_time, raw_data_cache
    )
    insert_data_into_supabase("demand_data", regional_data)


def populate_supabase_bid_data(start_time, end_time, raw_data_cache):
    """
    Function to populate database table containing bidding data by unit. For this function to run the supabase url and
    key need to be configured as environment variables labeled SUPABASE_BIDDING_DASHBOARD_URL and
    SUPABASE_BIDDING_DASHBOARD_WRITE_KEY respectively. Data is preped for loading by the function
    :py:func:`nem_bidding_dashboard.fetch_and_preprocess.bid_data`.

    Examples:

    >>> populate_supabase_bid_data(
    ... "2020/01/01 00:00:00",
    ... "2020/01/02 00:00:00",
    ... "D:/nemosis_cache")

    Arguments:
        start_time: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS"
        end_time: Ending datetime, formatted identical to start_time
        raw_data_cache: Filepath to directory for storing data that is fetched
    """
    combined_bids = fetch_and_preprocess.bid_data(start_time, end_time, raw_data_cache)
    insert_data_into_supabase("bidding_data", combined_bids)


def populate_supabase_duid_info(raw_data_cache):
    """
    Function to populate database table containing bidding data by unit. For this function to run
    the supabase url and key need to be configured as environment variables labeled
    SUPABASE_BIDDING_DASHBOARD_URL and SUPABASE_BIDDING_DASHBOARD_WRITE_KEY respectively. Data is preped for loading by
    the function :py:func:`nem_bidding_dashboard.fetch_and_preprocess.duid_info`.

    Examples:

    >>> populate_supabase_duid_info(
    ... "2020/01/01 00:00:00",
    ... "2020/01/02 00:00:00",
    ... "D:/nemosis_cache")

    Arguments:
        start_time: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS"
        end_time: Ending datetime, formatted identical to start_time
        raw_data_cache: Filepath to directory for storing data that is fetched
    """
    duid_info = fetch_and_preprocess.duid_info(raw_data_cache)
    insert_data_into_supabase("duid_info", duid_info)


def populate_supabase_unit_dispatch(start_time, end_time, raw_data_cache):
    """
    Function to populate database table containing unit time series metrics. For this function to run the supabase url
    and key need to be configured as environment variables labeled SUPABASE_BIDDING_DASHBOARD_URL and
    SUPABASE_BIDDING_DASHBOARD_WRITE_KEY respectively. Data is preped for loading by the function
    :py:func:`nem_bidding_dashboard.fetch_and_preprocess.unit_dispatch`.

    Examples:

    >>> populate_supabase_unit_dispatch(
    ... "2020/01/01 00:00:00",
    ... "2020/01/02 00:00:00",
    ... "D:/nemosis_cache")

    Arguments:
        start_time: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS"
        end_time: Ending datetime, formatted identical to start_time
        raw_data_cache: Filepath to directory for storing data that is fetched
    """
    unit_time_series_metrics = fetch_and_preprocess.unit_dispatch(
        start_time, end_time, raw_data_cache
    )
    insert_data_into_supabase("unit_dispatch", unit_time_series_metrics)


def populate_supabase_price_bin_edges_table():
    """
    Function to populate database table containing bin definitions for aggregating bids. For this function to run the
    supabase url and key need to be configured as environment variables labeled SUPABASE_BIDDING_DASHBOARD_URL and
    SUPABASE_BIDDING_DASHBOARD_WRITE_KEY respectively. Data is preped for loading by the function
    :py:func:`nem_bidding_dashboard.fetch_and_preprocess.region_data`.

    Examples:

    >>> populate_supabase_price_bin_edges_table()
    """
    price_bins = fetch_and_preprocess.define_and_return_price_bins()
    insert_data_into_supabase("price_bins", price_bins)


def populate_supabase_all_tables_two_most_recent_market_days(cache):
    """
    Upload data to supabase for a window starting at 4 am of the current day and going back 48 hrs. Upload is
    performed for all tables.

    Examples:

    >>> populate_supabase_all_tables_two_most_recent_market_days(
    ... "D:/nemosis_cache")

    Arguments:
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
    populate_supabase_region_data(
        start_time=two_days_before_today, end_time=start_today, raw_data_cache=cache
    )
    populate_supabase_bid_data(
        start_time=two_days_before_today, end_time=start_today, raw_data_cache=cache
    )
    populate_supabase_duid_info(raw_data_cache=cache)
    populate_supabase_unit_dispatch(
        start_time=two_days_before_today, end_time=start_today, raw_data_cache=cache
    )


if __name__ == "__main__":
    raw_data_cache = "D:/nemosis_cache"
    # for m in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]:
    #     start = "2020/{}/01 00:00:00".format((str(m)).zfill(2))
    #     end = "2020/{}/01 00:00:00".format((str(m + 1)).zfill(2))
    #     print(start)
    #     print(end)
    #     populate_supabase_duid_info(raw_data_cache)
    #     populate_supabase_bid_data(start, end, raw_data_cache)
    #     populate_supabase_region_data(start, end, raw_data_cache)
    #     populate_supabase_unit_dispatch(start, end, raw_data_cache)
    # populate_supabase_duid_info(raw_data_cache)
    populate_supabase_all_tables_two_most_recent_market_days(raw_data_cache)
