import math
import os
from datetime import datetime, timedelta

import postgrest

postgrest.constants.DEFAULT_POSTGREST_CLIENT_TIMEOUT = (
    300000  # Change supabase client timeout
)

import fetch_data
import numpy as np
import pandas as pd
import preprocessing
import pytz
from supabase import create_client

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


def populate_supabase_region_data(start_date, end_date, raw_data_cache):
    """
    Function to populate database table containing electricity demand and price data by region. The only pre-processing
    done is filtering out the intervention interval rows associated with the dispatch target runs, leaving just the
    pricing run data. For this function to run the supabase url and key need to be configured as environment variables
    labeled SUPABASE_BIDDING_DASHBOARD_URL and SUPABASE_BIDDING_DASHBOARD_WRITE_KEY respectively.

    Arguments:
        start_date: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS" (time always
            set to "00:00:00:)
        end_date: Ending datetime, formatted identical to start_date
        raw_data_cache: Filepath to directory for storing data that is fetched
    """
    regional_data = fetch_data.get_region_data(start_date, end_date, raw_data_cache)
    regional_data = preprocessing.remove_number_from_region_names(
        "REGIONID", regional_data
    )
    regional_data["SETTLEMENTDATE"] = regional_data["SETTLEMENTDATE"].dt.strftime(
        "%Y-%m-%d %X"
    )
    insert_data_into_supabase("demand_data", regional_data)


def populate_supabase_bid_table(start_date, end_date, raw_data_cache):
    """
    Function to populate database table containing bidding data by unit. For this function to run the supabase url and
    key need to be configured as environment variables labeled SUPABASE_BIDDING_DASHBOARD_URL and
    SUPABASE_BIDDING_DASHBOARD_WRITE_KEY respectively.

    Arguments:
        start_date: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS" (time always
            set to "00:00:00:)
        end_date: Ending datetime, formatted identical to start_date
        raw_data_cache: Filepath to directory for storing data that is fetched
    """
    volume_bids = fetch_data.get_volume_bids(start_date, end_date, raw_data_cache)
    volume_bids = volume_bids[volume_bids["BIDTYPE"] == "ENERGY"].drop(
        columns=["BIDTYPE"]
    )
    price_bids = fetch_data.get_price_bids(start_date, end_date, raw_data_cache)
    price_bids = price_bids[price_bids["BIDTYPE"] == "ENERGY"].drop(columns=["BIDTYPE"])
    availability = fetch_data.get_duid_availability_data(
        start_date, end_date, raw_data_cache
    )
    combined_bids = preprocessing.stack_unit_bids(volume_bids, price_bids)
    combined_bids = combined_bids[combined_bids["BIDVOLUME"] > 0.0].copy()
    combined_bids = preprocessing.adjust_bids_for_availability(
        combined_bids, availability
    )
    combined_bids["INTERVAL_DATETIME"] = combined_bids["INTERVAL_DATETIME"].dt.strftime(
        "%Y-%m-%d %X"
    )
    insert_data_into_supabase("bidding_data", combined_bids)


def populate_supabase_duid_info_table(raw_data_cache):
    """
    Function to populate database table containing bidding data by unit. For this function to run
    the supabase url and key need to be configured as environment variables labeled
    SUPABASE_BIDDING_DASHBOARD_URL and SUPABASE_BIDDING_DASHBOARD_WRITE_KEY respectively.

    Arguments:
        start_date: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS" (time always
            set to "00:00:00:)
        end_date: Ending datetime, formatted identical to start_date
        raw_data_cache: Filepath to directory for storing data that is fetched
    """
    duid_info = fetch_data.get_duid_data(raw_data_cache)
    duid_info = preprocessing.hard_code_fix_fuel_source_and_tech_errors(duid_info)
    duid_info = preprocessing.remove_number_from_region_names("REGION", duid_info)
    duid_info = preprocessing.tech_namer(duid_info)
    insert_data_into_supabase("duid_info", duid_info)


def populate_db_unit_dispatch(start_date, end_date, raw_data_cache):
    """
    Function to populate database table containing unit time series metrics. For this function to run the supabase url
    and key need to be configured as environment variables labeled SUPABASE_BIDDING_DASHBOARD_URL and
    SUPABASE_BIDDING_DASHBOARD_WRITE_KEY respectively.

    Arguments:
        start_date: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS" (time always
            set to "00:00:00:)
        end_date: Ending datetime, formatted identical to start_date
        raw_data_cache: Filepath to directory for storing data that is fetched
    """
    as_bid_metrics = fetch_data.get_volume_bids(start_date, end_date, raw_data_cache)
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
        start_date, end_date, raw_data_cache
    )
    unit_time_series_metrics = preprocessing.calculate_unit_time_series_metrics(
        as_bid_metrics, after_dispatch_metrics
    )
    unit_time_series_metrics["INTERVAL_DATETIME"] = unit_time_series_metrics[
        "INTERVAL_DATETIME"
    ].dt.strftime("%Y-%m-%d %X")
    insert_data_into_supabase("unit_dispatch", unit_time_series_metrics)


def populate_supabase_price_bin_edges_table():
    """
    Function to populate database table containing bin definitions for aggregating bids. For this function to run the
    supabase url and key need to be configured as environment variables labeled SUPABASE_BIDDING_DASHBOARD_URL and
    SUPABASE_BIDDING_DASHBOARD_WRITE_KEY respectively.

    Arguments:
        None
    """
    price_bins = pd.DataFrame(
        {
            "bin_name": [
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
            "lower_edge": [-2000, -100, 0, 50, 100, 200, 300, 500, 1000, 5000, 10000],
            "upper_edge": [-100, 0, 50, 100, 200, 300, 500, 1000, 5000, 10000, 16000],
        }
    )
    insert_data_into_supabase("price_bins", price_bins)


def populate_db_all_tables_two_most_recent_market_days(cache):
    """
    Upload data to supabase for a window starting at 4 am of the current day and going back 48 hrs. Upload is
    performed for all tables.

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
        start_date=two_days_before_today, end_date=start_today, raw_data_cache=cache
    )
    populate_supabase_bid_table(
        start_date=two_days_before_today, end_date=start_today, raw_data_cache=cache
    )
    populate_supabase_duid_info_table(raw_data_cache=cache)
    populate_db_unit_dispatch(
        start_date=two_days_before_today, end_date=start_today, raw_data_cache=cache
    )


if __name__ == "__main__":
    raw_data_cache = "D:/nemosis_cache"
    for m in [8, 9, 10, 11]:
        start = "2020/{}/01 00:00:00".format((str(m)).zfill(2))
        end = "2020/{}/01 00:00:00".format((str(m + 1)).zfill(2))
        print(start)
        print(end)
        populate_supabase_duid_info_table(raw_data_cache)
        populate_supabase_bid_table(start, end, raw_data_cache)
        populate_supabase_region_data(start, end, raw_data_cache)
        populate_db_unit_dispatch(start, end, raw_data_cache)
    # populate_supabase_duid_info_table(raw_data_cache)
