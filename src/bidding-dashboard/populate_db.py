import math
import os

import fetch_data
import numpy as np
import pandas as pd
import preprocessing
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
    rows_per_chunk = 10000
    number_of_chunks = math.ceil(data.shape[0] / rows_per_chunk)
    chunked_data = np.array_split(data, number_of_chunks)
    for chunk in chunked_data:
        _ = supabase.table(table_name).upsert(chunk.to_dict("records")).execute()


def populate_supabase_demand_table(start_date, end_date, raw_data_cache):
    """
    Function to populate database table containing electricity demand data by region. The only pre-processing done is
    filtering out the intervention interval rows associated with the dispatch target runs, leaving just the pricing
    run data. For this function to run the supabase url and key need to be configured as environment variables labeled
    SUPABASE_BIDDING_DASHBOARD_URL and SUPABASE_BIDDING_DASHBOARD_WRITE_KEY respectively.

    Arguments:
        start_date: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS" (time always
            set to "00:00:00:)
        end_date: Ending datetime, formatted identical to start_date
        raw_data_cache: Filepath to directory for storing data that is fetched
    """
    demand_data = fetch_data.get_region_demand_data(
        start_date, end_date, raw_data_cache
    )
    demand_data["SETTLEMENTDATE"] = demand_data["SETTLEMENTDATE"].dt.strftime(
        "%Y-%m-%d %X"
    )
    insert_data_into_supabase("demand_data", demand_data)


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
    combined_bids = combined_bids.rename(
        columns={
            "INTERVAL_DATETIME": "interval_datetime",
            "DUID": "duid",
            "BIDBAND": "bidband",
            "BIDVOLUME": "bidvolume",
            "BIDPRICE": "bidprice",
        }
    )
    insert_data_into_supabase("bidding_data", combined_bids)


def populate_supabase_duid_info_table(raw_data_cache):
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
    duid_info = fetch_data.get_duid_data(raw_data_cache)
    duid_info = preprocessing.remove_number_from_region_names("Region", duid_info)
    insert_data_into_supabase("duid_info", duid_info.loc[:, ["DUID", "Region"]])


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


if __name__ == "__main__":
    raw_data_cache = "D:/nemosis_cache"
    populate_supabase_bid_table(
        "2019/01/01 00:00:00", "2019/02/01 00:00:00", raw_data_cache
    )
    # populate_supabase_duid_info_table(
    #     raw_data_cache
    # )
    # populate_supabase_price_bin_edges_table()
