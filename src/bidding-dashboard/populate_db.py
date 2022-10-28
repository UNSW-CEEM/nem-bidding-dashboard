import os

import pandas as pd
from supabase import create_client, Client

import fetch_data

"""This module is used for populating the database used by the dashboard. The functions it contains co-ordinate
 fetching historical AEMO data, pre-processing to limit the work done by the dashboard (to improve responsiveness),
 and loading the processed data into the database. It will contain functions for both populating the production
 version for the hosted version of the dashboard and functions for populating an sqlite database for use by user
 on their local machine."""


def insert_data_into_supabase(table_name, data):
    """Insert data into the supabase database. For this function to run the supabase url and key need to be configured
    as environment variables labeled SUPABASE_BIDDING_DASHBOARD_URL and SUPABASE_BIDDING_DASHBOARD_WRITE_KEY respectively.

    Arguments:
        table_name: str which is the name of the table in the supabase database
        data: pd dataframe of data to be uploaded
    """

    url = os.environ.get("SUPABASE_BIDDING_DASHBOARD_URL")
    key = os.environ.get("SUPABASE_BIDDING_DASHBOARD_WRITE_KEY")
    supabase = create_client(url, key)
    data = supabase.table(table_name).insert(data.to_dict('records')).execute()


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
    demand_data = fetch_data.get_region_demand_data(start_date, end_date, raw_data_cache)
    demand_data['SETTLEMENTDATE'] = demand_data['SETTLEMENTDATE'].dt.strftime('%Y-%m-%d %X')
    insert_data_into_supabase('demand_data', demand_data)


if __name__ == "__main__":
    raw_data_cache = "D:/nemosis_cache"
    populate_supabase_demand_table("2019/01/23 00:00:00", "2019/01/24 00:00:00", raw_data_cache)
