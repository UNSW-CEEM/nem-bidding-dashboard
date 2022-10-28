import os

import pandas as pd
from supabase import create_client

"""This module is used for query the database used by the dashboard. It will contain functions for both query the
   production database for the hosted version of the dashboard and functions for querying an sqlite database for use
   by user on their local machine."""


def query_supabase(table_name, query):
    """Query the supabase database. For this function to run the supabase url and key need to be configured
    as environment variables labeled SUPABASE_BIDDING_DASHBOARD_URL and SUPABASE_BIDDING_DASHBOARD_KEY respectively.

    Arguments:
        table_name: str which is the name of the table in the supabase database
        query: str postgres sql query to run
    """

    url = os.environ.get("SUPABASE_BIDDING_DASHBOARD_URL")
    key = os.environ.get("SUPABASE_BIDDING_DASHBOARD_KEY")
    supabase = create_client(url, key)
    data = (
        supabase.table(table_name)
        .select("*")
        .gt("SETTLEMENTDATE", "2019/01/23 12:00:00")
        .lt("SETTLEMENTDATE", "2019/01/23 12:00:00")
        .execute()
    )
    return data


def query_supabase_demand_data(start_date, end_date):
    """
    Function to query demand data from supabase. For this function to run the supabase url and key need to be configured
    as environment variables labeled SUPABASE_BIDDING_DASHBOARD_URL and SUPABASE_BIDDING_DASHBOARD_KEY
    respectively.

    Arguments:
        start_date: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS" (time always
            set to "00:00:00:)
        end_date: Ending datetime, formatted identical to start_date
    """
    url = os.environ.get("SUPABASE_BIDDING_DASHBOARD_URL")
    key = os.environ.get("SUPABASE_BIDDING_DASHBOARD_KEY")
    supabase = create_client(url, key)
    data = (
        supabase.table("demand_data")
        .select("*")
        .gt("SETTLEMENTDATE", start_date)
        .lte("SETTLEMENTDATE", end_date)
        .execute()
    )
    data = pd.DataFrame(data.data)
    return data


if __name__ == "__main__":
    data = query_supabase_demand_data("2019-01-23 12:00:00", "2019-01-23 12:05:00")
    print(data)
