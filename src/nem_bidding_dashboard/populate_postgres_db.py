import math
from datetime import datetime, timedelta

import fetch_and_preprocess
import numpy as np
import postgress_helpers
import psycopg
import pytz

"""This module is used for populating the database used by the dashboard. The functions it contains co-ordinate
 fetching historical AEMO data, pre-processing to limit the work done by the dashboard (to improve responsiveness),
 and loading the processed data into the database. It will contain functions for both populating the production
 version for the hosted version of the dashboard and functions for populating an sqlite database for use by user
 on their local machine."""


def insert_data_into_postgres(connection_string, table_name, data):
    """Insert data into the supabase database. For this function to run the supabase url and key need to be configured
    as environment variables labeled SUPABASE_BIDDING_DASHBOARD_URL and SUPABASE_BIDDING_DASHBOARD_WRITE_KEY
    respectively.

    Arguments:
        table_name: str which is the name of the table in the supabase database
        data: pd dataframe of data to be uploaded
    """
    with psycopg.connect(connection_string) as conn:
        with conn.cursor() as cur:
            rows_per_chunk = 5000
            data.columns = data.columns.str.lower()
            number_of_chunks = math.ceil(data.shape[0] / rows_per_chunk)
            chunked_data = np.array_split(data, number_of_chunks)
            for chunk in chunked_data:
                column_list = [
                    c if " " not in c else '"' + c + '"' for c in data.columns
                ]
                columns = ", ".join(column_list)
                place_holders = ",".join(["%s" for c in data.columns])
                sets = ", ".join(
                    ["{c} = excluded.{c}".format(c=c) for c in column_list]
                )
                query = (
                    "INSERT INTO {table_name}({columns}) VALUES({place_holders}) ON CONFLICT ON CONSTRAINT "
                    + "{table_name}_pkey DO UPDATE SET {sets};"
                )
                query = query.format(
                    table_name=table_name,
                    columns=columns,
                    place_holders=place_holders,
                    sets=sets,
                )
                chunk = list(chunk.itertuples(index=False, name=None))
                cur.executemany(query, chunk)
                conn.commit()


def populate_postgres_region_data(
    connection_string, start_date, end_date, raw_data_cache
):
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

    Examples
    --------
    >>> from nem_bidding_dashboard import postgress_helpers

    >>> con_string = postgress_helpers.build_connection_string(
    ... hostname='localhost',
    ... dbname='bidding_dashboard_db',
    ... username='bidding_dashboard_maintainer',
    ... password='1234abcd',
    ... port=5433)

    >>> populate_postgres_region_data(
    ... con_string,
    ... "2020/01/01 00:00:00",
    ... "2020/01/02 00:00:00",
    ... "D:/nemosis_cache")
    """

    regional_data = fetch_and_preprocess.region_data(
        start_date, end_date, raw_data_cache
    )
    insert_data_into_postgres(connection_string, "demand_data", regional_data)


def populate_postgres_bid_table(
    connection_string, start_date, end_date, raw_data_cache
):
    """
    Function to populate database table containing bidding data by unit. For this function to run the supabase url and
    key need to be configured as environment variables labeled SUPABASE_BIDDING_DASHBOARD_URL and
    SUPABASE_BIDDING_DASHBOARD_WRITE_KEY respectively.

    Arguments:
        start_date: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS" (time always
            set to "00:00:00:)
        end_date: Ending datetime, formatted identical to start_date
        raw_data_cache: Filepath to directory for storing data that is fetched

    Examples
    --------
    >>> from nem_bidding_dashboard import postgress_helpers

    >>> con_string = postgress_helpers.build_connection_string(
    ... hostname='localhost',
    ... dbname='bidding_dashboard_db',
    ... username='bidding_dashboard_maintainer',
    ... password='1234abcd',
    ... port=5433)

    >>> populate_postgres_bid_table(
    ... con_string,
    ... "2020/01/01 00:00:00",
    ... "2020/01/02 00:00:00",
    ... "D:/nemosis_cache")
    """
    combined_bids = fetch_and_preprocess.bid_table(start_date, end_date, raw_data_cache)
    insert_data_into_postgres(connection_string, "bidding_data", combined_bids)


def populate_postgres_duid_info_table(connection_string, raw_data_cache):
    """
    Function to populate database table containing bidding data by unit. For this function to run
    the supabase url and key need to be configured as environment variables labeled
    SUPABASE_BIDDING_DASHBOARD_URL and SUPABASE_BIDDING_DASHBOARD_WRITE_KEY respectively.

    Examples
    --------
    >>> from nem_bidding_dashboard import postgress_helpers

    >>> con_string = postgress_helpers.build_connection_string(
    ... hostname='localhost',
    ... dbname='bidding_dashboard_db',
    ... username='bidding_dashboard_maintainer',
    ... password='1234abcd',
    ... port=5433)

    >>> populate_postgres_duid_info_table(
    ...  con_string,
    ... "D:/nemosis_cache",)

    Arguments:
        start_date: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS" (time always
            set to "00:00:00:)
        end_date: Ending datetime, formatted identical to start_date
        raw_data_cache: Filepath to directory for storing data that is fetched

    """
    duid_info = fetch_and_preprocess.duid_info_table(raw_data_cache)
    insert_data_into_postgres(connection_string, "duid_info", duid_info)


def populate_postgres_unit_dispatch(
    connection_string, start_date, end_date, raw_data_cache
):
    """
    Function to populate database table containing unit time series metrics. For this function to run the supabase url
    and key need to be configured as environment variables labeled SUPABASE_BIDDING_DASHBOARD_URL and
    SUPABASE_BIDDING_DASHBOARD_WRITE_KEY respectively.

    Examples
    --------
    >>> from nem_bidding_dashboard import postgress_helpers

    >>> con_string = postgress_helpers.build_connection_string(
    ... hostname='localhost',
    ... dbname='bidding_dashboard_db',
    ... username='bidding_dashboard_maintainer',
    ... password='1234abcd',
    ... port=5433)

    >>> populate_postgres_unit_dispatch(
    ... con_string,
    ... "2020/01/01 00:00:00",
    ... "2020/01/02 00:00:00",
    ... "D:/nemosis_cache")

    Arguments:
        start_date: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS" (time always
            set to "00:00:00:)
        end_date: Ending datetime, formatted identical to start_date
        raw_data_cache: Filepath to directory for storing data that is fetched
    """
    unit_time_series_metrics = fetch_and_preprocess.unit_dispatch(
        start_date, end_date, raw_data_cache
    )
    insert_data_into_postgres(
        connection_string, "unit_dispatch", unit_time_series_metrics
    )


def populate_postgres_price_bin_edges_table(connection_string):
    """
    Function to populate database table containing bin definitions for aggregating bids. For this function to run the
    supabase url and key need to be configured as environment variables labeled SUPABASE_BIDDING_DASHBOARD_URL and
    SUPABASE_BIDDING_DASHBOARD_WRITE_KEY respectively.

    Examples
    --------
    >>> from nem_bidding_dashboard import postgress_helpers

    >>> con_string = postgress_helpers.build_connection_string(
    ... hostname='localhost',
    ... dbname='bidding_dashboard_db',
    ... username='bidding_dashboard_maintainer',
    ... password='1234abcd',
    ... port=5433)

    >>> populate_postgres_price_bin_edges_table(
    ... con_string)

    Arguments:
        None
    """
    price_bins = fetch_and_preprocess.define_and_return_price_bins()
    insert_data_into_postgres(connection_string, "price_bins", price_bins)


def populate_postgres_all_tables_two_most_recent_market_days(connection_string, cache):
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
    populate_postgres_region_data(
        connection_string=connection_string,
        start_date=two_days_before_today,
        end_date=start_today,
        raw_data_cache=cache,
    )
    populate_postgres_bid_table(
        connection_string=connection_string,
        start_date=two_days_before_today,
        end_date=start_today,
        raw_data_cache=cache,
    )
    populate_postgres_duid_info_table(
        connection_string=connection_string, raw_data_cache=cache
    )
    populate_postgres_unit_dispatch(
        connection_string=connection_string,
        start_date=two_days_before_today,
        end_date=start_today,
        raw_data_cache=cache,
    )


if __name__ == "__main__":
    raw_data_cache = "D:/nemosis_cache"
    con_string = postgress_helpers.build_connection_string(
        hostname="localhost",
        dbname="bidding_dashboard_db",
        username="bidding_dashboard_maintainer",
        password="1234abcd",
        port=5433,
    )
    for m in [1]:
        start = "2020/{}/01 00:00:00".format((str(m)).zfill(2))
        end = "2020/{}/01 00:00:00".format((str(m + 1)).zfill(2))
        print(start)
        print(end)
        populate_postgres_duid_info_table(con_string, raw_data_cache)
        populate_postgres_bid_table(con_string, start, end, raw_data_cache)
        populate_postgres_region_data(con_string, start, end, raw_data_cache)
        populate_postgres_unit_dispatch(con_string, start, end, raw_data_cache)
    populate_postgres_price_bin_edges_table(con_string)
