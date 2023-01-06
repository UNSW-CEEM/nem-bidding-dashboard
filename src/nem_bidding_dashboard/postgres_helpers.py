import math

import numpy as np
import pandas as pd
import psycopg
from psycopg.rows import dict_row


def build_connection_string(
    hostname, dbname, username, password, port, timeout_seconds=None
):
    """
    Creates a properly formatted connection string for connecting to a PostgresSQL database.

    Examples:

    >>> from nem_bidding_dashboard import postgres_helpers

    >>> con_string = postgres_helpers.build_connection_string(
    ... hostname='localhost',
    ... dbname='bidding_dashboard_db',
    ... username='bidding_dashboard_maintainer',
    ... password='1234abcd',
    ... port=5433)

    Args:
        hostname: str where the database is hosted
        dbname: str name of database to connect to
        username: str name of user that has read and write permission on database
        password: str password of user
        port: int or str port that you can connect to the database on
        timeout_seconds: int number of seconds before timeout

    Returns: str in formatted required for connecting to postgres database.

    """

    connection_string = "host={hostname} dbname={dbname} user={username} password={password} port={port}"
    connection_string = connection_string.format(
        hostname=hostname,
        dbname=dbname,
        username=username,
        password=password,
        port=port,
    )

    if timeout_seconds is not None:
        timeout_setting = " options='-c statement_timeout={time_milliseconds}'".format(
            time_milliseconds=timeout_seconds * 1000
        )
        connection_string += timeout_setting

    return connection_string


_list_of_tables = [
    "bidding_data",
    "demand_data",
    "duid_info",
    "price_bins",
    "unit_dispatch",
]

_list_of_functions = [
    "distinct_unit_types",
    "aggregate_bids_v2",
    "aggregate_dispatch_data",
    "aggregate_dispatch_data_duids",
    "get_bids_by_unit",
    "get_duids_for_stations",
    "get_duids_and_stations",
    "aggregate_prices",
]


def drop_tables_and_functions(connection_string):
    """
    Drop all the tables and functions created by build_postgres.create_db_tables_and_functions. Intended for  to  help
    developement when testing the creation of tables and functions.

    Examples:

    >>> con_string = build_connection_string(
    ... hostname='localhost',
    ... dbname='bidding_dashboard_db',
    ... username='bidding_dashboard_maintainer',
    ... password='1234abcd',
    ... port=5433)

    >>> drop_tables_and_functions(con_string)

    Args:
        connection_string: str for connecting to PostgresSQL database, the function :py:func:`nem_bidding_dashboard.postgres_helpers.build_connection_string`
            can be used to build a properly formated connection string, or alternative any string that matches the
            format allowed by `PostgresSQL <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING>`_
            can be used

    """
    with psycopg.connect(connection_string) as conn:
        with conn.cursor() as cur:
            for table in _list_of_tables:
                cur.execute("DROP TABLE IF EXISTS {}".format(table))
            for function in _list_of_functions:
                cur.execute("DROP FUNCTION IF EXISTS {}".format(function))
            conn.commit()


def insert_data_into_postgres(connection_string, table_name, data):
    """Insert data into the postgres database.

    Arguments:
        connection_string: str for connecting to PostgresSQL database, the function :py:func:`nem_bidding_dashboard.postgres_helpers.build_connection_string`
            can be used to build a properly formated connection string, or alternative any string that matches the
            format allowed by `PostgresSQL <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING>`_
            can be used
        table_name: str which is the name of the table in the postgres database
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


def run_query_return_dataframe(connection_string, query):
    """
    Sends an arbitary query to the specified postgres database and return the result as a pd.DataFrame. Should only be
    used for queries that return a result as a table. Handles opening and closing connection to database.

    Examples:

    >>> from nem_bidding_dashboard import postgres_helpers

    >>> con_string = postgres_helpers.build_connection_string(
    ... hostname='localhost',
    ... dbname='bidding_dashboard_db',
    ... username='bidding_dashboard_maintainer',
    ... password='1234abcd',
    ... port=5433)

    >>> run_query_return_dataframe(con_string, "select * from duid_info limit 10;")
           DUID REGION  ...           UNIT TYPE                 STATION NAME
    0   ADPBA1G     SA  ...   Battery Discharge  Adelaide Desalination Plant
    1   ADPBA1L     SA  ...      Battery Charge  Adelaide Desalination Plant
    2    ADPMH1     SA  ...  Run of River Hydro  Adelaide Desalination Plant
    3    ADPPV3     SA  ...               Solar  Adelaide Desalination Plant
    4    ADPPV2     SA  ...               Solar  Adelaide Desalination Plant
    5    ADPPV1     SA  ...               Solar  Adelaide Desalination Plant
    6  AGLSITA1    NSW  ...              Engine              Agl Kemps Creek
    7   ANGAST1     SA  ...              Engine       Angaston Power Station
    8     APPIN    NSW  ...              Engine            Appin Power Plant
    9     ARWF1    VIC  ...                Wind             Ararat Wind Farm
    <BLANKLINE>
    [10 rows x 7 columns]

    Args:
        connection_string: str for connecting to PostgresSQL database, the function :py:func:`nem_bidding_dashboard.postgres_helpers.build_connection_string`
            can be used to build a properly formated connection string, or alternative any string that matches the
            format allowed by `PostgresSQL <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING>`_
            can be used
        query: str which is postgres select query

    Returns:
        pd.DataFrame column as per the query provided

    """
    with psycopg.connect(connection_string) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query)
            data = cur.fetchall()
    data = pd.DataFrame(data)
    data.columns = data.columns.str.upper()
    return data


def run_query(connection_string, query, autocommit=False):
    """
    Run a genric query in the database which isn't inserting or retrieving data. For example, creating and dropping
    tables, functions and indexes.

    Examples:

    >>> import os

    >>> from nem_bidding_dashboard import postgres_helpers

    >>> con_string = postgres_helpers.build_connection_string(
    ... hostname=os.environ.get("SUPABASEADDRESS"),
    ... dbname='postgres',
    ... username='postgres',
    ... password=os.environ.get("SUPABASEPASSWORD"),
    ... port=5432
    ... timeout_seconds=6000)

    >>> run_query_return_dataframe(con_string, "CREATE INDEX bidding_data_hour_index ON bidding_data (onhour, interval_datetime DESC);")

    >>> run_query_return_dataframe(con_string, "CREATE INDEX bidding_data_hour_index ON bidding_data (onhour, interval_datetime DESC);")

    Args:
        connection_string: str for connecting to PostgresSQL database, the function :py:func:`nem_bidding_dashboard.postgres_helpers.build_connection_string`
            can be used to build a properly formated connection string, or alternative any string that matches the
            format allowed by `PostgresSQL <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING>`_
            can be used
        query: str query to run in supabase database
        autocommit: boolean, set to True to run queries that must be run outside a transaction such as vaccum

    """
    with psycopg.connect(connection_string, autocommit=autocommit) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
        conn.commit()
