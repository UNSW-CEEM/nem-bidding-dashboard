import psycopg


def build_connection_string(hostname, dbname, username, password, port):
    connection_string = "host={hostname} dbname={dbname} user={username} password={password} port={port}"
    connection_string = connection_string.format(
        hostname=hostname,
        dbname=dbname,
        username=username,
        password=password,
        port=port,
    )
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
    "get_bids_by_unit",
    "get_duids_for_stations",
    "get_duids_and_stations",
    "aggregate_prices",
]


def drop_tables_and_functions(connection_string):
    """
    Examples
    --------
    >>> con_string = build_connection_string(
    ... hostname='localhost',
    ... dbname='bidding_dashboard_db',
    ... username='bidding_dashboard_maintainer',
    ... password='1234abcd',
    ... port=5433)
    >>> drop_tables_and_functions(con_string)

    """
    with psycopg.connect(connection_string) as conn:
        with conn.cursor() as cur:
            for table in _list_of_tables:
                cur.execute("DROP TABLE IF EXISTS {}".format(table))
            for function in _list_of_functions:
                cur.execute("DROP FUNCTION IF EXISTS {}".format(function))
            conn.commit()
