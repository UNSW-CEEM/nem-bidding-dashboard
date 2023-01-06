import os

from nem_bidding_dashboard import postgres_helpers


def create_remote_connection_string():
    return postgres_helpers.build_connection_string(
        hostname=os.environ.get("SUPABASEADDRESS"),
        dbname="postgres",
        username="postgres",
        password=os.environ.get("SUPABASEPASSWORD"),
        port=5432,
        timeout_seconds=6000,
    )


def create_bidding_data_index():
    con_string = create_remote_connection_string()
    postgres_helpers.run_query(
        con_string, "DROP INDEX IF EXISTS bidding_data_hour_index"
    )
    postgres_helpers.run_query(
        con_string,
        "CREATE INDEX bidding_data_hour_index ON bidding_data (onhour, interval_datetime DESC);",
    )


def create_unit_dispatch_index():
    con_string = create_remote_connection_string()
    postgres_helpers.run_query(
        con_string, "DROP INDEX IF EXISTS unit_dispatch_hour_index"
    )
    postgres_helpers.run_query(
        con_string,
        "CREATE INDEX unit_dispatch_hour_index ON unit_dispatch (onhour, interval_datetime DESC);",
    )


if __name__ == "__main__":
    # create_unit_dispatch_index()
    con_string = postgres_helpers.build_connection_string(
        hostname="localhost",
        dbname="bidding_dashboard_db",
        username="bidding_dashboard_maintainer",
        password="1234abcd",
        port=5433,
    )
    postgres_helpers.run_query(con_string, "DROP FUNCTION aggregate_prices;")
