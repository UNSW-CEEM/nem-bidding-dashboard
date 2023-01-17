from nem_bidding_dashboard import postgres_helpers

region_combos = [["SA"]]  # , ["TAS", "QLD"], ["QLD", "NSW", "VIC", "SA", "TAS"]]
query_time_windows = [("2022/01/01 00:05:00", "2022/01/01 01:00:00")]
resolutions = ["hourly"]
dispatch_types = ["Generator"]
bid_types = ["adjusted"]
tech_types_combos = [[]]
duid_combos = [["AGLHAL"]]
column_names = ["AVAILABILITY"]

con_string = postgres_helpers.build_connection_string(
    hostname="localhost",
    dbname="bidding_dashboard_db",
    username="bidding_dashboard_maintainer",
    password="1234abcd",
    port=5433,
)

test_cache = "nemosis_cache"
