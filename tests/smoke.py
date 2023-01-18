from nem_bidding_dashboard import postgres_helpers, query_cached_data, query_postgres_db

region_combos = [["SA"], ["QLD", "NSW", "VIC", "SA", "TAS"]]
query_time_windows = ("2022/01/01 00:05:00", "2022/01/01 01:00:00")
resolutions = ["hourly", "5-min"]
dispatch_types = ["Generator", "Load"]
bid_types = ["adjusted", "raw"]
tech_types_combos = [[], ["Battery Discharge", "Pump Storage Charge"]]
duid_combos = [["AGLHAL"], ["FINLYSF1", "MWPS1PV1"]]
column_names = [
    "AVAILABILITY",
    "FINALMW",
    "ASBIDRAMPUPMAXAVAIL",
    "ASBIDRAMPDOWNMINAVAIL",
    "RAMPUPMAXAVAIL",
    "RAMPDOWNMINAVAIL",
]

con_string = postgres_helpers.build_connection_string(
    hostname="localhost",
    dbname="bidding_dashboard_db",
    username="bidding_dashboard_maintainer",
    password="1234abcd",
    port=5433,
)

aggregate_bids_cache = query_cached_data.aggregate_bids(
    region_combos[1],
    query_time_windows[0],
    query_time_windows[1],
    "hourly",
    "raw",
    [],
    "Generator",
    "nemosis_cache",
)
# print(aggregate_bids_cache)

aggregate_bids_postgres = query_postgres_db.aggregate_bids(
    con_string,
    region_combos[1],
    query_time_windows[0],
    query_time_windows[1],
    "hourly",
    "raw",
    [],
    "Generator",
)
# print(aggregate_bids_postgres)
