from nem_bidding_dashboard import postgres_helpers

region_combos = [["SA"], ["QLD", "NSW", "VIC", "SA", "TAS"], []]
query_time_windows = [("2022/01/01 00:05:00", "2022/01/01 01:00:00")]
resolutions = ["hourly", "5-min"]
dispatch_types = ["Generator", "Load"]
bid_types = ["adjusted", "raw"]
tech_types_combos = [[], ["Battery Discharge", "Pump Storage Charge"]]
duid_combos = [["AGLHAL"], ["FINLYSF1", "MWPS1PV1"], ["duid_that_shouldnt_exist_18462"]]
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

test_cache = "tests/nemosis_cache"
