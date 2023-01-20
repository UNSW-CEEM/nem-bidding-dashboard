import random
from datetime import datetime, timedelta

from nem_bidding_dashboard import query_supabase_db


def get_test_intervals(number=100):
    start_time = datetime(year=2022, month=1, day=1, hour=0, minute=0)
    end_time = datetime(year=2022, month=12, day=1, hour=0, minute=0)
    difference = end_time - start_time
    difference_in_5_min_intervals = difference.days * 12 * 24
    random.seed(1)
    intervals = random.sample(range(1, difference_in_5_min_intervals), number)
    start_times = [start_time + timedelta(minutes=5 * i) for i in intervals]
    end_times = [t + timedelta(days=7) for t in start_times]
    start_times = [
        t.isoformat().replace("T", " ").replace("-", "/") for t in start_times
    ]
    end_times = [t.isoformat().replace("T", " ").replace("-", "/") for t in end_times]
    return start_times, end_times


def make_calls_to_aggregate_dispatch_data():
    start_times, end_times = get_test_intervals(100)
    c = 0
    for start_time, end_time in zip(start_times, end_times):
        query_supabase_db.aggregated_dispatch_data(
            "AVAILABILITY", start_time, end_time, ["NSW"], "Generator", [], "5-min"
        )
        print(c)
        c += 1


def make_calls_to_aggregate_bids():
    start_times, end_times = get_test_intervals(100)
    c = 0
    for start_time, end_time in zip(start_times, end_times):
        query_supabase_db.aggregate_bids(
            start_time,
            end_time,
            ["NSW", "QLD", "VIC", "SA", "TAS"],
            dispatch_type="Generator",
            tech_types=[],
            resolution="5-min",
            adjusted="adjusted",
        )
        print(c)
        c += 1


def make_calls_to_get_duids_and_stations():
    start_times, end_times = get_test_intervals(10000)
    c = 0
    for start_time, end_time in zip(start_times, end_times):
        query_supabase_db.stations_and_duids_in_regions_and_time_window(
            start_time,
            end_time,
            ["NSW", "QLD", "VIC", "SA", "TAS"],
            dispatch_type="Generator",
            tech_types=[],
        )
        print(c)
        c += 1


def make_mutiple_calls():
    start_times, end_times = get_test_intervals(100)
    c = 0
    for start_time, end_time in zip(start_times, end_times):
        query_supabase_db.stations_and_duids_in_regions_and_time_window(
            start_time,
            end_time,
            ["NSW", "QLD", "VIC", "SA", "TAS"],
            dispatch_type="Generator",
            tech_types=[],
        )
        query_supabase_db.aggregate_bids(
            start_time,
            end_time,
            ["NSW", "QLD", "VIC", "SA", "TAS"],
            dispatch_type="Generator",
            tech_types=[],
            resolution="hourly",
            adjusted="adjusted",
        )
        query_supabase_db.aggregated_dispatch_data(
            "AVAILABILITY",
            start_time,
            end_time,
            ["NSW", "QLD", "VIC", "SA", "TAS"],
            "Generator",
            [],
            "hourly",
        )
        print(c)
        c += 1


if __name__ == "__main__":
    make_mutiple_calls()
