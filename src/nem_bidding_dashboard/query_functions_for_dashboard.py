import defaults

if defaults.data_source == "local":
    import query_postgres_db

    def aggregate_bids(
        start_time, end_time, regions, dispatch_type, tech_types, resolution, adjusted
    ):
        return query_postgres_db.aggregate_bids(
            defaults.con_string,
            start_time,
            end_time,
            regions,
            dispatch_type,
            tech_types,
            resolution,
            adjusted,
        )

    def aggregated_dispatch_data(
        column_name,
        start_time,
        end_time,
        regions,
        dispatch_type,
        tech_types,
        resolution,
    ):
        return query_postgres_db.aggregated_dispatch_data(
            defaults.con_string,
            column_name,
            start_time,
            end_time,
            regions,
            dispatch_type,
            tech_types,
            resolution,
        )

    def aggregated_dispatch_data_by_duids(
        column_name, start_time, end_time, duids, resolution
    ):
        return query_postgres_db.aggregated_dispatch_data_by_duids(
            defaults.con_string, column_name, start_time, end_time, duids, resolution
        )

    def aggregated_vwap(start_time, end_time, regions):
        return query_postgres_db.aggregated_vwap(
            defaults.con_string, start_time, end_time, regions
        )

    def duid_bids(start_time, end_time, duids, resolution, adjusted):
        return query_postgres_db.duid_bids(
            defaults.con_string, start_time, end_time, duids, resolution, adjusted
        )

    def region_demand(start_time, end_time, regions):
        return query_postgres_db.region_demand(
            defaults.con_string, start_time, end_time, regions
        )

    def stations_and_duids_in_regions_and_time_window(
        start_time, end_time, regions, dispatch_type, tech_types
    ):
        return query_postgres_db.stations_and_duids_in_regions_and_time_window(
            defaults.con_string,
            start_time,
            end_time,
            regions,
            dispatch_type,
            tech_types,
        )

elif defaults.data_source == "remote":
    from query_supabase_db import (
        aggregate_bids,
        aggregated_dispatch_data,
        aggregated_dispatch_data_by_duids,
        aggregated_vwap,
        duid_bids,
        region_demand,
        stations_and_duids_in_regions_and_time_window,
    )

    aggregate_bids
    aggregated_dispatch_data
    aggregated_dispatch_data_by_duids
    aggregated_vwap
    duid_bids
    region_demand
    stations_and_duids_in_regions_and_time_window

else:
    raise ValueError(
        "Invalid value for defaults.data_source, should be 'local' or 'remote'."
    )
