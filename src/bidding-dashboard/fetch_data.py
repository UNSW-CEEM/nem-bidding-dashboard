from curses import raw
from nemosis import dynamic_data_compiler, static_table
import pandas as pd

"""
Function to fetch electricity demand data using NEMOSIS for use in 
plot_region_demand. Simply returns fetched dataframe containing required data at 
the moment, may be updated if a different method is used to get or store data. 
Also generates feather files for each month of data that is collected. 
Arguments:
    start_date: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS" (time always 
        set to "00:00:00:)
    end_date: Ending datetime, formatted identical to start_date
    raw_data_cache: Filepath to directory for storing data that is fetched
Returns:
    pd dataframe containing REGIONID, SETTLEMENTDATE and TOTALDEMAND columns 
    from the DISPATCHREGIONSUM table from AEMO, covering the specified time 
    period.
"""
def get_region_demand_data(
    start_time: str, 
    end_time: str, 
    raw_data_cache: str
):
    region_data = dynamic_data_compiler(
        start_time, 
        end_time, 
        "DISPATCHREGIONSUM", 
        raw_data_cache,
        keep_csv=True, 
        select_columns=["REGIONID", "SETTLEMENTDATE", "TOTALDEMAND", "INTERVENTION"]
    )
    region_data = region_data.loc[region_data["INTERVENTION"] == 0]
    region_data = region_data.filter(["REGIONID", "SETTLEMENTDATE", "TOTALDEMAND"])
    return region_data


"""
TODO: update this
Function to fetch electricity demand data using NEMOSIS for use in 
plot_region_demand. Simply returns fetched dataframe containing required data at 
the moment, may be updated if a different method is used to get or store data. 
Also generates feather files for each month of data that is collected. 
Arguments:
    start_date: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS" (time always 
        set to "00:00:00:)
    end_date: Ending datetime, formatted identical to start_date
    raw_data_cache: Filepath to directory for storing data that is fetched
Returns:
    pd dataframe containing REGIONID, SETTLEMENTDATE and TOTALDEMAND columns 
    from the DISPATCHREGIONSUM table from AEMO, covering the specified time 
    period.
"""
def get_duid_availability_data(
    start_time: str, 
    end_time: str, 
    raw_data_cache: str
):
    availability_data = dynamic_data_compiler(
        start_time, 
        end_time, 
        "DISPATCHLOAD", 
        raw_data_cache,
        keep_csv=True, 
        select_columns=["SETTLEMENTDATE", "DUID", "AVAILABILITY"]
    )
    #availability_data = availability_data.loc[availability_data["AVAILABILITY"] != 0]
    return availability_data

def get_duid_data(raw_data_cache: str):
    duid_data = static_table(
        "Generators and Scheduled Loads", 
        raw_data_cache, 
        select_columns=[
            "Region", "Fuel Source - Primary", 
            "DUID", "Dispatch Type"
        ]
    )
    duid_data = duid_data.loc[duid_data["Dispatch Type"] == "Generator"]
    duid_data = duid_data.loc[duid_data["Fuel Source - Primary"] != ""]
    duid_data = duid_data.filter(["Region", "Fuel Source - Primary", "DUID"])
    return duid_data

if __name__ == "__main__":
    raw_data_cache = "/home/pat/bidding-dashboard/src/bidding-dashboard/nemosis_data_cache/"
    get_duid_data(raw_data_cache)
    #get_region_demand_data("2019/01/23 00:00:00", "2019/01/28 00:00:00", raw_data_cache)
