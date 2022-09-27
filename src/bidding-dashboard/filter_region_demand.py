from nemosis import dynamic_data_compiler
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
) -> pd.Dataframe:
    region_data = dynamic_data_compiler(
        start_time, 
        end_time, 
        "DISPATCHREGIONSUM", 
        raw_data_cache,
        keep_csv=False, 
        select_columns=["REGIONID", "SETTLEMENTDATE", "TOTALDEMAND"]
    )
    return region_data
