# Database Guide
This section provides a guide to the PostgresSQL database used by nem_bidding_dashboard to store and aggregate data.
It describes the database tables and function created by the function 
py:func:build_postgres_db.create_db_tables_and_functions

## Database tables

### bidding_data
The bid volume and price pairs used for each dispatch interval and duid (dispatch unit).

Columns:
- interval_datetime timestamp
- duid text
- bidband int4
- bidprice float4 ($/MWh)
- bidvolume float4 (MW)
- bidvolumeadjusted float4 

Primary Key: interval_datetime, duid, bidband

Notes: 
- When populated with data bidbands with zero volume are excluded to reduce storage space.
- The column bidvolumeadjusted is the bid volume adjusted down such that the total volume bid by a duid does not exceed
the availability reported by AEMO. See the table unit_dispatch for a description of the availability value. The 
adjustment of the bid volume proceeds from top down, with the most expensive binds bands being reduced first.
- Notes: Columns are taken from or derived from data in AEMO mms tables DISPATCHLOAD, BIDPEROFFER_D, BIDDAYOFFER_D.

### demand_data
The demand and price data for each dispatch interval on a regional basis.

Columns:
- settlementdate timestamp
- regionid text
- totaldemand float8 (MW)
- rrp float8 ($/MWh)

Primary Key: settlementdate, regionid

### duid_info
Data on duids, columns from the AEMO NEM Registration and Exemption List work book, Generators and Scheduled Loads tab, 
except for the "unit type" column which is derived using the logic set out in the function 
:py:func:preprocessing.tech_namer_by_row

Columns:
- duid text
- region text,
- "fuel source - descriptor" text,
- "dispatch type" text,
- "technology type - descriptor" text,
- "unit type" text,
- "station name" text

Primary Key: duid

### price_bins 
Price bins used when aggregating bidding data.

Columns:
- bin_name text eg. [50, 100)
- lower_edge float8 lower limit of bin inclusive
- upper_edge float8 upper limit of bin no inclusive

### unit_dispatch
A collection of various dispatch associated values on a duid and dispatch interval basis. 

Columns:
- interval_datetime timestamp
- duid text
- availability float4
  (unit availability in MW, presumed to be the lesser of the unit bid availability (MAXAVAIL column) and forecast 
  availability for variable renewables, so for non-variable renewables should be equal to MAXAVAIL)
- totalcleared float4 (The dispatch target for unit at the end of interval)
- finalmw float4 (The SCADA generation/consumption value recorded for this unit at the end of the interval)
- asbidrampupmaxavail float4 (The upper limit the unit could ramp to based on its as bid ramp rate)
- asbidrampdownminavail float4 (The lower limit the unit could ramp to based on its as bid ramp rate)
- rampupmaxavail float4 (The upper limit the unit could ramp to based on the lesser of its as bid or telemetered ramp 
  rates)
- rampdownminavail float4 (The lower limit the unit could ramp to based on the lesser of its as bid or telemetered ramp 
  rates)
- pasaavailability float4 (The technical maximum availability of unit given 24h notice)
- maxavail float4 (The as bid availability of the unit)

Notes: Columns are taken or derived from data in AEMO mms tables DISPATCHLOAD and BIDPEROFFER_D.

Primary Key: interval_datetime, duid

## Database functions
These are SQL functions that are defined on the database, they are primarily used for filtering and aggregating
data.

### distinct_unit_types()
Returns the unique values in the "unit type" column in the table duid_info. 

Args: None

### aggregate_bids_v2(regions text[], start_datetime timestamp, end_datetime timestamp, resolution text, dispatch_type text, adjusted text, tech_types text[])
Filters and aggregates the data in the table bidding_data. Filters bids by the given regions, start_datetime, 
end_datetime, resolution, dispatch_type, and tech_type. Bids are then classified into the bins defined in table 
price_bins. Finally, the data is aggregated by interval_datatime and price_bin by summing the bid_volume. If the 
adjusted argument is set to "adjusted" then the bid volume is taken from column bidvolumeadjusted, otherwise the column 
bidvolume is used.

Examples:



Args: 
- regions: array of text values, regions to return bids from e.g. QLD, NSW, VIC, SA, TAS
- start_datetime: timestamp, select bids with an interval_datetime equal to or greater than this value
- end_datetime: timestamp, select bids with an interval_datetime equal to or less than this value
- resolution: text, if the value "hourly" is provided than only bids with an interval datetime on the hour selected.
Otherwise, data for all intervals is returned.
- dispatch_type: text, should be Generator or Load, used to filter bids
- adjusted: text, If "adjusted" then the bid volume is taken from column bidvolumeadjusted, otherwise the column 
bidvolume is used.
- tech_type: array of text values. Used to filter values by "unit type" column in duid_info. If an empty array is 
provided filtering by this value is not performed.
  