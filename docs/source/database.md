
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
:py:func:`nem_bidding_dashboard.preprocessing.tech_namer_by_row`

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
- upper_edge float8 upper limit of bin not inclusive

### unit_dispatch
A collection of various dispatch associated values on a duid and dispatch interval basis. 

Columns:
- interval_datetime timestamp
- duid text
- availability float4
  (unit availability in MW, presumed to be the lesser of the unit bid availability (MAXAVAIL column)) and forecast 
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

Returns: TABLE ("unit type" text)

### aggregate_bids_v2(regions text[], start_timetime timestamp, end_timetime timestamp, resolution text, dispatch_type text, adjusted text, tech_types text[])
Filters and aggregates the data in the table bidding_data. Filters bids by the given regions, start_timetime, 
end_timetime, resolution, dispatch_type, and tech_type. Bids are then classified into the bins defined in table 
price_bins. Finally, the data is aggregated by interval_datatime and price_bin by summing the bid_volume. If the 
adjusted argument is set to "adjusted" then the bid volume is taken from column bidvolumeadjusted, otherwise the column 
bidvolume is used.

Examples:

```
SELECT * FROM aggregate_bids_v2(
             '{"SA", "TAS"}',
             (timestamp '2020/01/01 00:00:00'),
             (timestamp '2020/01/01 2:00:00'),
             'hourly',
             'Generator',
             'adjusted',
             '{}'
             )
```

Args: 
- regions: array of text values, regions to return bids from e.g. QLD, NSW, VIC, SA, TAS
- start_timetime: timestamp, select bids with an interval_datetime equal to or greater than this value
- end_timetime: timestamp, select bids with an interval_datetime equal to or less than this value
- resolution: text, if the value "hourly" is provided than only bids with an interval datetime on the hour selected.
Otherwise, data for all intervals is returned.
- dispatch_type: text, should be Generator or Load, used to filter bids
- adjusted: text, If "adjusted" then the bid volume is taken from column bidvolumeadjusted, otherwise the column 
bidvolume is used.
- tech_type: array of text values. Used to filter values by "unit type" column in duid_info. If an empty array is 
provided filtering by this value is not performed.

Returns: TABLE (interval_datetime timestamp, bin_name text, bidvolume float4)

### aggregate_dispatch_data(regions text[], start_timetime timestamp, end_timetime timestamp, resolution text, dispatch_type text, tech_types text[])
Filters and aggregates the data in the table unit_dispatch. Filters data by the given regions, start_timetime, 
end_timetime, resolution, dispatch_type, and tech_type. Aggregation is on interval datetime by summing. Before summing
the column rampupmaxavail is capped at the unit's availability so that the aggregate ramping capability is not
over represented. Similarly, asbidrampupmaxavail is capped at the unit maxavail value, and the columns 
asbidrampdownminavail and rampdownminavail are limited to greater than or equal to zero.


Examples:

```
SELECT * FROM aggregate_dispatch_data(
             '{"SA", "TAS"}',
             (timestamp '2020/01/01 00:00:00'),
             (timestamp '2020/01/01 2:00:00'),
             'hourly',
             'Generator',
             '{}'
             )
```

Args: 
- regions: array of text values, regions to return bids from e.g. QLD, NSW, VIC, SA, TAS
- start_timetime: timestamp, select bids with an interval_datetime equal to or greater than this value
- end_timetime: timestamp, select bids with an interval_datetime equal to or less than this value
- resolution: text, if the value "hourly" is provided than only bids with an interval datetime on the hour selected.
Otherwise, data for all intervals is returned.
- dispatch_type: text, should be Generator or Load, used to filter bids
- tech_type: array of text values. Used to filter values by "unit type" column in duid_info. If an empty array is 
provided filtering by this value is not performed.

Returns: TABLE (interval_datetime timestamp, availability float4, totalcleared float4, finalmw float4,
asbidrampupmaxavail float4, asbidrampdownminavail float4, rampupmaxavail float4, rampdownminavail float4, 
pasaavailability float4, maxavail float4)

### get_bids_by_unit_v2(duids text[], start_timetime timestamp, end_timetime timestamp, resolution text, adjusted text)
Filters the data in the table bidding_data. Filters bids by duid, start_timetime, end_timetime, and 
resolution. If the adjusted argument is set to "adjusted" then the bid volume is taken from column bidvolumeadjusted, 
otherwise the column bidvolume is used.

Examples:

```
SELECT * FROM aggregate_bids_v2(
             '{"AGLHAL", "HDWF1"}',
             (timestamp '2020/01/01 00:00:00'),
             (timestamp '2020/01/01 2:00:00'),
             'hourly',
             'adjusted',
             '{}'
             )
```

Args: 
- duids: array of text values, duids to return bids from.
- start_timetime: timestamp, select bids with an interval_datetime equal to or greater than this value
- end_timetime: timestamp, select bids with an interval_datetime equal to or less than this value
- resolution: text, if the value "hourly" is provided than only bids with an interval datetime on the hour selected.
Otherwise, data for all intervals is returned.
- adjusted: text, If "adjusted" then the bid volume is taken from column bidvolumeadjusted, otherwise the column 
bidvolume is used.

Returns: TABLE (interval_datetime timestamp, duid text, bidband int, bidvolume float4, bidprice float4)

### get_duids_for_stations(stations text[])
Filters list of duids in duid_info based on a list of station names. Used to update filters on dashboard when the user
selects a station name.

Examples:
```SELECT * FROM get_duids_for_stations('{"Adelaide Desalination Plant", "Yawong Wind Farm"}')```

Args:
- stations: array of text values, stations to get duids of.

Returns: TABLE (duid text)

### get_duids_and_stations(regions text[], start_timetime timestamp, end_timetime timestamp, dispatch_type text, tech_types text[])
Finds the set of stations and duids with bids in a given set of regions, of a given dispatch_type, tech_type, and
between a given start_timetime and end_timetime. Used to update duid and station name filters on dashboard.

```
SELECT * FROM get_duids_and_staions_in_regions_and_time_window_v2(
  '{"NSW"}', 
  (timestamp '2020/01/21 00:00:00'), 
  (timestamp '2020/01/21 02:00:00'), 
  'Generator', 
  '{"OCGT"}')
```

Args: 
- regions: array of text values, regions to return bids from e.g. QLD, NSW, VIC, SA, TAS
- start_timetime: timestamp, look for bids with an interval_datetime equal to or greater than this value
- end_timetime: timestamp, look for bids with an interval_datetime equal to or less than this value
- dispatch_type: text, should be Generator or Load, used to filter bids
- tech_type: array of text values. Used to filter values by "unit type" column in duid_info. If an empty array is 
provided filtering by this value is not performed.

Returns: TABLE (duid text, "station name" text)


### aggregate_prices(regions text[], start_timetime timestamp, end_timetime timestamp)
Filters and aggregates regional energy prices. Filters by regions and between start_timetime and end_timetime. 
Aggregation is volume weighted.

```
SELECT * FROM aggregate_prices(
  '{"NSW", "VIC"}', 
  (timestamp '2020/03/21 00:00:00'), 
  (timestamp '2020/03/21 02:00:00'))
```

Args: 
- regions: array of text values, regions to return bids from e.g. QLD, NSW, VIC, SA, TAS
- start_timetime: timestamp, look for bids with an interval_datetime equal to or greater than this value
- end_timetime: timestamp, look for bids with an interval_datetime equal to or less than this value

Returns: TABLE (settlementdate timestamp, price float)