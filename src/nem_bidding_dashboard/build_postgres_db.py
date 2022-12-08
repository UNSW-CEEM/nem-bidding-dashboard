import psycopg

_create_bidding_data_table = """
    CREATE TABLE bidding_data (
        interval_datetime timestamp,
        duid text,
        bidband int4,
        bidprice float4,
        bidvolume float4,
        bidvolumeadjusted float4,
        PRIMARY KEY(interval_datetime, duid, bidband)
    );
"""

_create_demand_data_table = """
    CREATE TABLE demand_data (
        settlementdate timestamp,
        regionid text,
        totaldemand float8,
        rrp float8,
        PRIMARY KEY(settlementdate, regionid)
    );
"""

_create_duid_info_table = """
    CREATE TABLE duid_info (
        duid text PRIMARY KEY,
        region text,
        "fuel source - descriptor" text,
        "dispatch type" text,
        "technology type - descriptor" text,
        "unit type" text,
        "station name" text
    );
"""

_create_price_bins_table = """
    CREATE TABLE price_bins (
        bin_name text PRIMARY KEY,
        lower_edge float8,
        upper_edge float8
    );
"""

_create_unit_dispatch_table = """
    CREATE TABLE unit_dispatch (
        interval_datetime timestamp,
        duid text,
        availability float4,
        totalcleared float4,
        finalmw float4,
        asbidrampupmaxavail float4,
        asbidrampdownminavail float4,
        rampupmaxavail float4,
        rampdownminavail float4,
        pasaavailability float4,
        maxavail float4,
        PRIMARY KEY (interval_datetime, duid)
    );
"""

_create_distinct_unit_types_function = """
    CREATE OR REPLACE FUNCTION distinct_unit_types()
      RETURNS TABLE ("unit type" text)
      LANGUAGE plpgsql AS
    $func$
    BEGIN
      RETURN QUERY SELECT DISTINCT t."unit type" from duid_info t;
    END
    $func$;
    """


_create_aggregate_bids_function = """
    CREATE OR REPLACE FUNCTION aggregate_bids_v2(regions text[], start_timetime timestamp, end_timetime timestamp,
                                                 resolution text, dispatch_type text, adjusted text, tech_types text[])
      RETURNS TABLE (interval_datetime timestamp, bin_name text, bidvolume float4)
      LANGUAGE plpgsql AS
    $func$
    BEGIN

      DROP TABLE IF EXISTS filtered_regions;
      DROP TABLE IF EXISTS filtered_duid_info;
      DROP TABLE IF EXISTS time_filtered_bids;
      DROP TABLE IF EXISTS correct_volume_column;
      DROP TABLE IF EXISTS region_filtered_bids;
      DROP TABLE IF EXISTS bids_with_bins;

      IF array_length(tech_types, 1) > 0 THEN
        CREATE TEMP TABLE filtered_duid_info as
        SELECT * FROM duid_info d WHERE d."unit type" = ANY(tech_types);
      ELSE
        CREATE TEMP TABLE filtered_duid_info as
        SELECT * FROM duid_info d;
      END IF;

      CREATE TEMP TABLE filtered_regions AS
      SELECT * FROM filtered_duid_info WHERE region = ANY(regions) and "dispatch type" = dispatch_type;

      IF resolution = 'hourly' THEN
        CREATE TEMP TABLE time_filtered_bids as
        SELECT * FROM bidding_data b WHERE EXTRACT(MINUTE FROM b.interval_datetime) = 0
                                       AND b.interval_datetime between start_timetime and end_timetime;
      ELSE
       CREATE TEMP TABLE time_filtered_bids as
        SELECT * FROM bidding_data b WHERE b.interval_datetime between start_timetime and end_timetime;
      END IF;

      IF adjusted = 'adjusted' THEN
        CREATE TEMP TABLE correct_volume_column as
        SELECT t.interval_datetime, t.duid, t.bidband, t.bidvolumeadjusted as bidvolume, t.bidprice
          FROM time_filtered_bids t;
      ELSE
        CREATE TEMP TABLE correct_volume_column as
        SELECT t.interval_datetime, t.duid, t.bidband, t.bidvolume, t.bidprice FROM time_filtered_bids t;
      END IF;

      CREATE TEMP TABLE region_filtered_bids as
      SELECT * FROM correct_volume_column WHERE duid IN (SELECT duid FROM filtered_regions);

      CREATE TEMP TABLE bids_with_bins as
      SELECT * FROM region_filtered_bids a LEFT JOIN price_bins b ON a.bidprice >= b.lower_edge
                                                                 AND a.bidprice < b.upper_edge;

      RETURN QUERY SELECT b.interval_datetime, b.bin_name, SUM(b.bidvolume) as bidvolume
                     FROM bids_with_bins b group by b.interval_datetime, b.bin_name;

    END
    $func$;
    """

_create_aggregate_dispatch_data_function = """
CREATE OR REPLACE FUNCTION aggregate_dispatch_data(regions text[], start_timetime timestamp, end_timetime timestamp,
                                                   resolution text, dispatch_type text, tech_types text[])
  RETURNS TABLE (interval_datetime timestamp, availability float4, totalcleared float4, finalmw float4,
                 asbidrampupmaxavail float4, asbidrampdownminavail float4, rampupmaxavail float4,
                 rampdownminavail float4, pasaavailability float4, maxavail float4)
  LANGUAGE plpgsql AS
$func$
BEGIN

  DROP TABLE IF EXISTS filtered_duid_info;
  DROP TABLE IF EXISTS filtered_regions;
  DROP TABLE IF EXISTS time_filtered_dispatch;
  DROP TABLE IF EXISTS region_filtered_dispatch;

  IF array_length(tech_types, 1) > 0 THEN
    CREATE TEMP TABLE filtered_duid_info as
    SELECT * FROM duid_info d WHERE d."unit type" = ANY(tech_types);
  ELSE
    CREATE TEMP TABLE filtered_duid_info as
    SELECT * FROM duid_info d;
  END IF;

  CREATE TEMP TABLE filtered_regions AS
  SELECT * FROM filtered_duid_info WHERE region = ANY(regions) and "dispatch type" = dispatch_type;

  IF resolution = 'hourly' THEN
    CREATE TEMP TABLE time_filtered_dispatch as
    SELECT * FROM unit_dispatch d WHERE EXTRACT(MINUTE FROM d.interval_datetime) = 0
                                    AND d.interval_datetime between start_timetime and end_timetime;
  ELSE
   CREATE TEMP TABLE time_filtered_dispatch as
    SELECT * FROM unit_dispatch d WHERE d.interval_datetime between start_timetime and end_timetime;
  END IF;

  CREATE TEMP TABLE region_filtered_dispatch as
  SELECT * FROM time_filtered_dispatch WHERE duid IN (SELECT duid FROM filtered_regions);

  UPDATE region_filtered_dispatch d SET asbidrampupmaxavail = d.maxavail WHERE d.asbidrampupmaxavail > d.maxavail;
  UPDATE region_filtered_dispatch d SET asbidrampdownminavail = 0  WHERE d.asbidrampdownminavail < 0;

  UPDATE region_filtered_dispatch d SET rampupmaxavail = d.availability WHERE d.rampupmaxavail > d.availability;
  UPDATE region_filtered_dispatch d SET rampdownminavail = 0 WHERE d.rampdownminavail < 0;

  RETURN QUERY SELECT d.interval_datetime,
                      SUM(d.availability) as availability,
                      SUM(d.totalcleared) as totalcleared,
                      SUM(d.finalmw) as finalmw,
                      SUM(d.asbidrampupmaxavail) as asbidrampupmaxavail,
                      SUM(d.asbidrampdownminavail) as asbidrampdownminavail,
                      SUM(d.rampupmaxavail) as rampupmaxavail,
                      SUM(d.rampdownminavail) as rampdownminavail,
                      SUM(d.pasaavailability) as pasaavailability,
                      SUM(d.maxavail) as maxavail
                 FROM region_filtered_dispatch d group by d.interval_datetime;

END
$func$;
"""

_create_get_bids_by_unit_function = """
    CREATE OR REPLACE FUNCTION get_bids_by_unit_v2(duids text[], start_timetime timestamp, end_timetime timestamp, resolution text, adjusted text)
      RETURNS TABLE (interval_datetime timestamp, duid text, bidband int, bidvolume float4, bidprice float4)
      LANGUAGE plpgsql AS
    $func$
    BEGIN
    
      DROP TABLE IF EXISTS time_filtered_bids;
      DROP TABLE IF EXISTS correct_volume_column;
    
      IF resolution = 'hourly' THEN
        CREATE TEMP TABLE time_filtered_bids as
        SELECT * FROM bidding_data b WHERE EXTRACT(MINUTE FROM b.interval_datetime) = 0 AND b.interval_datetime between
        start_timetime and end_timetime;
      ELSE
       CREATE TEMP TABLE time_filtered_bids as
        SELECT * FROM bidding_data b WHERE b.interval_datetime between start_timetime and end_timetime;
      END IF;
    
      IF adjusted = 'adjusted' THEN
        CREATE TEMP TABLE correct_volume_column as
        SELECT t.interval_datetime, t.duid, t.bidband, t.bidvolumeadjusted as bidvolume, t.bidprice
          FROM time_filtered_bids t;
      ELSE
        CREATE TEMP TABLE correct_volume_column as
        SELECT t.interval_datetime, t.duid, t.bidband, t.bidvolume, t.bidprice FROM time_filtered_bids t;
      END IF;
    
      RETURN QUERY SELECT b.interval_datetime, b.duid, b.bidband, b.bidvolume, b.bidprice FROM correct_volume_column b WHERE b.duid = ANY(duids);
      
    END
    $func$;
"""

_create_aggregate_dispatch_data_duids_function = """
    CREATE OR REPLACE FUNCTION aggregate_dispatch_data_duids(duids text[], start_timetime timestamp,
                                                             end_timetime timestamp, resolution text)
      RETURNS TABLE (interval_datetime timestamp, availability float4, totalcleared float4, finalmw float4,
                     asbidrampupmaxavail float4, asbidrampdownminavail float4, rampupmaxavail float4,
                     rampdownminavail float4, pasaavailability float4, maxavail float4)
      LANGUAGE plpgsql AS
    $func$
    BEGIN

      DROP TABLE IF EXISTS time_filtered_dispatch;
      DROP TABLE IF EXISTS duids_filtered_dispatch;

      IF resolution = 'hourly' THEN
        CREATE TEMP TABLE time_filtered_dispatch as
        SELECT * FROM unit_dispatch d WHERE EXTRACT(MINUTE FROM d.interval_datetime) = 0
                                        AND d.interval_datetime between start_timetime and end_timetime;
      ELSE
       CREATE TEMP TABLE time_filtered_dispatch as
        SELECT * FROM unit_dispatch d WHERE d.interval_datetime between start_timetime and end_timetime;
      END IF;

      CREATE TEMP TABLE duids_filtered_dispatch as
      SELECT * FROM time_filtered_dispatch WHERE duid = ANY(duids);

      UPDATE duids_filtered_dispatch d SET asbidrampupmaxavail = d.maxavail WHERE d.asbidrampupmaxavail > d.maxavail;
      UPDATE duids_filtered_dispatch d SET asbidrampdownminavail = 0  WHERE d.asbidrampdownminavail < 0;

      UPDATE duids_filtered_dispatch d SET rampupmaxavail = d.availability WHERE d.rampupmaxavail > d.maxavail;
      UPDATE duids_filtered_dispatch d SET rampdownminavail = 0 WHERE d.rampdownminavail < 0;

      RETURN QUERY SELECT d.interval_datetime,
                          SUM(d.availability) as availability,
                          SUM(d.totalcleared) as totalcleared,
                          SUM(d.finalmw) as finalmw,
                          SUM(d.asbidrampupmaxavail) as asbidrampupmaxavail,
                          SUM(d.asbidrampdownminavail) as asbidrampdownminavail,
                          SUM(d.rampupmaxavail) as rampupmaxavail,
                          SUM(d.rampdownminavail) as rampdownminavail,
                          SUM(d.pasaavailability) as pasaavailability,
                          SUM(d.maxavail) as maxavail
                     FROM duids_filtered_dispatch d group by d.interval_datetime;

    END
    $func$;
    """

_create_get_duids_for_stations = """
    CREATE OR REPLACE FUNCTION get_duids_for_stations(stations text[])
      RETURNS TABLE (duid text)
      LANGUAGE plpgsql AS
    $func$

    BEGIN

      RETURN QUERY SELECT d.duid FROM duid_info d WHERE d."station name" = ANY(stations) ;

    END
    $func$;
    """

_create_get_duids_and_stations_function = """
    CREATE OR REPLACE FUNCTION get_duids_and_stations(regions text[], start_timetime timestamp, end_timetime timestamp,
                                                     dispatch_type text, tech_types text[])
      RETURNS TABLE (duid text, "station name" text)
      LANGUAGE plpgsql AS
    $func$

      DECLARE available_duids text[];

    BEGIN

      DROP TABLE IF EXISTS time_filtered_bids;
      DROP TABLE IF EXISTS filtered_duid_info;

      CREATE TEMP TABLE time_filtered_bids as
      SELECT * FROM bidding_data b WHERE b.interval_datetime between start_timetime and end_timetime;

      IF array_length(tech_types, 1) > 0 THEN
        CREATE TEMP TABLE filtered_duid_info as
        SELECT * FROM duid_info d WHERE d."unit type" = ANY(tech_types) and "dispatch type" = dispatch_type 
                                        and region = ANY(regions);
      ELSE
        CREATE TEMP TABLE filtered_duid_info as
        SELECT * FROM duid_info d WHERE "dispatch type" = dispatch_type and region = ANY(regions);
      END IF;

      RETURN QUERY SELECT d.duid, d."station name" FROM duid_info d
                    WHERE d.duid IN (SELECT DISTINCT t.duid from filtered_duid_info t);

    END
    $func$;
    """

_create_aggregate_prices_function = """
    CREATE OR REPLACE FUNCTION aggregate_prices(regions text[], start_timetime timestamp, end_timetime timestamp)
      RETURNS TABLE (settlementdate timestamp, price float)
      LANGUAGE plpgsql AS
    $func$
    BEGIN

      DROP TABLE IF EXISTS time_filtered_price;

      CREATE TEMP TABLE time_filtered_price as
      SELECT b.settlementdate, b.regionid, b.totaldemand, b.rrp FROM demand_data b
       WHERE b.settlementdate between start_timetime and end_timetime and regionid = ANY(regions);

      RETURN QUERY SELECT b.settlementdate, sum(b.totaldemand) as totaldemand, 
                          sum(b.rrp*b.totaldemand)/sum(b.totaldemand) as rrp
                     FROM time_filtered_price b GROUP BY b.settlementdate;

    END
    $func$;
    """

_create_statements = [
    _create_bidding_data_table,
    _create_demand_data_table,
    _create_duid_info_table,
    _create_price_bins_table,
    _create_unit_dispatch_table,
    _create_aggregate_bids_function,
    _create_aggregate_dispatch_data_function,
    _create_distinct_unit_types_function,
    _create_get_bids_by_unit_function,
    _create_get_duids_for_stations,
    _create_get_duids_and_stations_function,
    _create_aggregate_prices_function,
    _create_aggregate_dispatch_data_duids_function,
]


def create_db_tables_and_functions(connection_string):
    """
    Creates the tables and functions needed to store and retreive data in a PostgresSQL database. This function
    should be run after creating an empty database, then functions in the
    :py:mod:`nem_bidding_dashboard.populate_postgres_db` can be used to add data to the database.

    Examples:

    >>> from nem_bidding_dashboard import postgres_helpers

    >>> con_string = postgres_helpers.build_connection_string(
    ... hostname='localhost',
    ... dbname='bidding_dashboard_db',
    ... username='bidding_dashboard_maintainer',
    ... password='1234abcd',
    ... port=5433)

    >>> create_db_tables_and_functions(con_string)

    Args:
        connection_string: str for connecting to PostgresSQL database, the function :py:func:`nem_bidding_dashboard.postgres_helpers.build_connection_string`
            can be used to build a properly formated connection string, or alternative any string that matches the
            format allowed by `PostgresSQL <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING>`_
            can be used

    """
    with psycopg.connect(connection_string) as conn:
        with conn.cursor() as cur:
            for statement in _create_statements:
                cur.execute(statement)
            conn.commit()
