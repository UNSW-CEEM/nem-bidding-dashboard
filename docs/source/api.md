
# API Reference

## Dashboard lanucher interface

```{eval-rst}
.. automodule:: nem_bidding_dashboard.run_dashboard
   :members:
```

## PostgresSQL interfaces

A set of functions to allow the user to store and retrieve bidding and operational data from a PostgresSQL database.

### build_postgres_db

```{eval-rst}
.. automodule:: nem_bidding_dashboard.build_postgres_db
   :members:
```

### populate_postgres_db

```{eval-rst}
.. automodule:: nem_bidding_dashboard.populate_postgres_db
   :members:
```

### query_postgres_db

```{eval-rst}
.. automodule:: nem_bidding_dashboard.query_postgres_db
   :members:
```

### postgres_helpers

```{eval-rst}
.. automodule:: nem_bidding_dashboard.postgres_helpers
   :members:
```

## Supabase interfaces

A set of functions to allow the user to store and retrieve bidding and operational data from a database hosted by 
Supabase. To use these functions a Supabase database must have been created with the necessary tables and functions and 
the supabase url and keys need to be configured as environment variables labeled SUPABASE_BIDDING_DASHBOARD_URL,
SUPABASE_BIDDING_DASHBOARD_WRITE_KEY, and SUPABASE_BIDDING_DASHBOARD_KEY.

### populate_supabase_db

```{eval-rst}
.. automodule:: nem_bidding_dashboard.populate_supabase_db
   :members:
```

### query_supabase_db

```{eval-rst}
.. automodule:: nem_bidding_dashboard.query_supabase_db
   :members:
```

## Interfaces for accessing raw AEMO data

A set of function to allow the user to access raw bidding and operation data from AEMO. These interfaces wrap the 
functionality provided by the [NEMOSIS](https://github.com/UNSW-CEEM/NEMOSIS) package. For certain datasets they also 
combined data from different AEMO tables, and combine data stored by AEMO in there 'current' and 'archive' datasets.

### fetch_data

```{eval-rst}
.. automodule:: nem_bidding_dashboard.fetch_data
   :members:
```

## Interfaces for processing raw data

A set of function for processing raw AEMO data before loading into a database. The general purpose of these processing
steps is to limit the data manipulation that must be performed later, and so improve the responsiveness of dashboard
applications that pull data from the database, or speed up runtimes of analysis scripts that use the database.

### preprocessing

```{eval-rst}
.. automodule:: nem_bidding_dashboard.preprocessing
   :members:
```

### fetch_and_preprocess

```{eval-rst}
.. automodule:: nem_bidding_dashboard.fetch_and_preprocess
   :members:
```

## Interface for aggregating from NEMOSIS cache

```{eval-rst}
.. automodule:: nem_bidding_dashboard.query_cached_data
   :members:
```