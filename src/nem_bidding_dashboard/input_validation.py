import os as _os
from datetime import datetime


def datetime_format(date_time_text, variable_name):
    """ "
    Examples:

    >>> datetime_format('2020-01-01 00:00:00', 'start_time')
    Traceback (most recent call last):
     ...
    ValueError: start_time not in the correct format. The format should be %Y/%m/%d %H:%M:%S

    >>> datetime_format('2020/01/01 00:00:00', 'start_time')

    """
    message = (
        "{} not in the correct format. The format should be %Y/%m/%d %H:%M:%S".format(
            variable_name
        )
    )
    try:
        if date_time_text != datetime.strptime(
            date_time_text, "%Y/%m/%d %H:%M:%S"
        ).strftime("%Y/%m/%d %H:%M:%S"):
            raise ValueError
    except ValueError:
        raise ValueError(message)


def start_time_before_end_time(start_datetime, end_datetime):
    """ "
    Examples:

    >>> start_time_before_end_time('2020/01/01 01:00:00', '2020/01/01 00:00:00')
    Traceback (most recent call last):
     ...
    ValueError: start_time not before end_time.


    """
    if start_datetime > end_datetime:
        raise ValueError("start_time not before end_time.")


def regions_in_expected_set(regions):
    """ "
    Examples:

    >>> regions_in_expected_set(['SA1'])
    Traceback (most recent call last):
     ...
    ValueError: start_time not before end_time.


    """
    for region in regions:
        if region not in ["QLD", "NSW", "VIC", "SA", "TAS"]:
            raise ValueError(
                "Provided region, {}, not one of 'QLD', 'NSW', 'VIC', 'SA', or 'TAS'".format(
                    region
                )
            )


def variable_is_list_of_strings(variable, variable_name):
    """ "
    Examples:

    >>> variable_is_list_of_strings([1], 'regions')
    Traceback (most recent call last):
     ...
    ValueError: regions not a list of strings.

    >>> variable_is_list_of_strings('SA', 'regions')
    Traceback (most recent call last):
     ...
    ValueError: regions not a list.


    """
    if not isinstance(variable, list):
        raise ValueError("{} not a list.".format(variable_name))
    for element in variable:
        if not isinstance(element, str):
            raise ValueError("{} not a list of strings.".format(variable_name))


def variable_in_allowed_set(variable, allowed_set, variable_name):
    """ "
    Examples:

    >>> variable_in_allowed_set('30-min', ['hourly', '5-min'], 'resolution')
    Traceback (most recent call last):
     ...
    ValueError: resolution value provided, 30-min, not in allowed options: hourly, 5-min.

    """
    if variable not in allowed_set:
        allowed_set = ", ".join(allowed_set)
        raise ValueError(
            "{} value provided, {}, not in allowed options: {}.".format(
                variable_name, variable, allowed_set
            )
        )


def data_cache_exits(data_cache):
    """ "
    Examples:

    >>> data_cache_exits('F:/nemosis_data_cache')
    Traceback (most recent call last):
     ...
    ValueError: The raw_data_cache provided does not exist.

    """
    if not _os.path.isdir(data_cache):
        raise ValueError("The raw_data_cache provided does not exist.")


def validate_start_end_and_cache_location(start_time, end_time, raw_data_cache):
    datetime_format(start_time, "start_time")
    datetime_format(end_time, "end_time")
    start_time_before_end_time(start_time, end_time)
    data_cache_exits(raw_data_cache)


def validate_region_demand_args(start_time, end_time, regions):
    datetime_format(start_time, "start_time")
    datetime_format(end_time, "end_time")
    start_time_before_end_time(start_time, end_time)
    variable_is_list_of_strings(regions, "regions")
    regions_in_expected_set(regions)


def validate_aggregate_bids_args(
    regions, start_time, end_time, resolution, raw_adjusted, tech_types, dispatch_type
):
    datetime_format(start_time, "start_time")
    datetime_format(end_time, "end_time")
    start_time_before_end_time(start_time, end_time)
    variable_is_list_of_strings(regions, "regions")
    regions_in_expected_set(regions)
    variable_in_allowed_set(resolution, ["hourly", "5-min"], "resolution")
    variable_in_allowed_set(raw_adjusted, ["raw", "adjusted"], "raw_adjusted")
    variable_is_list_of_strings(tech_types, "tech_types")
    variable_in_allowed_set(dispatch_type, ["Generator", "Load"], "dispatch_type")


def validate_duid_bids_args(duids, start_time, end_time, resolution, raw_adjusted):
    datetime_format(start_time, "start_time")
    datetime_format(end_time, "end_time")
    start_time_before_end_time(start_time, end_time)
    variable_is_list_of_strings(duids, "duids")
    variable_in_allowed_set(resolution, ["hourly", "5-min"], "resolution")
    variable_in_allowed_set(raw_adjusted, ["raw", "adjusted"], "raw_adjusted")


def validate_stations_and_duids_in_regions_and_time_window_args(
    regions, start_time, end_time, dispatch_type, tech_types
):
    datetime_format(start_time, "start_time")
    datetime_format(end_time, "end_time")
    start_time_before_end_time(start_time, end_time)
    variable_is_list_of_strings(regions, "regions")
    regions_in_expected_set(regions)
    variable_is_list_of_strings(tech_types, "tech_types")
    variable_in_allowed_set(dispatch_type, ["Generator", "Load"], "dispatch_type")


def validate_get_aggregated_dispatch_data_args(
    column_name, regions, start_time, end_time, resolution, dispatch_type, tech_types
):
    variable_in_allowed_set(
        column_name,
        [
            "AVAILABILITY",
            "TOTALCLEARED",
            "FINALMW",
            "ASBIDRAMPUPMAXAVAIL",
            "ASBIDRAMPDOWNMINAVAIL",
            "RAMPUPMAXAVAIL",
            "RAMPDOWNMINAVAIL",
            "PASAAVAILABILITY",
            "MAXAVAIL",
        ],
        "column_name",
    )
    datetime_format(start_time, "start_time")
    datetime_format(end_time, "end_time")
    start_time_before_end_time(start_time, end_time)
    variable_is_list_of_strings(regions, "regions")
    regions_in_expected_set(regions)
    variable_in_allowed_set(resolution, ["hourly", "5-min"], "resolution")
    variable_is_list_of_strings(tech_types, "tech_types")
    variable_in_allowed_set(dispatch_type, ["Generator", "Load"], "dispatch_type")


def validate_get_aggregated_dispatch_data_by_duids_args(
    column_name, duids, start_time, end_time, resolution
):
    variable_in_allowed_set(
        column_name,
        [
            "AVAILABILITY",
            "TOTALCLEARED",
            "FINALMW",
            "ASBIDRAMPUPMAXAVAIL",
            "ASBIDRAMPDOWNMINAVAIL",
            "RAMPUPMAXAVAIL",
            "RAMPDOWNMINAVAIL",
            "PASAAVAILABILITY",
            "MAXAVAIL",
        ],
        "column_name",
    )
    datetime_format(start_time, "start_time")
    datetime_format(end_time, "end_time")
    start_time_before_end_time(start_time, end_time)
    variable_is_list_of_strings(duids, "duids")
    variable_in_allowed_set(resolution, ["hourly", "5-min"], "resolution")


def validate_unit_types_args(dispatch_type, regions):
    variable_in_allowed_set(dispatch_type, ["Generator", "Load"], "dispatch_type")
    variable_is_list_of_strings(regions, "regions")
    regions_in_expected_set(regions)
