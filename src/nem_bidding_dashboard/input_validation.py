from datetime import datetime


def datetime_format(date_time_text, variable_name):
    message = (
        "{} not in the correct format. The format should be %Y-%m-%d HH:MM:SS".format(
            variable_name
        )
    )
    try:
        if date_time_text != datetime.strptime(
            date_time_text, "%Y-%m-%d HH:MM:SS"
        ).strftime("%Y-%m-%d HH:MM:SS"):
            raise ValueError
    except ValueError:
        ValueError(message)
