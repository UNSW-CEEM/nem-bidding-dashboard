from datetime import date

import dash
import fetch_data
import pandas as pd
import plotly.express as px
import query_supabase_db
from dash import Dash, Input, Output, dcc, html

raw_data_cache = "D:/nemosis_cache"

run_local = False

app = Dash(__name__)
application = app.server

app.layout = html.Div(
    [
        html.H2("Total demand trends per region"),
        dcc.Checklist(
            id="region_checklist",
            options=["NSW", "VIC", "TAS", "SA", "QLD"],
            value=["NSW"],
        ),
        dcc.DatePickerSingle(
            id="start_time_picker",
            date=date(2019, 1, 23),
            display_format="DD/MM/YY",
        ),
        dcc.DatePickerSingle(
            id="end_time_picker",
            date=date(2019, 1, 24),
            display_format="DD/MM/YY",
        ),
        html.Button("Update Graph", id="update_graph_button", n_clicks=0),
        dcc.Graph(id="graph"),
    ]
)


"""
Update plot when the user interacts with any of the inputs, i.e. the start date,
end date or region selection.
Arguments:
    regions: List of regions to display electricity demand data for, taken from
        the region checklist on the webpage.
    start_time: Initial date for graph in form "DD-MM-YYYY", taken from the
        starting date picker.
    end_time: Ending date for graph in form "DD-MM-YYYY", taken from the
        ending date picker.
Returns:
    px line graph figure displaying electricity demand data for the selected
    time period and regions.
"""


@app.callback(
    Output("graph", "figure"),
    Input("region_checklist", "value"),
    Input("start_time_picker", "date"),
    Input("end_time_picker", "date"),
    Input("update_graph_button", "n_clicks"),
)
def update(regions: list, start_time: str, end_time: str, num_clicks: int):
    trigger_id = dash.ctx.triggered_id
    if trigger_id and trigger_id != "update_graph_button":
        return dash.no_update
    # TODO: only update dataframe when required
    start_time = f"{start_time.replace('-', '/')} 00:00:00"
    end_time = f"{end_time.replace('-', '/')} 00:00:00"
    return plot_region_demand(regions, start_time, end_time)


"""
Plots the electricity demand of Australian states/territories over time. Regions
to plot are listed in the regions argument
Arguments:
    regions: List of regions to show on graph figure
    start_time: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS" (time always
        set to "00:00:00:)
    end_time: Ending datetime, formatted identical to start_time
Returns:
    fig: A px line graph showing the electricity demand of each region over time
"""


def plot_region_demand(regions: list, start_time: str, end_time: str):
    region_demand_data = get_region_demand(start_time, end_time)
    fig = px.line(
        region_demand_data[region_demand_data["regionid"].isin(regions)],
        x="SETTLEMENTDATE",
        y="TOTALDEMAND",
        color="REGIONID",
        title="Total Electricity Demand By Region",
        labels={
            "SETTLEMENTDATE": "Settlement Date",
            "TOTALDEMAND": "Electricity Demand (MW)",
            "REGIONID": "State/Territory",
        },
        color_discrete_map={
            "NSW": "blue",
            "VIC": "green",
            "TAS": "purple",
            "SA": "red",
            "QLD": "orange",
        },
    )

    fig.update_yaxes(range=[0, 20000])

    return fig


"""
Get the electricity demand for all regions and format it correctly for use in
the electricity demand graph.
Arguments:
    start_time: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS" (time always
        set to "00:00:00:)
    end_time: Ending datetime, formatted identical to start_time
Returns:
    df: Dataframe containing electricity demand data over the period specified
        by start_time and end_time, adjusted to correct region names, format
        dates and sort by datetime
"""


def get_region_demand(start_time: str, end_time: str) -> pd.DataFrame:
    # TODO: Find proper location for data cache
    if run_local:
        df = fetch_data.get_region_demand_data(start_time, end_time, raw_data_cache)
    else:
        df = query_supabase_db.region_data(start_time, end_time)
    # Change dates in dataframe to ISO formatted dates for use in plotly figure
    df["SETTLEMENTDATE"] = df["SETTLEMENTDATE"].apply(
        lambda txt: str(txt).replace("/", "-")
    )
    df["REGIONID"] = df["REGIONID"].apply(correct_region_name)
    df = df.sort_values("SETTLEMENTDATE")
    return df


"""
Corrects region name for display on graph
Arguments:
    region_name: Name of region e.g. "NSW1"
Returns:
    Display name for region without trailing number e.g. "NSW"
"""


def correct_region_name(region_name: str) -> str:
    return str(region_name)[:-1]


if __name__ == "__main__":
    application.run(debug=True, port=8080)
