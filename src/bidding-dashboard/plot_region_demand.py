from datetime import date
import fetch_data

import pandas as pd
import plotly.express as px
from dash import Dash, dcc, html, Input, Output
import dash


raw_data_cache = "/home/pat/bidding-dashboard/src/bidding-dashboard/nemosis_data_cache"

app = Dash(__name__)

app.layout = html.Div([
        html.H2("Total demand trends per region"), 
        dcc.Checklist(
            id="region_checklist", 
            options=["NSW", "VIC", "TAS", "SA", "QLD"], 
            value=["NSW"]
        ),
        dcc.DatePickerSingle(
            id="start_date_picker",
            date=date(2020, 1, 1), 
            display_format="DD/MM/YY",
        ), 
        dcc.DatePickerSingle(
            id="end_date_picker",
            date=date(2020, 2, 1),
            display_format="DD/MM/YY",
        ), 
        html.Button("Update Graph", id="update_graph_button", n_clicks=0), 
        dcc.Graph(id="graph"), 
])


"""
Update plot when the user interacts with any of the inputs, i.e. the start date, 
end date or region selection. 
Arguments:
    regions: List of regions to display electricity demand data for, taken from 
        the region checklist on the webpage. 
    start_date: Initial date for graph in form "DD-MM-YYYY", taken from the 
        starting date picker. 
    end_date: Ending date for graph in form "DD-MM-YYYY", taken from the 
        ending date picker. 
Returns:
    px line graph figure displaying electricity demand data for the selected 
    time period and regions. 
"""
@app.callback(
    Output("graph", "figure"),
    Input("region_checklist", "value"),
    Input("start_date_picker", "date"),
    Input("end_date_picker", "date"),
    Input("update_graph_button", "n_clicks"))
def update(regions: list, start_date: str, end_date: str, num_clicks: int):
    trigger_id = dash.ctx.triggered_id
    if trigger_id and trigger_id != "update_graph_button":
        return dash.no_update
    # TODO: only update dataframe when required
    start_date = f"{start_date.replace('-', '/')} 00:00:00"
    end_date = f"{end_date.replace('-', '/')} 00:00:00"
    return plot_region_demand(regions, start_date, end_date)


"""
Plots the electricity demand of Australian states/territories over time. Regions 
to plot are listed in the regions argument
Arguments:
    regions: List of regions to show on graph figure
    start_date: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS" (time always 
        set to "00:00:00:)
    end_date: Ending datetime, formatted identical to start_date 
Returns:
    fig: A px line graph showing the electricity demand of each region over time
"""
def plot_region_demand(regions: list, start_date: str, end_date: str):
    region_demand_data = get_region_demand(start_date, end_date)
    fig = px.line(
        region_demand_data[region_demand_data["REGIONID"].isin(regions)], 
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
            "QLD": "orange"
        }
    )

    fig.update_yaxes(
        range=[0,20000]
    )

    return fig


"""
Get the electricity demand for all regions and format it correctly for use in 
the electricity demand graph. 
Arguments:
    start_date: Initial datetime, formatted "DD/MM/YYYY HH:MM:SS" (time always 
        set to "00:00:00:)
    end_date: Ending datetime, formatted identical to start_date
Returns:
    df: Dataframe containing electricity demand data over the period specified 
        by start_date and end_date, adjusted to correct region names, format 
        dates and sort by datetime
"""
def get_region_demand(start_date: str, end_date: str) -> pd.DataFrame:
    # TODO: Find proper location for data cache
    df = fetch_data.get_region_demand_data(start_date, end_date,raw_data_cache)
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
    app.run()
