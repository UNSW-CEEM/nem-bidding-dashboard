from datetime import date
import fetch_data

import pandas as pd
import plotly.express as px
from dash import Dash, dcc, html, Input, Output
import dash
import os

cwd = os.path.dirname(__file__)
raw_data_cache = os.path.join(cwd, "nemosis_data_cache/")

app = Dash(__name__)

app.layout = html.Div([
        html.H2("Available electricity generation"), 
        dcc.Checklist(
            id="fuel_source_checklist", 
            options=[
                "Hydro", "Solar", "Fossil", "Wind", 
                "Renewable/ Biomass/ Waste", "Battery Storage", 
                "Fuel Source Average", "Fuel Source Total"
            ], 
            value=["Fuel Source Average"]
        ),
        dcc.DatePickerSingle(
            id="start_date_picker",
            date=date(2019, 12, 18), 
            display_format="DD/MM/YY",
        ), 
        dcc.DatePickerSingle(
            id="end_date_picker",
            date=date(2019, 12, 25),
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
    Input("fuel_source_checklist", "value"),
    Input("start_date_picker", "date"),
    Input("end_date_picker", "date"),
    Input("update_graph_button", "n_clicks"))
def update(fuel_source: list, start_date: str, end_date: str, num_clicks: int):
    trigger_id = dash.ctx.triggered_id
    if trigger_id and trigger_id != "update_graph_button":
        return dash.no_update
    
    # TODO: only update dataframe when required
    start_date = f"{start_date.replace('-', '/')} 00:00:00"
    end_date = f"{end_date.replace('-', '/')} 00:00:00"

    # TODO: Replace with better way of selecting fuel source averages/sums
    if "Fuel Source Average" in fuel_source:
        return plot_availability_by_fuel_source(start_date, end_date, "avg")
    if "Fuel Source Total" in fuel_source:
        return plot_availability_by_fuel_source(start_date, end_date, "sum")
    
    
    return plot_duid_availability(fuel_source, start_date, end_date)


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
def plot_duid_availability(fuel_source: list, start_date: str, end_date: str):
    duid_availability_data = get_duid_availability(fuel_source, start_date, end_date)
    fig = px.line(
        duid_availability_data,
        x="SETTLEMENTDATE", 
        y="AVAILABILITY", 
        color="DUID",
        title="Available Generation By DUID",
        labels={
            "SETTLEMENTDATE": "Settlement Date", 
            "AVAILABILITY": "Available Electricity Generationn (MW)", 
            "DUID": "DUID",
        }, 
    )

    #fig.update_yaxes(
    #    range=[0,20000]
    #)

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
def get_duid_availability(fuel_source: list, start_date: str, end_date: str):
    # TODO: Find proper location for data cache 
    df = fetch_data.get_duid_availability_data(start_date, end_date, raw_data_cache)
    # Change dates in dataframe to ISO formatted dates for use in plotly figure
    df["SETTLEMENTDATE"] = df["SETTLEMENTDATE"].apply(
        lambda txt: str(txt).replace("/", "-")
    )
    # TODO: Do this better, i.e. actually pair each DUID with its primary fuel
    # source, so it can then be colorcoded etc.
    duid_info = fetch_data.get_duid_data(raw_data_cache)
    duid_info = duid_info.loc[duid_info["Fuel Source - Primary"].isin(fuel_source)]
    df = df.loc[df["DUID"].isin(duid_info["DUID"])]
    df = df.sort_values("SETTLEMENTDATE")

    return df

def plot_availability_by_fuel_source(start_date: str, end_date: str, calc: str):
    fuel_source_availability_data = get_fuel_source_availability_data(start_date, end_date, calc)
    title = f"{'Average' if calc == 'avg' else 'Total'} Available Generation By Fuel Source"
    fig = px.line(
        fuel_source_availability_data,
        x="SETTLEMENTDATE", 
        y="AVAILABILITY", 
        color="FUELSOURCE",
        title=title,
        labels={
            "SETTLEMENTDATE": "Settlement Date", 
            "AVAILABILITY": "Available Electricity Generationn (MW)", 
            "FUELSOURCE": "Fuel Source",
        }, 
        color_discrete_map={
            "Hydro": "blue",
            "Solar": "orange",
            "Fossil": "brown",
            "Wind": "yellow",
            "Renewable/ Biomass/ Waste": "green",
            "Battery Storage": "red"
        }
    )

 
    return fig

def get_fuel_source_availability_data(start_date: str, end_date: str, calc: str):
    df = fetch_data.get_duid_availability_data(start_date, end_date, raw_data_cache)
    fuel_sources = [
        "Hydro", "Solar", "Fossil", "Wind", 
        "Renewable/ Biomass/ Waste", "Battery Storage", 
    ] 
    avg_source_avail = pd.DataFrame(columns=["SETTLEMENTDATE", "FUELSOURCE", "AVAILABILITY"])
    duid_info = fetch_data.get_duid_data(raw_data_cache)
    for source in fuel_sources:
        fuel_source_duids = duid_info.loc[duid_info["Fuel Source - Primary"] == source]
        intermediate = pd.DataFrame(columns=["SETTLEMENTDATE", "FUELSOURCE", "AVAILABILITY"])
        intermediate = df.loc[df["DUID"].isin(fuel_source_duids["DUID"])]
        #intermediate["FUEL SOURCE"] = source
        intermediate.drop(columns=["DUID"])
        if (calc == 'avg'):
            intermediate = intermediate.groupby("SETTLEMENTDATE", as_index=False).mean("AVAILABILITY")
        else: 
            intermediate = intermediate.groupby("SETTLEMENTDATE", as_index=False).sum("AVAILABILITY")

        intermediate = intermediate.assign(FUELSOURCE=source)
                
        avg_source_avail = pd.concat([avg_source_avail, intermediate])

    
    avg_source_avail = avg_source_avail.sort_values("SETTLEMENTDATE")
    return avg_source_avail



        
if __name__ == "__main__":
    app.run()