"""
This program runs a Dash application displaying info about the National 
Electricity Market (NEM). 
"""

import dash
from dash import Dash, dcc, html, Input, Output, State
from datetime import datetime, date, timedelta 
from typing import List, Tuple

from create_plots import *
from query_supabase import unit_types
import layout_template


app = Dash(__name__)
app.title = 'NEM Dashboard'

# Initial state of the dashboard
region_options = ['NSW', 'VIC', 'TAS', 'SA', 'QLD']
initial_regions = region_options
max_date = date.today() - timedelta(days=1)
# Sets initial start date to be yesterday, will require database updating daily
# initial_start_date_obj = max_date
initial_start_date_obj = date(2020, 1, 21)
initial_start_date_str = initial_start_date_obj.strftime('%Y/%m/%d %H:%M:%S')
initial_duration = 'Daily'
duid_station_options = get_duid_station_options(initial_start_date_str, initial_regions, initial_duration)
duid_options = sorted(duid_station_options['DUID']) #[1:]
station_options = sorted(list(set(duid_station_options['STATION NAME'])))
tech_type_options = sorted(unit_types()['UNIT TYPE'])

app.layout = layout_template.build(
    region_options, 
    initial_regions, 
    max_date,
    initial_start_date_obj, 
    initial_duration, 
    duid_options, 
    station_options, 
    tech_type_options
)


@app.callback(
    Output('duid-dropdown', 'value'),
    Output('station-dropdown', 'value'),
    Input('duid-dropdown', 'value'),
    Input('tech-type-dropdown', 'value'), 
    Input('dispatch-type-selector', 'value'), 
    Input('station-dropdown', 'value'),
    State('start-date-picker', 'date'),
    State('start-hour-picker', 'value'),
    State('start-minute-picker', 'value'),
    State('duration-selector', 'value'),
    State('region-checklist', 'value'))
def update_duids_from_date_region(
    duids: List[str], 
    tech_types: List[str],
    dispatch_type: str,
    station: str, 
    start_date: str, 
    hour: str, 
    minute: str, 
    duration: str, 
    regions: List[str]
) -> Tuple[List[str], str]:
    """
    TODO
    Callback to update the duid dropdown when a station name is selected and 
    remove the value from the station dropdown when the duid options are 
    changed. 
    
    Arguments:
        duids: List of DUIDs currently selected in the DUID dropdown
        station: The currently selected station name in the station dropdown
        start_date: Date of initial datetime 
        hour: Hour of initial datetime (in 24 hour format)
        minute: Minute of initial datetime
        duration: Either 'Daily' or 'Weekly'
        regions: List of currently selected regions
    Returns:
        'duid-dropdown' value (list): List of currently selected DUIDs. If a
            station has been selected, this consists of the list of all DUIDs 
            that fall under that station. 
        'station-dropdown' value (str): Name of currently selected station. If 
            the 'duid-dropdown' was the component that triggered the callback, 
            this value is empty. 
    """

    trigger_id = dash.callback_context.triggered_id
    if not trigger_id:
        return dash.no_update, dash.no_update

    start_date = f'{start_date.replace("-", "/")} {hour}:{minute}:00'
    duid_options = get_duid_station_options(start_date, regions, duration, tech_types, dispatch_type)
    duid_options = duid_options.loc[duid_options['STATION NAME'] == station]
    duid_options = sorted(duid_options['DUID'])

    if trigger_id == 'duid-dropdown':
        if not station:
            return dash.no_update, dash.no_update
        if duids and sorted(duids) != duid_options:
            return dash.no_update, None
        return dash.no_update, dash.no_update

    if trigger_id == 'station-dropdown':
        if not station:
            return dash.no_update, dash.no_update
        return duid_options, dash.no_update
        
        
@app.callback(
    Output('graph', 'figure'),
    Output('graph-name', 'children'),
    Input('start-date-picker', 'date'),
    Input('start-hour-picker', 'value'),
    Input('start-minute-picker', 'value'),
    Input('duration-selector', 'value'),
    Input('region-checklist', 'value'),
    Input('duid-dropdown', 'value'),
    Input('price-demand-checkbox', 'value'),
    Input('raw-adjusted-selector', 'value'),
    Input('tech-type-dropdown', 'value'), 
    Input('dispatch-type-selector', 'value'))
def update(
    start_date: str, 
    hour: str, 
    minute: str, 
    duration: str, 
    regions: List[str], 
    duids: List[str], 
    price_demand_checkbox: str, 
    raw_adjusted: str,
    tech_types: List[str],
    dispatch_type: str
) -> Figure:
    """
    TODO
    Callback to update the graph when the user interacts with any of the graph 
    selectors. 

    Arguments:
        start_date: Date of initial datetime for graph in form 'DD-MM-YYYY', 
            taken from the starting date picker. 
        hour: Hour of initial datetime (in 24 hour format)
        minute: Minute of initial datetime
        duration: Defines the length of time to show data from. Either 'Daily' 
            or 'Weekly'
        regions: List of regions to show data for
        duids: List of DUIDs of units to show data for
        price_demand_checkbox: Contains values 'Demand' and/or 'Price', 
            controlling which of these measures is display
    Returns:
        px figure showing the data specified using the graph selectors. See 
        create_plots.plot_bids for more info. 
    """
    start_date = f'{start_date.replace("-", "/")} {hour}:{minute}:00'
    start_date_obj = datetime.strptime(start_date, '%Y/%m/%d %H:%M:%S')
    if (duration == 'Daily'):
        end_date = (start_date_obj + timedelta(days=1)).strftime('%Y/%m/%d %H:%M:%S')
        resolution = '5-min'
    elif (duration == 'Weekly'):
        end_date = (start_date_obj + timedelta(days=7)).strftime('%Y/%m/%d %H:%M:%S')
        resolution = 'hourly'
    show_demand = 'Demand' in price_demand_checkbox
    show_price = 'Price' in price_demand_checkbox
    raw_adjusted = 'raw' if raw_adjusted == 'Raw Bids' else 'adjusted'
    fig = plot_bids(
        start_date, end_date, resolution, regions, duids, show_demand, 
        show_price, raw_adjusted, tech_types, dispatch_type
    )
    fig = adjust_fig_layout(fig)
    graph_name = get_graph_name(duids)

    return fig, graph_name


if __name__ == '__main__':
    app.run(debug=True)