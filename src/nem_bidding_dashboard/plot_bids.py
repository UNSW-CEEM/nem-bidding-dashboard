"""
This program runs a Dash application displaying info about the National 
Electricity Market (NEM). 
"""


import dash
from dash import Dash, dcc, html, Input, Output, State
from datetime import date

import layout_template
from create_plots import *


app = Dash(__name__)
app.title = 'NEM Dashboard'

region_options = ['NSW', 'VIC', 'TAS', 'SA', 'QLD']
initial_regions = region_options
# Sets initial start date to be yesterday, will require database updating daily
# initial_start_date_obj = date.today() - timedelta(days=1)
initial_start_date_obj = date(2020, 1, 21)

initial_start_date_str = initial_start_date_obj.strftime('%Y/%m/%d %H:%M:%S')
initial_duration = 'Daily'
duid_station_options = get_duid_station_options(initial_start_date_str, initial_regions, initial_duration)
duid_options = sorted(duid_station_options['DUID'])[1:]
station_options = sorted(list(set(duid_station_options['STATION NAME'])))

settings_content = [
    html.Div(
        id='date-selector',
        children=[
            html.H6('Select Time Period', className='selector-title'),
            html.Div(
                id='datetime-picker', 
                children=[
                    dcc.DatePickerSingle(
                        id='start-date-picker',
                        date=initial_start_date_obj, 
                        display_format='DD/MM/YY',
                    ), 
                    dcc.Dropdown(
                        className='start-time-picker',
                        id='start-hour-picker',
                        options=[f'{x:02}' for x in range(0, 25)],
                        value='00',
                        clearable=False,
                    ),
                    dcc.Dropdown(
                        className='start-time-picker',
                        id='start-minute-picker',
                        options=[f'{x:02}' for x in range(0, 61, 5)],
                        value='00',
                        clearable=False,
                    ),
                ],
            ),
            dcc.RadioItems(
                id='duration-selector',
                options=['Daily', 'Weekly'],
                value=initial_duration,
                inline=True,
            ),
        ],
    ),
    html.Div(
        id='region-div',
        children=[
            html.H6('Select Region', className='selector-title'),
            dcc.Checklist(
                id='region-checklist',
                options=region_options,
                value=initial_regions,
                inline=True,
            ),
        ],
    ),
    html.Div(
        id='duid-div',
        children=[
            html.H6('Select Units by DUID', className='selector-title'),
            dcc.Dropdown(
                id='duid-dropdown',
                value=None,
                options=duid_options,
                multi=True,
            ),
            html.H6('Select Units by Station', className='selector-title'),
            dcc.Dropdown(
                #TODO: change format of options so they don't overlap
                id='station-dropdown',
                value=None,
                options=station_options,
                multi=False,
            ),
        ],
    ),
    html.Div(
        id='show-demand-div',
        children=[
            html.H6('Other Metrics', className='selector-title'),
            dcc.Checklist(
                id='price-demand-checkbox', 
                options=['Demand', 'Price'], 
                value=['Demand', 'Price'],
            ),
        ]
    )
]
title = ''
graph_content = dcc.Graph(
    id='graph',
    figure={
        'layout':go.Layout(margin={'t': 20}),
    }, 
)
app.layout = layout_template.build(title, settings_content, graph_content)


@app.callback(
    Output('duid-dropdown', 'value'),
    Output('station-dropdown', 'value'),
    Input('duid-dropdown', 'value'),
    Input('station-dropdown', 'value'),
    State('start-date-picker', 'date'),
    State('start-hour-picker', 'value'),
    State('start-minute-picker', 'value'),
    State('duration-selector', 'value'),
    State('region-checklist', 'value'))
def update_duids_from_date_region(
    duids: List[str], 
    station: str, 
    start_date: str, 
    hour: str, 
    minute: str, 
    duration: str, 
    regions: List[str]
):
#) -> Tuple[List[str], str]:
    """
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
    if trigger_id == 'duid-dropdown':
        if not station:
            return dash.no_update, dash.no_update

        start_date = f'{start_date.replace("-", "/")} {hour}:{minute}:00'
        duid_options = get_duid_station_options(start_date, regions, duration)
        duid_options = duid_options.loc[duid_options['STATION NAME'] == station]
        duid_options = sorted(duid_options['DUID'])
        print(f"duids for station: {duid_options}")
        print(f"duids selected: {duids}")
        if duids and sorted(duids) != duid_options:
            
            return dash.no_update, None

        return dash.no_update, dash.no_update

    if trigger_id == 'station-dropdown':
        if not station:
            return dash.no_update, dash.no_update
    
        start_date = f'{start_date.replace("-", "/")} {hour}:{minute}:00'
        duid_options = get_duid_station_options(start_date, regions, duration)
        if station:
            duid_options = duid_options.loc[duid_options['STATION NAME'] == station]
            duid_options = sorted(duid_options['DUID'])

        return duid_options, dash.no_update



# @app.callback(
    # Output('duid-dropdown', 'value'),
    # Input('station-dropdown', 'value'),
    # State('duid-dropdown', 'value'),
    # State('start-date-picker', 'date'),
    # State('start-hour-picker', 'value'),
    # State('start-minute-picker', 'value'),
    # State('duration-selector', 'value'),
    # State('region-checklist', 'value'))
# def update_duids_from_date_region(
    # station: str, 
    # duids: List[str], 
    # start_date: str, 
    # hour: str, 
    # minute: str, 
    # duration: str, 
    # regions: List[str]
# ) -> Tuple[List[str], str]:
    # """
    # Update duids based on station name
    # """
    # if not station:
        # return dash.no_update
    # 
    # start_date = f'{start_date.replace("-", "/")} {hour}:{minute}:00'
    # duid_options = get_duid_station_options(start_date, regions, duration)
    # if station:
        # duid_options = duid_options.loc[duid_options['STATION NAME'] == station]
        # duid_options = sorted(duid_options['DUID'])
# 
    # return duid_options

     
@app.callback(
    Output('graph', 'figure'),
    Output('graph-name', 'children'),
    Input('start-date-picker', 'date'),
    Input('start-hour-picker', 'value'),
    Input('start-minute-picker', 'value'),
    Input('duration-selector', 'value'),
    Input('region-checklist', 'value'),
    Input('duid-dropdown', 'value'),
    Input('price-demand-checkbox', 'value'))
def update(
    start_date: str, 
    hour: str, 
    minute: str, 
    duration: str, 
    regions: List[str], 
    duids: List[str], 
    price_demand_checkbox: str
) -> Figure:
    """
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
    fig = plot_bids(start_date, end_date, resolution, regions, duids, show_demand, show_price)
    fig = adjust_fig_layout(fig)
    graph_name = get_graph_name(duids)
    return fig, graph_name


if __name__ == '__main__':
    app.run(debug=True)
