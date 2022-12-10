""" 
TODO:
    Pages and links
    Any other GUI elements, aesthetics
    Documentation
"""

from dash import Dash, dcc, html, Input, Output, State
import dash_bootstrap_components as dbc
from datetime import datetime, date, timedelta 
from typing import List, Tuple

from create_plots import *
from query_supabase import unit_types



from dash import html

def build_banner():
    return html.Div(
        id="banner",
        className="banner",
        children=[
            html.Div(
                id="banner-text",
                children=[
                    html.H5("NEM Dashboard"),
                ],
            ),
        ],
    )

def how_to_use():
    return """
This dashboard plots the aggregate bids made in the National Electricity Market (NEM). 
This data can be filtered by datetime, and will either be shown in daily periods 
(at 5-min resolution) or weekly periods (at hourly resolution). 
...
"""





def get_settings_content(duid_options, station_options, tech_type_options, region_options, initial_regions):
    settings_content = [
        html.Div(
            id='tech-type-div', 
            children=[
                html.H6('Regions', className='selector-title'),
                dcc.Checklist(
                    id='region-checklist',
                    options=region_options,
                    value=initial_regions,
                    inline=True,
                ),
                html.H6('Unit Type', className='selector-title'),
                dcc.Dropdown(
                    id='tech-type-dropdown', 
                    options=tech_type_options, 
                    value=None,
                    multi=True 
                ),
            ]
        ),
        html.Div(
            id='duid-div', 
            children=[
                html.H6('Select Units by Station', className='selector-title'),
                dcc.Dropdown(
                    id='station-dropdown', 
                    value=None,
                    options=station_options,
                    multi=False,
                ),
                html.H6('Select Units by DUID', className='selector-title'),
                dcc.Dropdown(
                    id='duid-dropdown', 
                    value=None,
                    options=duid_options,
                    multi=True,
                ),
            ]
        ),
        html.Div(
            id='dispatch-type-div',
            children=[
                html.H6('Dispatch Type', className='selector-title'),
                dcc.RadioItems(
                    id='dispatch-type-selector', 
                    options=['Generator', 'Load'], 
                    value='Generator',
                ),
                html.H6('Bidding Data Options', className='selector-title'),
                dcc.RadioItems(
                    id='raw-adjusted-selector', 
                    options=['Raw Bids', 'Adjusted Bids'], 
                    value='Adjusted Bids', 
                ), 
            ]
        ), 
        html.Div(
            id='show-demand-div',
            children=[
                html.H6('Show other Metrics', className='selector-title'),
                dcc.Checklist(
                    id='price-demand-checkbox', 
                    options=['Demand', 'Price'], 
                    value=['Demand', 'Price'],
                ),
            ]
        ), 
    ]
    return settings_content

title = 'Aggregated Bids by Region'
graph_content = dcc.Graph(
    id='graph',
    figure={
        'layout':go.Layout(margin={'t': 20}),
    }, 
)

def get_content(
    region_options, 
    initial_regions, 
    max_date,
    initial_start_date_obj, 
    initial_duration, 
    duid_options, 
    station_options, 
    tech_type_options
):
    content = [

        html.Div(
            html.H6(id="graph-name", children=title), 
        ),
        html.Div(
            html.H6(id='datetime-duration-title', className='selector-title', children='Pick starting datetime and duration:')
        ),
        html.Div(
            id="datetime-duration-selector", 
            children=[
                html.Div(
                    id='datetime-picker', 
                    children=[
                        dcc.DatePickerSingle(
                            id='start-date-picker',
                            date=initial_start_date_obj, 
                            max_date_allowed=max_date,
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
                    ]
                ),
                dcc.RadioItems(
                    id='duration-selector',
                    options=['Daily', 'Weekly'],
                    value=initial_duration,
                    inline=True,
                ),
                
            ]
        ),
        html.Div(
            id="graph-box", 
            children=graph_content,
        ),
        html.Div(
            id="graph-selectors", 
            children=get_settings_content(duid_options, station_options, tech_type_options, region_options, initial_regions),
        ), 
        html.Div(
            id='dispatch-metric-selectors',
            children=[
                html.H6(className='selector-title', children='Additional Dispatch Data'), 
                dcc.Checklist(
                    id='dispatch-checklist', 
                    options=list(DISPATCH_COLUMNS.keys()),
                    # options=['Avalailability', 'Dispatch Volume', 'Final MW', 'As Bid Ramp Up Max Avail', 'As Bid Ramp Down Min Avail', 'Ramp Up Max Avail', 'Ramp Down Min Avail', 'PASA Availability', 'Max Availability'], 
                    value=[]
                )
            ]
        ),
        html.Div(
            id="how-to-use", 
            children=[
                html.H6(id="how-to-use-header", children="How to use"),
                how_to_use(), 
            ],
        )
    ]

    return content


# def build (title:str, settings_content:list, graph_content:list):
def build (
    region_options, 
    initial_regions, 
    max_date,
    initial_start_date_obj, 
    initial_duration, 
    duid_options, 
    station_options, 
    tech_type_options
):
    return html.Div(
        [
            build_banner(),
            html.Div(
                id="app-container",
                children=
                    get_content(region_options, initial_regions, max_date,
                        initial_start_date_obj, initial_duration, duid_options, 
                        station_options, tech_type_options)
                ,
            ),
            html.Div(
                className="banner",
                id="footer",
            )
            
        ]
    )

