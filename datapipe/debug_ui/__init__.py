import dash_html_components as html
import dash_core_components as dcc
import dash_bootstrap_components as dbc
from dash import Dash
from dash.dependencies import Input, Output

from datapipe.compute import Catalog, Pipeline
from datapipe.datatable import DataStore

from .overview import ui_overview_setup, ui_overview_index
from .tables import ui_tables_setup, ui_tables_index
from .e2e import ui_e2e_setup, ui_e2e_index


def ui_main(ds: DataStore, catalog: Catalog, pipeline: Pipeline):
    app = Dash('Datapipe', external_stylesheets=[dbc.themes.BOOTSTRAP])

    ui_overview_setup(app, ds, catalog, pipeline)
    ui_tables_setup(app, ds, catalog, pipeline)
    ui_e2e_setup(app, ds, catalog, pipeline)

    sidebar = html.Div(
        [
            dbc.Nav([
                dbc.NavItem(dbc.NavLink('Overview', href='/', active='exact')),
                dbc.NavItem(dbc.NavLink('Tables', href='/tables', active='exact')),
                dbc.NavItem(dbc.NavLink('E2E', href='/e2e', active='exact')),
            ], pills=True, vertical=True),
        ],
        style={
            'padding': '2rem 1rem',
            'background-color': '#f8f9fa',
        }
    )

    content = html.Div(
        id='content'
    )

    app.layout = dbc.Container([
        dcc.Location(id="url"),
        dbc.Row([
            dbc.Col(
                sidebar,
                width=2
            ),
            dbc.Col(
                content,
                width=10
            )
        ])
    ])

    @app.callback(
        Output('content', 'children'),
        [Input('url', 'pathname')]
    )
    def route(url):
        if url == '/':
            return ui_overview_index(app, ds, catalog, pipeline)
        elif url == '/tables':
            return ui_tables_index(app, ds, catalog, pipeline)
        elif url == '/e2e':
            return ui_e2e_index(app, ds, catalog, pipeline)

        else:
            return f'Unknown route {url}'

    return app
