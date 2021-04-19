from typing import List

import dash_html_components as html
import dash_core_components as dcc
import dash_table as dt
from dash.dependencies import Input, Output

import e8_dash_app

from datapipe.compute import build_compute, ComputeStep
from datapipe.metastore import MetaStore
from datapipe.dsl import Catalog, Pipeline


def ui_pipeline_overview(ms: MetaStore, catalog: Catalog, pipeline: Pipeline):
    steps = build_compute(ms, catalog, pipeline)

    def _build_dash_catalog_list():
        res = []

        for name, tbl in catalog.catalog.items():
            di = ms.get_table_debug_info(name)

            res.append(html.Li([
                di.name,
                html.Ul([
                    html.Li([f'size: {di.size}'])
                ]),
            ]))

        return html.Ul(res)


    def _build_dash_pipeline_list():
        res = []

        for step in steps:
            res.append(html.Li([
                step.name,
                html.Ul([
                    html.Li(f'Inputs: [' + ', '.join(i.name for i in step.input_dts) + ']'),
                    html.Li(f'Outputs: [' + ', '.join(i.name for i in step.output_dts) + ']'),
                    html.Li(f'To process: {len(ms.get_process_ids(step.input_dts, step.output_dts))}')
                ])
            ]))
        
        return html.Ul(res)


    def _dash_index():
        return dcc.Tabs([
            dcc.Tab(label='Catalog', children=[
                _build_dash_catalog_list(),
            ]),
            dcc.Tab(label='Compute steps', children=[
                _build_dash_pipeline_list(),
            ])
        ])


    app = e8_dash_app.E8Dash('Datapipe', e8_nav_links=[], e8_contact_us_button=False)
    app.layout = _dash_index()
    return app


def ui_table_view(ms: MetaStore, catalog: Catalog, pipeline: Pipeline):
    app = e8_dash_app.E8Dash('Datapipe', e8_nav_links=[], e8_contact_us_button=False)

    def _dash_index():
        return html.Div([
            dcc.Dropdown(
                id='table-name',
                options=[{'label': k, 'value': k} for k,v in catalog.catalog.items()],
            ),
            html.Div(id='table-div', children=[
            ])
        ])
    
    @app.callback(
        Output('table-div', 'children'),
        [Input('table-name', 'value')]
    )
    def update_table(table_name):
        if table_name:
            meta_df = ms.get_metadata(table_name).head()
            data_df = catalog.catalog[table_name].store.read_rows(meta_df.index)
            data_df = data_df.reset_index()

            return [dt.DataTable(
                columns=[{"name": f'_{i}', "id": i} for i in data_df.columns],
                data=data_df.to_dict('records')
            )]
        else:
            return []

    app.layout = _dash_index()
    return app
