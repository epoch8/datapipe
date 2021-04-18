from typing import List

import dash_html_components as html
import dash_core_components as dcc

import e8_dash_app

from datapipe.compute import build_compute, ComputeStep
from datapipe.metastore import MetaStore
from datapipe.dsl import Catalog


def _build_dash_catalog_list(ms: MetaStore, catalog: Catalog):
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


def _build_dash_pipeline_list(ms: MetaStore, steps: List[ComputeStep]):
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


def main(ms, catalog, pipeline):
    app = e8_dash_app.E8Dash('Datapipe', e8_nav_links=[], e8_contact_us_button=False)

    steps = build_compute(ms, catalog, pipeline)

    app.layout = html.Div([
        html.H1('Catalog'),
        _build_dash_catalog_list(ms, catalog),

        html.H1('Compute steps'),
        _build_dash_pipeline_list(ms, steps),
    ])


    app.run_server(host='0.0.0.0')
