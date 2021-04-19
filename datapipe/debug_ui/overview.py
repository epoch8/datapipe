import dash_html_components as html
import dash_core_components as dcc


from datapipe.metastore import MetaStore
from datapipe.dsl import Catalog, Pipeline
from datapipe.compute import build_compute


def ui_overview_setup(app, ms: MetaStore, catalog: Catalog, pipeline: Pipeline):
    pass

def ui_overview_index(app, ms: MetaStore, catalog: Catalog, pipeline: Pipeline):
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


    return _dash_index()
