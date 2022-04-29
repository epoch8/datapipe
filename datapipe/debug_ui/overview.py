import dash_html_components as html
import dash_interactive_graphviz as gv
from dash.dependencies import Input, Output

from datapipe.compute import Catalog, Pipeline
from datapipe.compute import build_compute
from datapipe.datatable import DataStore


def ui_overview_setup(app, ds: DataStore, catalog: Catalog, pipeline: Pipeline):
    @app.callback(
        Output('overview_text', 'children'),
        [Input('pipeline_graph', 'selected_node')]
    )
    def graph_node_selected(node_name):
        return node_name


def ui_overview_index(app, ds: DataStore, catalog: Catalog, pipeline: Pipeline):
    steps = build_compute(ds, catalog, pipeline)

    steps_dots = []

    for table_name, table in ds.tables.items():
        di = table.meta_table.get_table_debug_info()

        steps_dots.append(
            f'{table_name} [shape=box3d label=<<B>{table_name}</B><BR/>'
            f'size={di.size}<BR/>'
            f'{"<BR/>".join(table.primary_keys)}>]'
        )

    for step in steps:
        steps_dots.append(f'{step.get_name()} [shape=box]')
        for inp in step.get_input_dts():
            steps_dots.append(f'{inp.name} -> {step.get_name()}')
        for out in step.get_output_dts():
            steps_dots.append(f'{step.get_name()} -> {out.name}')

    steps_dot = '\n'.join(steps_dots)

    dot_source = f'''
digraph {{
{steps_dot}
}}
'''

    def _build_dash_catalog_list():
        res = []

        for name, tbl in catalog.catalog.items():
            di = ds.get_table_debug_info(name)

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
                    html.Li('Inputs: [' + ', '.join(i.name for i in step.input_dts) + ']'),
                    html.Li('Outputs: [' + ', '.join(i.name for i in step.output_dts) + ']'),
                    html.Li(f'To process: {len(ds.get_full_process_ids(step.input_dts, step.output_dts))}')
                ])
            ]))

        return html.Ul(res)

    def _dash_index():
        return [
            html.H1('Pipeline overview'),
            html.Div(
                gv.DashInteractiveGraphviz(
                    id='pipeline_graph',
                    dot_source=dot_source,
                    style={'height': '1000px'}
                )
            ),
            html.Div(id='overview_text'),
        ]

    return _dash_index()
