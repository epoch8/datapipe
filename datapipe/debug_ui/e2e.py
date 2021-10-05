import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
from dash.exceptions import PreventUpdate


def _id_options(ms, catalog):
    # FIXME грязный хак: считаем, что пространство id берется из самой первой таблицы
    tbl = list(catalog.catalog.keys())[0]
    df = ms.get_metadata(tbl)
    return list(df.index)


def ui_e2e_index(app, ms, catalog, pipeline):
    return [
        dcc.Dropdown(
            id='id-dropdown',
            options=[
                {
                    "label": i,
                    "value": i
                }
                for i in _id_options(ms, catalog)
            ]
        ),
        html.Div(
            id='e2e-content'
        )
    ]


def ui_e2e_setup(app, ms, catalog, pipeline):
    @app.callback(
        Output('e2e-content', 'children'),
        [Input('id-dropdown', 'value')]
    )
    def update_e2e(id_value):
        if not id_value:
            raise PreventUpdate

        res = []

        for name, tbl in catalog.catalog.items():
            t = catalog.catalog[name]
            val = t.store.read_rows([id_value])

            res.extend([
                html.H1(name),
                str(val.to_dict()),
            ])

        return res
