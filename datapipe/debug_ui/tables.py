import dash_html_components as html
import dash_core_components as dcc
import dash_table as dt

from dash.dependencies import Input, Output


def ui_tables_index(app, ds, catalog, pipeline):
    return [
        html.H1('Tables data'),
        dcc.Dropdown(
            id='table-name',
            options=[{'label': k, 'value': k} for k, v in catalog.catalog.items()],
        ),
        html.Div(id='table-content')
    ]


def ui_tables_setup(app, ds, catalog, pipeline):
    @app.callback(
        Output('table-content', 'children'),
        [Input('table-name', 'value')]
    )
    def update_table(table_name):
        if table_name:
            tbl = catalog.get_datatable(ds, table_name)

            idx_df = tbl.meta_table.get_existing_idx().head(20)
            data_df = tbl.get_data(idx_df)

            data_df = data_df.applymap(str).applymap(lambda x: x if len(x) < 100 else x[:100] + '...')

            return [dt.DataTable(
                columns=[{"name": i if i != 'id' else '_id', "id": i} for i in data_df.columns],
                data=data_df.to_dict('records')
            )]
        else:
            return []
