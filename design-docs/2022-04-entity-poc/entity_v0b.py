from gettext import Catalog


class Table:
    pass


catalog = Catalog({
    "feed": Table(
        entity="item",
    ),
    "ozon_edit": Table(
        entity="item"
    )
})
