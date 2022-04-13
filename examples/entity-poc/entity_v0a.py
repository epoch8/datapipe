class Entity:
    pass


class DataStore:
    pass


class Catalog:
    pass


def api_start(ds, catalog, entities):
    pass


entities = {
    "item": Entity(
        name="item",
        idx=["pipeline_id", "id"],
        parts=[
            "feed",
            "ozon_edit",
            "aliexpress_edit",
        ]
    )
}
ds = DataStore()
catalog = Catalog()

api_start(ds, catalog, entities)


"""
query item(pipeline_id = 5, id = 15) {
    pipeline_id
    id

    feed {
        title
    }

    feed_title

    ozon_edit {
        title
    }
}
"""
