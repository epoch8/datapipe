class Entity:
    pass


class PropGroup:
    pass


class String:
    pass


class Integer:
    pass


entities = {
    "item": Entity(
        name="item",
        idx=["pipeline_id", "id"],

        properties=[
            PropGroup(
                "feed",
                [
                    ("title", String()),
                    ("price", Integer()),
                ]
            )
        ],
        parts=[
            "feed",
            "ozon_edit",
            "aliexpress_edit",
        ]
    )
}
