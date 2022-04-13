from typing import List

# DataPipe ######################################################################################


class Entity:
    properties = {}
    indexes = ()


class EnityLinkPoint:
    def __init__(self, cls, field_name):
        self.cls = cls
        self.fielf_name = field_name


class EntityLink:
    def __init__(self, point1, point2):
        self.point1 = point1
        self.point2 = point2


class EntityRelations:
    links = []


class Transformation:
    func = None
    inputs = []
    outputs = []


class TransformationGraph:
    relations = None
    steps = []


class APISchema:
    relations = None
    schema = {}


class APIProperty:
    entity = None
    fields = []
    relations = {}


class StringParam:
    pass


class IntParam:
    pass


class TableStoreDB:
    pass


class MemoryStoreDB:
    pass


class EntityStore:
    def __init__(self, entity, store):
        self.entity = entity
        self.store = store


class DataStore:
    def __init__(self, entity_stores, default_store=None):
        self.entity_stores = entity_stores
        self.default_store = default_store


class Executor:
    pass


class Deployment:
    pass


def run_graph(graph, ds, executor):
    pass


def run_api(api, ds):
    pass


def kub_deploy(graph, ds, deploy):
    pass

# T4mp ######################################################################################


class Product(Entity):
    properties = {
        "id": StringParam()
    }
    indexes = ('id')


class OzonProduct(Entity):
    properties = {
        "id": StringParam()
    }
    indexes = ('id')


class T4mpEntityRelations(EntityRelations):
    links = [
        EntityLink(
            EnityLinkPoint(Product, 'id'),
            EnityLinkPoint(OzonProduct, 'id')
        )
    ]


class OzonTransformations:
    @classmethod
    def get_ozon_product(cls, products: List[Product]) -> List[OzonProduct]:
        return []


class T4mpGraph(TransformationGraph):
    relations = T4mpEntityRelations
    steps = [
        Transformation(
            inputs=[Product],
            outputs=[OzonProduct],
            func=OzonTransformations.get_ozon_product,
        )
    ]


class T4mpApi(APISchema):
    relations = T4mpEntityRelations
    schema = {
        "products": APIProperty(
            entity=Product,
            fields=['id'],
            relations={
                "ozon_product": APIProperty(
                    entity=OzonProduct,
                    fields=['id']
                )
            }
        )
    }

# Runtime ---------------------------------------------------------------------------------------


mem_store = MemoryStoreDB()
db_store = TableStoreDB()
meta_store = TableStoreDB()

datastore = DataStore(
    [EntityStore(Product, db_store)],
    default_store=mem_store,
    meta_store=meta_store
)

executor = Executor()
deploy = Deployment()

run_graph(T4mpGraph, datastore, executor)
run_api(T4mpApi, datastore)
kub_deploy(T4mpGraph, datastore, deploy)
