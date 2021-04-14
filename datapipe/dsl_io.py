from c12n_pipe.store.table_store_filedir import PILAdapter

from .dsl import FileStoreAdapter


class Textfile(FileStoreAdapter):
    def __init__(self) -> None:
        pass


# TODO implement
class CSVFile(FileStoreAdapter):
    def __init__(self) -> None:
        pass


