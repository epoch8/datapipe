from typing import List

import logging


logger = logging.getLogger('datapipe.compute.metastore')


def hash_python_object(obj) -> int:
    return hash(obj)


class MetaStore:
    def __init__(self, connstr):
        self.connstr = connstr

    def update_hashes_full(self, name: str, ids: List[str], hashes: List[int]) -> None:
        logger.debug(f'Update hashes: table={name}, len(ids)={len(ids)}')

    # def store_chunk(self, name: str, ids: List[str], hashes:)