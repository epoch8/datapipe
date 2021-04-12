from dataclasses import dataclass
from importlib.metadata import metadata
from typing import List, Union, Any, TYPE_CHECKING

import re

import fsspec

from ..dsl import Filedir, FileStoreAdapter
from .steps import ComputeStep
from .metastore import MetaStore, hash_python_object


def _pattern_to_attrnames(pat: str) -> List[str]:
    return re.findall(r'\{([^/]+?)\}', pat)

def _pattern_to_glob(pat: str) -> str:
    return re.sub(r'\{([^/]+?)\}', '*', pat)

def _pattern_to_match(pat: str) -> str:
    return re.sub(r'\{([^/]+?)\}', r'(?P<\1>[^/]+?)', pat)



@dataclass
class FiledirUpdater(ComputeStep):
    table_name: str
    filedir: Filedir

    def __post_init__(self):
        assert(_pattern_to_attrnames(self.filedir.filename_pattern) == ['id'])

    def run(self, ms: MetaStore) -> None:
        filename_pattern = self.filedir.filename_pattern

        filename_glob = _pattern_to_glob(filename_pattern)
        filename_match = _pattern_to_match(filename_pattern)

        files = fsspec.open_files(filename_glob)

        ids = []
        hashes = []

        for f in files:
            m = re.match(filename_match, f.path)
            assert(m is not None)

            ids.append(m.group('id'))

            info = files.fs.info(f.path)
            meta = (info.get('size'), info.get('mtime'))
            hashes.append(hash_python_object(meta))
        
        ms.update_hashes_full(self.table_name, ids, hashes)
