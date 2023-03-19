import os
import sys

sys.path.insert(0, os.path.abspath(".."))

project = "Datapipe"
copyright = "2023, Epoch8 Team and Contributors"
author = "Epoch8 Team and Contributors"

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "myst_parser",
]

exclude_patterns = ["_build"]

html_theme = "furo"
html_theme_options = {
    "source_repository": "https://github.com/epoch8/datapipe/",
    "source_branch": "master",
    "source_directory": "docs/",
}
