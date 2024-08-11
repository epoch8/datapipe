import datetime
import os
import sys

sys.path.insert(0, os.path.abspath("../.."))

project = "Datapipe"
copyright = f"{datetime.date.today().year}, Epoch8 Team and Contributors"
author = "Epoch8 Team and Contributors"

extensions = [
    "sphinx.ext.duration",
    "sphinx.ext.doctest",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.intersphinx",
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "myst_parser",
]

exclude_patterns = [
    "build",
]

source_suffix = [
    ".rst",
    ".md",
]

html_theme = "sphinx_rtd_theme"
