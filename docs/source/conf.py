# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys

sys.path.insert(0, os.path.abspath("../../src/"))


# -- Project information -----------------------------------------------------

project = "nem-bidding-dashboard"
copyright = "2022, Nicholas Gorman and Patrick Chambers"
author = "Nicholas Gorman and Patrick Chambers"

# The full version, including alpha/beta/rc tags
release = "1.0.1"


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.

# autodoc allows docstrings to be pulled straight from your code
# napoleon supports Google/NumPy style docstrings
# intersphinx can link to other docs, e.g. standard library docs for try:
# doctest enables doctesting
# todo is self explanatory
# viewcode adds links to highlighted source code
# MyST is a CommonMark parser that plugs into Sphinx. Enables you to write docs in md.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.intersphinx",
    "sphinx.ext.doctest",
    "sphinx.ext.todo",
    "sphinx.ext.viewcode",
    "myst_parser",
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []


# -- Formats for MyST --------------------------------------------------------
source_suffix = [".rst", ".md"]

# --  Napoleon options--------------------------------------------------------
# use the :param directive
napoleon_use_param = True

# -- Autodoc options ---------------------------------------------------------

# Automatically extract typehints when specified and place them in
# descriptions of the relevant function/method.
autodoc_typehints = "both"

# Only insert class docstring
autoclass_content = "class"

# --  Intersphinx options-----------------------------------------------------
intersphinx_mapping = {"python": ("https://docs.python.org/3", None)}

# --  MyST options------------------------------------------------------------

myst_enable_extensions = [
    "colon_fence",
    "deflist",
]

myst_heading_anchors = 3

# --  Todo options------------------------------------------------------------

todo_include_todos = True

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "furo"

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]
