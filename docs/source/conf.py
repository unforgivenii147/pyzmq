import sys
from pathlib import Path

here = Path(__file__).parent.absolute()
repo_root = here.parents[1]
sys.path.append(str(repo_root))
from buildutils.bundle import bundled_version

sys.path = sys.path[:-1]
target_libzmq = bundled_version
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.intersphinx",
    "sphinx.ext.napoleon",
    "myst_parser",
    "enum_tools.autoenum",
]
myst_enable_extensions = ["colon_fence", "linkify", "smartquotes", "substitution"]
myst_linkify_fuzzy_links = False
source_suffix = [".md"]
source_encoding = "utf-8"
master_doc = "index"
project = "PyZMQ"
copyright = "Brian E. Granger & Min Ragan-Kelley.\nØMQ logo © iMatix Corporation, used under the Creative Commons Attribution-Share Alike 3.0 License.\nPython logo ™ of the Python Software Foundation, used by Min RK with permission from the Foundation"
intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "tornado": ("https://www.tornadoweb.org/en/stable", None),
}
import zmq

version = ".".join(zmq.__version__.split(".")[:2])
release = zmq.__version__
myst_substitutions = {
    "version": version,
    "release": release,
    "target_libzmq": target_libzmq,
}
exclude_trees = ["build"]
default_role = "literal"
pygments_style = "sphinx"
suppress_warnings = ["epub.unknown_project_files"]
nitpick_ignore = [
    ("py:class", "optional"),
    ("py:class", "Python object"),
    ("py:class", "native socket"),
    ("py:class", "iterable"),
    ("py:class", "callable"),
    ("py:class", "basestring"),
    ("py:class", "unicode"),
]
autodoc_type_aliases = {
    "C.int": "int",
    "bint": "bool",
    "_MonitorMessage": "dict",
    "Frame": "zmq.Frame",
    "Socket": "zmq.Socket",
    "Context": "zmq.Context",
    "_SocketType": "zmq.Socket",
    "_ContextType": "zmq.Context",
}
html_theme = "pydata_sphinx_theme"
html_logo = "_static/logo.png"
html_theme_options = {
    "icon_links": [
        {
            "name": "PyZMQ on GitHub",
            "url": "https://github.com/zeromq/pyzmq",
            "icon": "fa-brands fa-github-square",
        }
    ]
}
html_favicon = "_static/zeromq.ico"
html_static_path = ["_static"]
htmlhelp_basename = "PyZMQdoc"
latex_documents = [
    (
        "index",
        "PyZMQ.tex",
        "PyZMQ Documentation",
        "Brian E. Granger \\and Min Ragan-Kelley",
        "manual",
    )
]
linkcheck_ignore = [
    "https://github\\.com(.*)#",
    "https://github\\.com/zeromq/pyzmq/(issues|commits)(.*)",
]
