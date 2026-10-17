"""Sphinx configuration for the pylocker documentation."""
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

from pylocker import __version__ as VERSION

project = 'pylocker'
author = 'Bachir Aoun'
copyright = '2016, Bachir Aoun'
version = VERSION
release = VERSION
rst_epilog = f".. |VERSION| replace:: {VERSION}"

extensions = ['sphinx.ext.autodoc', 'sphinx.ext.napoleon', 'sphinx.ext.githubpages']
master_doc = 'index'
source_suffix = '.rst'
exclude_patterns = ['_build']
pygments_style = 'sphinx'

autodoc_member_order = 'bysource'
autoclass_content = 'class'

html_theme = 'alabaster'
html_theme_options = {'github_user': 'bachiraoun', 'github_repo': 'pylocker',
                      'github_banner': True, 'show_powered_by': False}
html_title = f'PYthon LOCKER package. pylocker v{VERSION}'
html_static_path = []
html_show_sourcelink = False
html_show_sphinx = False
