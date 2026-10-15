"""
PYthon LOCKER or pylocker package provides a pythonic way to create locking system that
can be used for general purposes as well as for locking files upon reading or writing.


The asyncio implementation lives in the Locker module. A ServerLocker
instance either becomes the server or connects to the running one, and
lets threads and processes lock and release string entities such as file
paths. By default the server serves this machine only; pass
``allowRemoteClients=True`` to serve other machines too.

The legacy implementation based on multiprocessing connections is kept in
the ServerLocker module.


Installation guide:
===================
pylocker is a pure python 3.9+ module that needs no particular
installation. One can either fork pylocker's `github repository <https://github.com/bachiraoun/pylocker/>`_
and copy the package to python's site-packages or use pip as the following:


.. code-block:: console

    pip install pylocker


Package Functions:
==================
"""
from .__pkginfo__ import __version__, __author__, __email__, __onlinedoc__, __repository__, __pypi__
from .Locker import ServerLocker, SingleLocker, FACTORY, AuthenticationError

def get_version():
    """Returns pylocker's version number.

    :Returns:
        #. version (str): The package version.
    """
    return __version__

def get_author():
    """Returns pylocker's author's name.

    :Returns:
        #. author (str): The author's name.
    """
    return __author__

def get_email():
    """Returns pylocker's author's email.

    :Returns:
        #. email (str): The author's email address.
    """
    return __email__

def get_doc():
    """Returns pylocker's official online documentation link.

    :Returns:
        #. url (str): The documentation URL.
    """
    return __onlinedoc__

def get_repository():
    """Returns pylocker's official online repository link.

    :Returns:
        #. url (str): The repository URL.
    """
    return __repository__

def get_pypi():
    """Returns pylocker's Python Package Index (PyPI) link.

    :Returns:
        #. url (str): The PyPI project URL.
    """
    return __pypi__
