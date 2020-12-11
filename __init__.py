"""
PYthon LOCKER or pylocker package provides a pythonic way to create locking system that
can be used for general purposes as well as for locking files upon reading or writing.


Old pylocker implementation is available in Locker module and it works by
creating and updating a general locking file anytime a lock is requested with a
certain pass. Lock pass is used to specify the user who sets the lock and who
can have access to whatever is locked. Any user who knows the lock pass can
access whatever is locked.

New pylocker implementation is a better and more advanced implementation
that can be used to orchestrate locking and releasing string entities between
threads and processes on the machine or between different machines as well.
This new implementation can be found in ServerLocker module and is primarily
implemented to distribute permissions between threads and processes to read
and write system files.


Installation guide:
===================
pylocker is a pure python 2.7.x and 3.x module that needs no particular
installation. One can either fork pylocker's `github repository <https://github.com/bachiraoun/pylocker/>`_
and copy the package to python's site-packages or use pip as the following:


.. code-block:: console

    pip install pylocker


Package Functions:
==================
"""
from .__pkginfo__ import __version__, __author__, __email__, __onlinedoc__, __repository__, __pypi__
from .Locker import Locker
from .ServerLocker import ServerLocker, SingleLocker, FACTORY


def get_version():
    """Get pylocker's version number."""
    return __version__

def get_author():
    """Get pylocker's author's name."""
    return __author__

def get_email():
    """Get pylocker's author's email."""
    return __email__

def get_doc():
    """Get pylocker's official online documentation link."""
    return __onlinedoc__

def get_repository():
    """Get pylocker's official online repository link."""
    return __repository__

def get_pypi():
    """Get pylocker pypi's link."""
    return __pypi__
