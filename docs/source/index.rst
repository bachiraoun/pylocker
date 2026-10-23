Welcome to pylocker V. |VERSION| documentation!
===============================================

.. automodule:: pylocker

Quick start
===========

.. code-block:: python

    from pylocker import ServerLocker

    locker = ServerLocker(password='my-password', serverFile='/path/to/server.lock')
    with locker.lock('/path/to/resource') as (acquired, lockId):
        if acquired:
            pass  ## protected work here

Security and limits
===================
A locker serves this computer only unless ``allowRemoteClients=True``. Logins use a password proof, never
the password. Stop orders are ignored unless ``allowRemoteOrders=True``. The full list of limits and the
upgrade notes from 4.0 are in the project README.

ServerLocker
============
.. autoclass:: pylocker.Locker.ServerLocker
    :members:
    :show-inheritance:

SingleLocker
============
.. autoclass:: pylocker.Locker.SingleLocker
    :show-inheritance:

FACTORY
=======
.. autoclass:: pylocker.Locker._LockerFactory
    :special-members: __call__
    :noindex:

AuthenticationError
===================
.. autoexception:: pylocker.AuthenticationError

Package functions
=================
.. autofunction:: pylocker.get_version
.. autofunction:: pylocker.get_author
.. autofunction:: pylocker.get_email
.. autofunction:: pylocker.get_doc
.. autofunction:: pylocker.get_repository
