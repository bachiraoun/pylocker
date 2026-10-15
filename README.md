# pylocker
PYthon LOCKER or pylocker package provides a pythonic way to create locking system that
can be used for general purposes as well as for locking files upon reading or writing.

Installation
============
##### pylocker requires:
* Python >= 3.9

##### Installation using pip:
```bash
pip install pylocker
```

Testing
=======
Run every test from the package folder:

```bash
python3 tests/run_tests.py                 # everything, one line per test
python3 tests/run_tests.py --quiet         # summary only
python3 tests/run_tests.py --repeat 5      # find tests that sometimes fail
python3 tests/run_tests.py election auth   # only tests/test_election.py and tests/test_auth.py
python3 tests/run_tests.py --list          # the test files
```

The exit code is 0 when every test passed. The long stress and speed
scripts are in `benchmarks/`; run each one on its own, for example
`python3 benchmarks/test_concurrent_stress.py`.

Security
========
By default the server only accepts connections from the same computer
(`127.0.0.1`). To let other computers connect, opt in:

```python
from pylocker import ServerLocker
locker = ServerLocker(password='my-password', allowRemoteClients=True)
```

The password is sent as plain text, so for connections between computers
use a trusted network or a TLS tunnel. A client with the wrong password
gets `pylocker.AuthenticationError` immediately.

Online documentation
====================
https://bachiraoun.github.io/pylocker/


Authors and developers
======================
* [Bachir Aoun](https://www.linkedin.com/in/bachiraoun) (Author, Developer)
