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

The password is never sent. The server sends a random challenge and the
client answers with a Hash-based Message Authentication Code (HMAC) made
from the password and the challenge; the server answers with its own proof,
so a client never talks to a program that does not know the password.
A client with the wrong password gets `pylocker.AuthenticationError`
immediately. The traffic itself is not encrypted, so for connections between
computers use a trusted network or a TLS tunnel. A login that someone recorded can be used to
guess a weak password offline, so choose a long random password.

A password must not be empty. To run without a login, pass `password=None`. This is for the local computer
only: it cannot be combined with `allowRemoteClients=True`, and remote stop
orders then need their own password
(`allow_receiving_remote_orders(True, password=...)`).

A stop order is made for the lockers named in `receivers`, and its proof covers
that list, so a recorded order cannot be passed on to other lockers. A locker
ignores an order that names other lockers.

After 3 failed logins (`maxAuthFailures`) a sender is blocked for 300 seconds
(`blockSeconds`); `maxAuthFailures=None` switches this off. A sender from
another computer is identified by its address. A sender on the same computer
is identified by the process id it reports, which stops a mistaken program
but not a determined one. `locker.blockedClients` lists the blocked senders
and `locker.unblock_client()` lifts the blocks.

The server limits what one peer can cost it: a message above
`maxMessageBytes` (65536) ends the connection, at most `maxClients` (1000)
connections are held, one client may have `maxPendingPerClient` (1000)
lock requests waiting, and one request may name at most 1000 paths. A lock
can only be released by the connection that holds it, and two connections
cannot use the same unique name.

A publication or an order that is not well formed is dropped with a warning
and never reaches the other clients, and a client that receives a message it
cannot handle logs an error and keeps its connection. The sender name shown
to other clients comes from the login, not from the message.

Connections ask the operating system to probe a peer that has gone silent
(after 60 seconds, then every 10 seconds, up to 5 times). A peer that
vanished without a goodbye, such as a computer that lost its network, is
therefore dropped after about 110 seconds, and its slot, its name and its
locks are freed.
A peer that is connected but stops reading is different: the operating system
still answers the probes for it. When the server cannot send to a peer for 10
seconds, it closes that connection, so one stuck client cannot hold up the
lock grants of the others.

Lock requests are served in order for the paths they share: a request for
several paths is not overtaken again and again by newer requests for one of
them. Only the oldest 100 waiting requests keep their paths this way, and
requests for other paths are never held back.

More caps protect the server's memory: one client may hold at most
`maxLocksPerClient` (10000) paths locked, a locker keeps at most
`maxPublications` (1000) publications, and at most `maxLoginsInProgress`
(100) connections may be logging in at once; when that is reached the one
that is silent and has waited longest is closed first, so connections that say
nothing cannot keep real clients out; connections that already sent their
hello message are closed last. `maxClients` counts only clients that have logged in.
The election lock is held on a private file next to the server file
(`<serverFile>.lock`), so a user who may only read the server file cannot
hold it up. If another user already created that lock file in a
shared folder, startup stops with a `PermissionError`; remove the file or use a
private folder.

The server file is created readable and writable by its owner only. A
server file is refused with a `PermissionError` when it is not a regular
file, belongs to another user, or can be written by every user or by its
group (run `chmod 600` on an old file), and so is
a file in a folder that every user can change (a folder with the sticky
bit, like `/tmp`, is fine).

A running server refreshes its server file every 2 seconds. A server that has
missed three refreshes (about 6 seconds) is taken for dead and may be replaced.

Online documentation
====================
https://bachiraoun.github.io/pylocker/


Authors and developers
======================
* [Bachir Aoun](https://www.linkedin.com/in/bachiraoun) (Author, Developer)
