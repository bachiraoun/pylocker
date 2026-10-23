# pylocker

**PYthon LOCKER**: a lock manager for Python programs. It lets threads, processes and even computers agree on who may use a resource (a file, a folder, any name you choose) at a given moment, so two programs never change the same thing at once.

## What is pylocker?
A lock is a named permission. A program asks the locker for a name such as `/data/results.dat`, works while it holds the lock, then releases it. Any other program that asks for the same name waits its turn.

The first program to start becomes the *server*. Every other program connects to it as a *client*. Programs agree on who is the server through a small shared *server file*, and a new server takes over when the old one dies. You do not run or manage a server.

```python
from pylocker import ServerLocker

locker = ServerLocker(password='my-password', serverFile='/path/to/server.lock')
with locker.lock('/path/to/resource') as (acquired, lockId):
    if acquired:
        pass  ## protected work here
```

## Why use it?
* **No setup:** no separate service to install or start. Programs elect a server among themselves.
* **Works with any program style:** plain scripts, threads, web frameworks, and `asyncio` code (`acquire_lock` blocks the caller, `acquire_async` does not block your event loop).
* **Handles failure:** a client's locks are freed when its connection drops, and a server that missed three refreshes (about 6 seconds) is replaced.
* **Ordered:** requests are served in order for the paths they share, so a request for several paths is not overtaken again and again by newer requests.
* **Secure by default:** local computer only, a password proof instead of the password, blocking after failed logins, and caps on what one client can cost the server.
* **Pure Python:** needs Python 3.9 or newer. `msgpack` is optional.

## Limitations
* **Names, not files:** a lock path is only a name. Programs must all use the same name, so use absolute paths. Letter case counts.
* **Cooperative:** pylocker does not stop a program that ignores the locker and touches the resource anyway.
* **Locks expire:** the server force-releases any lock held longer than `maxLockTime` (120 seconds by default).
* **Check `acquired`:** `with locker.lock(...)` runs its body even when the lock was not acquired.
* **No encryption:** traffic between computers is not encrypted. Use a trusted network or a TLS tunnel.
* **Clocks:** computers sharing a server file must keep their clocks within a few seconds of each other.
* **Silent peers:** a client that vanished without a goodbye, such as a computer that lost its network, keeps its locks for about 110 seconds.
* **Python only:** only a Python client is provided. Versions 4.0 and 5.0 cannot log in to each other.

## Installation
pylocker is pure Python and needs Python >= 3.9. Fork or download the [github repository](https://github.com/bachiraoun/pylocker/) and copy the `pylocker` package to your python's site-packages.

## Testing
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
For hours of load with random client crashes, run
`python3 benchmarks/test_soak.py --minutes 120`.

## Lock paths
A lock path is a name, not a file that pylocker looks up. It is not made
absolute and symlinks are not followed, so a relative path means different
files for programs started in different folders. Letter case counts, so
`Data/x` and `data/x` do not exclude each other even on a file system that
treats them as one file. Use the same absolute path everywhere.

`with locker.lock(path) as (acquired, lockId):` runs its body even when the lock
was not acquired, so check `acquired` first.

## Security
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

A locker ignores stop orders unless you opt in with `ServerLocker(..., allowRemoteOrders=True)` or
`allow_receiving_remote_orders(True)`, because an order can end the process (`killRemotePID=True`).

A stop order is made for the lockers named in `receivers`, and its proof covers
that list, so a recorded order cannot be passed on to other lockers. A locker
ignores an order that names other lockers.

After 3 failed logins (`maxAuthFailures`) a sender is blocked for 300 seconds
(`blockSeconds`); `maxAuthFailures=None` switches this off. A sender from
another computer is identified by its address, so computers behind one shared
address (a router doing address translation) are blocked together. A sender on the same computer
is identified by the process id it reports, which stops a mistaken program
but not a determined one. `locker.blockedClients` lists the blocked senders
and `locker.unblock_client()` lifts the blocks.

The server limits what one peer can cost it: a message above
`maxMessageBytes` (65536) ends the connection, at most `maxClients` (1000)
connections are held, one client may have `maxPendingPerClient` (1000)
lock requests waiting, and one request may name at most 1000 paths of at most 4096 characters each. A
request or publication too big for `maxMessageBytes` is refused with a `ValueError` before it is sent,
so it can never cost the connection. A lock
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
still answers the probes for it. The server never waits for a peer to read: it
queues what it sends, and when more than 1 MiB for one peer is waiting it closes
that connection, so one stuck client cannot hold up the lock grants of the
others.

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

Lock hold times and waiting times are measured on a clock that ignores changes of the system time and the
time the computer sleeps. A running server refreshes its server file every 2 seconds. A server that has
missed three refreshes (about 6 seconds) is taken for dead and may be replaced.
The server file holds the clock time of the computer that serves, so computers that share a server file
must keep their clocks within a few seconds of each other (for example with a time synchronisation
service). Otherwise a server that is alive can look dead to the other computers, and a second server may
start.

## Upgrading from 4.0
* Versions 4.0 and 5.0 cannot log in to each other, so upgrade every computer
  that shares a server file.
* `receivers=[]` now means nobody. It used to mean everybody.
* A server file that its group can write is refused with a `PermissionError`;
  run `chmod 600` on it.
* A server that has missed three refreshes is replaced after about 6 seconds
  instead of 2.
* `load()` refuses settings that `save()` never writes.
* A peer that falls more than 1 MiB behind in reading is disconnected.
* Timeouts and lock times must be finite numbers; `True` and `inf` are refused.
* The unused `reconnect` argument is removed; remove it from your calls.
* A path longer than 4096 characters, and a request, publication or order too big for `maxMessageBytes`, raise
  `ValueError`.
* `retries` and `retryDelay` refuse `True` and `inf`, like the other numbers.
* Stop orders are ignored unless you switch them on with `allowRemoteOrders=True`.

## Online documentation
https://bachiraoun.github.io/pylocker/


## Authors and developers
* [Bachir Aoun](https://www.linkedin.com/in/bachiraoun) (Author, Developer)
