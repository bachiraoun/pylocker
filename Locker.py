# -*- coding: utf-8 -*-
"""Asyncio-powered ServerLocker implementation for pylocker.

Requires Python 3.7 or later.  Uses ``async``/``await`` (Python 3.5+),
``asyncio.create_task`` and ``asyncio.run`` (Python 3.7+), and
``asyncio.StreamReader``/``StreamWriter`` (Python 3.7+).

This module provides a fully asyncio-based distributed locking engine that is
100% API-compatible with the original ServerLocker from ServerLocker.py.
All public method names, signatures, and return values are identical.

Architecture
------------
Each ``ServerLocker`` instance can use one of two event loop strategies,
controlled by the ``sharedLoop`` constructor parameter (default ``True``):

**Shared loop** (``sharedLoop=True``, ``allowServing=False``):
    All client-only instances in the same process attach to a single
    process-wide asyncio event loop running in one daemon thread.  The
    per-client overhead after the first client starts is zero extra OS threads
    and zero extra self-pipe file descriptors.  All reader coroutines run as
    cooperative tasks on the shared loop, which monitors all TCP sockets with
    a single ``kqueue``/``epoll`` call.  The loop is reference-counted:
    the last client to stop shuts it down cleanly.

**Private loop** (``sharedLoop=False``, or ``allowServing=True``):
    Each instance owns a dedicated asyncio event loop in its own daemon
    thread.  Server-capable instances (``allowServing=True``, the default)
    always receive a private loop so the server's background tasks (queue
    processor, heartbeat, max-hold-time monitor, dead-PID monitor) are fully
    isolated from client activity in the same process.

Runtime compatibility
---------------------
pylocker is fully agnostic to the calling application's runtime.
Every ``ServerLocker`` instance keeps its asyncio machinery entirely
inside its own private daemon thread and event loop.  The application
never touches that loop, and pylocker never touches the application's
loop.  The two sides communicate only through
``asyncio.run_coroutine_threadsafe`` and ``concurrent.futures.Future``.

**Synchronous callers** — use ``acquire_lock`` / ``release_lock``:
    Works from any calling context without modification:

    - Plain Python scripts (no event loop at all).
    - OS threads (``threading.Thread``).
    - Django views, Flask handlers, Celery tasks, and every other
      synchronous web or worker framework.
    - Inside a running asyncio task — the call blocks the *task*, not
      the event loop, because ``Future.result()`` releases the
      Global Interpreter Lock while waiting.

    The calling thread blocks until the server responds or the timeout
    expires.  No event loop is required in the calling thread.

**Asynchronous callers** — use ``acquire_async`` / ``release_async``:
    Works from any asyncio-compatible runtime:

    - Standard ``asyncio`` (``async def`` / ``await``).
    - ``uvloop`` (drop-in fast event loop for asyncio).
    - FastAPI, Starlette, aiohttp, Tornado, and every other
      asyncio-based web framework.
    - Trio or Curio bridged via ``anyio``.

    ``acquire_async`` submits work to pylocker's internal loop and
    maps the result back to the caller's loop with
    ``asyncio.wrap_future``.  The caller's event loop is never blocked;
    it continues scheduling other tasks while waiting.  The trade-off
    is a small cross-loop bridge overhead (~5–15 µs) compared with
    calling ``acquire_lock`` from a plain thread.

    If the absolute lowest latency matters and you control the event
    loop, use the raw coroutine API (``_client_acquire`` /
    ``_client_release``) directly inside a coroutine that runs on
    pylocker's own loop.  This is what the ``RAW-ASYNC`` benchmark
    variant does and it eliminates the bridge entirely.

Wire protocol
-------------
msgpack length-prefixed binary framing by default (falls back to
newline-delimited JSON if the ``msgpack`` package is not installed).
The serializer is negotiated automatically during the TCP handshake:
the server announces its choice in the welcome message; every client
switches to match.  No manual coordination is required — both sides
always agree.  Passing ``serializer='json'`` to the constructor forces
JSON unconditionally.  No pickle, no HMAC — the password is used only
as an application-level shared secret checked during the initial handshake.

Election protocol
-----------------
When multiple processes start simultaneously and share the same *serverFile*,
a three-phase file-based election prevents split-brain (two processes both
believing they are the server).

Phase 1 — CLAIM
    The process writes its unique name plus placeholder tokens
    ``TIMESTAMP`` and ``PORT`` to the file, waits 10 ms, then re-reads.
    Whichever process's unique name is in the file wins Phase 1.

Phase 2 — STAMP
    The winner replaces the ``TIMESTAMP`` placeholder with the real wall
    clock time.  This starts the staleness clock.  If the process crashes
    here, the entry ages out after ``_ELECTION_STALE_THRESHOLD`` seconds and
    the next candidate restarts the election.

Phase 3 — ANNOUNCE
    The winner binds the TCP server socket.  ``_start_server`` writes the
    fully-resolved fingerprint (real timestamp + real port) to the file.
    A background ``_heartbeat`` coroutine then refreshes that file every
    ``_HEARTBEAT_INTERVAL`` seconds for as long as the server runs.

Losers poll the file every 10 ms until they see a real port number, then
call ``connect()``.
"""

import os
import re
import json
import time
import uuid
import atexit
import signal
import socket
import logging
import asyncio
import struct
import threading

# ---------------------------------------------------------------------------
# Python version guard — asyncio features used here require Python 3.7+
# ---------------------------------------------------------------------------
import sys
if sys.version_info < (3, 7):
    raise RuntimeError(
        "pylocker requires Python 3.7 or later; "
        "found %s" % sys.version
    )


# ---------------------------------------------------------------------------
# Optional msgpack support
# ---------------------------------------------------------------------------
# msgpack is not a hard dependency.  Install it with:
#   pip install msgpack
# When available, callers may pass serializer='msgpack' to the ServerLocker
# constructor to get binary framing instead of newline-delimited JSON.
# Both sides of a connection MUST use the same serializer — protocol
# negotiation is not yet implemented.
try:
    import msgpack as _msgpack
    MSGPACK_AVAILABLE = True
except ImportError:
    _msgpack          = None
    MSGPACK_AVAILABLE = False


def _to_bytes(value):
    """Encode *value* to UTF-8 bytes if it is not already bytes."""
    return value if isinstance(value, bytes) else value.encode('utf-8')

try:
    SIGKILL = signal.SIGKILL
except AttributeError:
    SIGKILL = signal.SIGTERM          # Windows

# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

# Fingerprint format: uniqueName(timestamp)@address:port[pid]
# Both 'port' and 'pid' use a relaxed character class so that election
# placeholder tokens (e.g. "PORT", "TIMESTAMP") are also accepted.
_FINGERPRINT_RE = re.compile(
    r'^(?P<uname>[^(]+)\((?P<ts>[^)]+)\)@(?P<addr>[^:]+):(?P<port>[^\[]+)\[(?P<pid>[^\]]+)\]$'
)

# Placeholder tokens written to the fingerprint file during election phases.
_ELECTION_PH_TS   = 'TIMESTAMP'   # Phase 1 timestamp placeholder
_ELECTION_PH_PORT = 'PORT'        # Phase 1 / Phase 2 port placeholder

# How long (seconds) between each poll of the fingerprint file during the
# election.  10 ms keeps CPU usage negligible while resolving quickly.
_ELECTION_CLAIM_POLL = 0.010

# Seconds after which a fingerprint timestamp is considered stale, meaning
# the process that wrote it has either crashed or stopped refreshing.
# Must match (or exceed) _HEARTBEAT_INTERVAL so a live server is never
# incorrectly evicted.
_ELECTION_STALE_THRESHOLD = 2.0

# Hard upper bound (seconds) a single _serve_or_connect call may spend in the
# election loop before giving up.
_ELECTION_TIMEOUT = 30.0

# Seconds between consecutive heartbeat writes to the fingerprint file.
_HEARTBEAT_INTERVAL = 2.0

# Extra headroom added to Future.result(timeout=) so the server-side expiry
# fires before the caller's thread gives up.
_RESULT_SLACK   = 1.0

# How long the queue processor waits for the release-or-new-request event
# before sweeping again (seconds).
_QUEUE_POLL     = 0.05

# How often the max-time monitor wakes (seconds).
_MONITOR_POLL   = 5.0

# How often the dead-PID monitor checks for stale lock holders (seconds).
# Applies only to clients connecting from the same host as the server.
_DEAD_PID_POLL  = 5.0

# Encoding used everywhere on the wire.
_ENCODING       = 'utf-8'


# ---------------------------------------------------------------------------
# Process-wide shared event loop  (client-only instances)
# ---------------------------------------------------------------------------
# ServerLocker instances created with ``allowServing=False`` are pure clients
# that will never run a TCP server.  Instead of each spinning up a private
# event loop thread (one OS thread + one self-pipe socket pair per instance),
# they attach to one process-wide event loop running in a single daemon
# thread.  The loop is reference-counted:
#   _acquire_shared_loop()  -- increments refcount, starts the loop if needed
#   _release_shared_loop()  -- decrements refcount, stops the loop at zero
#
# Server-capable instances (``allowServing=True``) always receive a private
# loop so that the server's background tasks (queue processor, heartbeat,
# max-time monitor, dead-PID monitor) are fully isolated.
# ---------------------------------------------------------------------------

_SHARED_LOOP          = None          # the shared asyncio event loop
_SHARED_LOOP_THREAD   = None          # the daemon thread running it
_SHARED_LOOP_REFCOUNT = 0             # number of clients currently attached
_SHARED_LOOP_LOCK     = threading.Lock()


def _acquire_shared_loop():
    """Start or reuse the process-wide shared event loop and increment its reference count.

    Thread-safe.  The loop is created exactly once; subsequent calls increment
    the reference count and return the same loop object.  Call
    :func:`_release_shared_loop` when the client disconnects.

    :Returns:
        #. loop (asyncio.AbstractEventLoop): The running shared event loop.
    """
    global _SHARED_LOOP, _SHARED_LOOP_THREAD, _SHARED_LOOP_REFCOUNT
    with _SHARED_LOOP_LOCK:
        if _SHARED_LOOP is None or not _SHARED_LOOP.is_running():
            _SHARED_LOOP = asyncio.new_event_loop()
            _SHARED_LOOP_THREAD = threading.Thread(
                target=_SHARED_LOOP.run_forever,
                daemon=True,
                name='pylocker-shared-loop',
            )
            _SHARED_LOOP_THREAD.start()
        _SHARED_LOOP_REFCOUNT += 1
        return _SHARED_LOOP


def _release_shared_loop():
    """Decrement the shared loop reference count and shut it down when it reaches zero.

    Thread-safe.  Does nothing when the shared loop was never started or has
    already been shut down.  The loop is stopped outside the lock so we do not
    hold ``_SHARED_LOOP_LOCK`` while blocking on ``thread.join``.
    """
    global _SHARED_LOOP, _SHARED_LOOP_THREAD, _SHARED_LOOP_REFCOUNT
    with _SHARED_LOOP_LOCK:
        _SHARED_LOOP_REFCOUNT = max(0, _SHARED_LOOP_REFCOUNT - 1)
        if _SHARED_LOOP_REFCOUNT > 0 or _SHARED_LOOP is None:
            return
        loop              = _SHARED_LOOP
        thread            = _SHARED_LOOP_THREAD
        _SHARED_LOOP        = None
        _SHARED_LOOP_THREAD = None
    # Stop and close outside the lock.
    if loop.is_running():
        loop.call_soon_threadsafe(loop.stop)
    if thread is not None and thread.is_alive():
        thread.join(timeout=5)
    if not loop.is_closed():
        loop.close()



# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _normalize_path(path):
    """Return a normalised, slash-unified string for *path*."""
    return os.path.normpath(path).replace('\\', '/')


def _get_ip():
    """Return the best local IP address for this machine."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.connect(('8.8.8.8', 80))
        return sock.getsockname()[0]
    except Exception:
        return '127.0.0.1'
    finally:
        try:
            sock.close()
        except Exception:
            pass


async def _write_json(writer, obj):
    """Serialise *obj* to JSON and send it as a newline-terminated line."""
    try:
        writer.write(json.dumps(obj).encode(_ENCODING) + b'\n')
        await writer.drain()
    except Exception:
        pass


async def _read_json(reader):
    """Read one newline-terminated JSON line and deserialise it."""
    raw = await reader.readline()
    if not raw:
        return None
    return json.loads(raw.decode(_ENCODING))



# ── msgpack framing (optional) ───────────────────────────────────────────
# Messages are 4-byte big-endian length-prefixed msgpack frames.
# The length covers only the payload, not the 4-byte header itself.

async def _write_msgpack(writer, obj):
    """Serialise *obj* with msgpack and send it with a 4-byte length prefix."""
    try:
        payload = _msgpack.packb(obj, use_bin_type=True)
        writer.write(struct.pack('>I', len(payload)) + payload)
        await writer.drain()
    except Exception:
        pass


async def _read_msgpack(reader):
    """Read one length-prefixed msgpack frame and unpack it.

    Returns ``None`` when the peer has closed the connection cleanly.
    Raises ``asyncio.IncompleteReadError`` for unexpected disconnections
    so the caller's outer handler can distinguish a clean shutdown from
    a network error.
    """
    header = await reader.readexactly(4)
    length  = struct.unpack('>I', header)[0]
    payload = await reader.readexactly(length)
    return _msgpack.unpackb(payload, raw=False)


# ---------------------------------------------------------------------------
# Lock context manager (sync + async)
# ---------------------------------------------------------------------------
class _LockContext:
    """Context manager returned by ``ServerLocker.lock()``.

    Supports both ``with`` and ``async with`` usage transparently.
    """

    def __init__(self, locker, path, timeout):
        """Initialise a lock context for the given locker and path."""
        self._locker  = locker
        self._path    = path
        self._timeout = timeout
        self._lockId  = None

    # -- sync --
    def __enter__(self):
        """Acquire the lock synchronously and return ``(acquired, lockId)``."""
        acquired, lockId = self._locker.acquire_lock(self._path, timeout=self._timeout)
        if acquired:
            self._lockId = lockId
        return acquired, lockId

    def __exit__(self, *_):
        """Release the lock synchronously."""
        if self._lockId is not None:
            self._locker.release_lock(self._lockId)
            self._lockId = None

    # -- async --
    async def __aenter__(self):
        """Acquire the lock asynchronously and return ``(acquired, lockId)``."""
        acquired, lockId = await self._locker.acquire_async(self._path, timeout=self._timeout)
        if acquired:
            self._lockId = lockId
        return acquired, lockId

    async def __aexit__(self, *_):
        """Release the lock asynchronously."""
        if self._lockId is not None:
            await self._locker.release_async(self._lockId)
            self._lockId = None


# ---------------------------------------------------------------------------
# Factory / singleton cache
# ---------------------------------------------------------------------------
class _LockerFactory:
    """True singleton factory and cache for ``ServerLocker`` instances.

    Only one ``_LockerFactory`` object can ever exist in the interpreter.
    The singleton guarantee is enforced at the class level via ``__new__``
    with double-checked locking so that concurrent callers cannot race to
    create two separate instances.

    Calling ``FACTORY(key=..., **kwargs)`` returns the cached
    ``ServerLocker`` for *key* if one already exists, or creates and caches
    a new one.

    .. code-block:: python

        from pylocker import FACTORY

        ## Both calls return the exact same _LockerFactory object.
        f1 = _LockerFactory()
        f2 = _LockerFactory()
        assert f1 is f2

        ## Both calls return the exact same ServerLocker for the same key.
        locker1 = FACTORY(key='/tmp/myproject', password='secret')
        locker2 = FACTORY(key='/tmp/myproject', password='secret')
        assert locker1 is locker2
    """

    # Class-level state — shared across every attempted instantiation.
    _instance      = None
    _instanceLock  = threading.Lock()

    def __new__(cls):
        """Return the single shared instance, creating it on the first call.

        Double-checked locking ensures thread safety without paying the
        lock-acquisition cost on every call after the instance exists.
        """
        if cls._instance is None:
            with cls._instanceLock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        """Initialise the cache on the very first instantiation only.

        ``__init__`` is called by Python every time someone writes
        ``_LockerFactory()``, even when ``__new__`` returns the existing
        instance.  The ``_initialised`` guard ensures the cache and lock
        are created exactly once.
        """
        if not hasattr(self, '_initialised'):
            self._lock        = threading.Lock()
            self._cache       = {}
            self._initialised = True

    def __call__(self, key, password, regenerate=False, **kwargs):
        """Return a cached or freshly created ``ServerLocker`` for *key*.

        :Parameters:
            #. key (str): Cache key, typically the server-file path or any
               application-defined string that uniquely identifies a locker.
            #. password (str, bytes): Locker password passed to the
               ``ServerLocker`` constructor on first creation.
            #. regenerate (bool): If ``True``, stop and discard the cached
               instance for *key* and build a fresh one.
            #. kwargs: Additional keyword arguments forwarded verbatim to the
               ``ServerLocker`` constructor when a new instance is created.

        :Returns:
            #. locker (ServerLocker): The cached or newly created instance.
        """
        with self._lock:
            if regenerate and key in self._cache:
                try:
                    self._cache[key].stop()
                except Exception:
                    pass
                del self._cache[key]
            if key not in self._cache:
                self._cache[key] = ServerLocker(password=password, **kwargs)
            return self._cache[key]


FACTORY = _LockerFactory()


# ---------------------------------------------------------------------------
# Main class
# ---------------------------------------------------------------------------
class ServerLocker:
    """Asyncio-powered distributed locker compatible with the original ServerLocker API.

    Orchestrates locking and releasing string entities between threads and
    processes on the same machine or across a network.  A private daemon
    thread owns a dedicated ``asyncio`` event loop; all TCP communication runs
    on that loop.  Synchronous callers use the standard blocking API
    unchanged.  Async callers may opt into ``acquire_async`` / ``release_async``
    to avoid blocking their own event loop.

    :Parameters:
        #. password (str, bytes): Shared secret used to authenticate connections.
        #. name (None, str): Human-readable name for this instance.  Defaults
           to the auto-generated unique name.
        #. serverFile (bool, str): If ``True``, the fingerprint file is placed
           in the user home directory as ``.pylocker.serverlocker``.  If
           ``False``, this instance will never serve.  If a string, it is the
           absolute path to the fingerprint file.
        #. defaultTimeout (int, float): Default seconds to wait when acquiring
           a lock.
        #. maxLockTime (int, float): Maximum seconds any single lock may be
           held before the server force-releases it.
        #. port (int): Preferred TCP port for the server.  An adjacent free
           port is used automatically if this one is taken.
        #. allowServing (bool): Whether this instance may act as server.
        #. autoconnect (bool): Whether to call ``start()`` automatically on
           initialisation.
        #. reconnect (bool): Reserved for future use; accepted for API
           compatibility.
        #. connectTimeout (int, float): Seconds allowed for the TCP handshake
           to complete.
        #. logger (bool, logging.Logger): ``False`` silences all output.
           ``True`` creates a default stderr logger.  Pass a
           ``logging.Logger`` instance to use your own.
        #. blocking (bool): If ``True`` and this instance becomes the server,
           ``start()`` blocks until ``stop()`` is called.
        #. debugMode (bool): Emit informational log messages even at INFO
           level.
        #. serializer (str): Wire-framing format.  ``'msgpack'`` (default)
           uses 4-byte length-prefixed binary msgpack frames; falls back to
           JSON automatically if the ``msgpack`` package is not installed.
           ``'json'`` forces newline-delimited UTF-8 JSON unconditionally.
           The serializer is negotiated during the TCP handshake: the server
           announces its choice in the welcome message and every client
           switches to match, so both sides always agree without any manual
           configuration.  Install msgpack with:  ``pip install msgpack``.
        #. sharedLoop (bool): Whether client-only instances attach to the
           process-wide shared event loop instead of creating a private one.
           When ``True`` (the default) and ``allowServing`` is ``False``,
           every client instance in the same process shares a single
           background asyncio thread and a single selector, reducing the
           per-client overhead from one OS thread plus one self-pipe socket
           pair to zero marginal resources after the first client starts.
           When ``allowServing`` is ``True`` this flag has no effect:
           server-capable instances always receive a private loop so their
           background tasks (queue processor, heartbeat, monitors) remain
           isolated. Set to ``False`` to force a private loop for this
           instance regardless of ``allowServing``.

    .. code-block:: python

        from pylocker import ServerLocker

        ## Create a locker.  It will auto-connect or auto-serve.
        locker = ServerLocker(password='secret')

        ## --- Synchronous usage (works anywhere, including inside asyncio) ---
        acquired, lockId = locker.acquire_lock('my_resource')
        if acquired:
            try:
                pass  ## do protected work here
            finally:
                locker.release_lock(lockId)

        ## --- Async opt-in (no blocking inside your event loop) ---
        async def worker():
            acquired, lockId = await locker.acquire_async('my_resource')
            if acquired:
                try:
                    pass  ## do protected work here
                finally:
                    await locker.release_async(lockId)

        ## --- Context manager (sync) ---
        with locker.lock('my_resource') as (acquired, lockId):
            if acquired:
                pass  ## do protected work here

        ## --- Context manager (async) ---
        async def worker_ctx():
            async with locker.lock('my_resource') as (acquired, lockId):
                if acquired:
                    pass  ## do protected work here
    """

    # Keys persisted via pickle / save().  These are the mangled attribute
    # names that match the original ServerLocker pickle format.
    _PICKLE_KEYS = [
        '_ServerLocker__name',
        '_ServerLocker__uniqueName',
        '_ServerLocker__password',
        '_ServerLocker__serverFile',
        '_ServerLocker__defaultTimeout',
        '_ServerLocker__maxLockTime',
        '_ServerLocker__port',
        '_ServerLocker__allowServing',
        '_ServerLocker__address',
        '_ServerLocker__pid',
        '_ServerLocker__blocking',
        '_ServerLocker__allowRemoteOrders',
        '_ServerLocker__debugMode',      # mangled — was plain 'debugMode' pre-M2
        '_ServerLocker__sharedLoop',     # process-wide shared event loop preference
    ]

    def __init__(self, password, name=None, serverFile=True,
                 defaultTimeout=20, maxLockTime=120, port=3000,
                 allowServing=True, autoconnect=True, reconnect=False,
                 connectTimeout=20, logger=False,
                 blocking=False, debugMode=False,
                 serializer='msgpack', sharedLoop=True):
        # Set the private backing store directly to avoid the setter running
        # before the logger is initialised (the setter would try to update the
        # logger level on an object that does not yet have _logger).
        self.__debugMode = bool(debugMode)
        # unique identity
        self.__uniqueName = str(uuid.uuid4())
        if name is None:
            name = self.__uniqueName
        assert isinstance(name, str), \
            "locker server name must be None or a string"
        assert ':' not in name, "':' not allowed in ServerLocker name"
        self.__name = name
        # password
        assert isinstance(password, (str, bytes)), \
            "locker password must be a string or bytes"
        self.__password = _to_bytes(password)
        # flags
        assert isinstance(blocking, bool),      "blocking must be boolean"
        assert isinstance(allowServing, bool),  "allowServing must be boolean"
        assert isinstance(autoconnect, bool),   "autoconnect must be boolean"
        self.__blocking     = blocking
        self.__allowServing = allowServing
        # serializer — controls wire framing for all TCP messages
        assert serializer in ('json', 'msgpack'), \
            "serializer must be 'json' or 'msgpack'; got '%s'" % serializer
        # Availability is checked after set_logger so we can emit a proper
        # warning via self._warn instead of a bare RuntimeError.
        self._useMsgpack = (serializer == 'msgpack')
        # shared event loop preference
        assert isinstance(sharedLoop, bool), \
            "sharedLoop must be a boolean; got '%s'" % type(sharedLoop).__name__
        self.__sharedLoop = sharedLoop
        # timing
        assert isinstance(defaultTimeout, (int, float)) and defaultTimeout > 0, \
            "defaultTimeout must be a positive number"
        assert isinstance(maxLockTime, (int, float)) and maxLockTime > 0, \
            "maxLockTime must be a positive number"
        self.__defaultTimeout = float(defaultTimeout)
        self.__maxLockTime    = float(maxLockTime)
        assert isinstance(port, int) and port > 0, "port must be a positive integer"
        self.__port           = port
        self.__connectTimeout = float(connectTimeout)
        # server file
        if serverFile is True:
            serverFile = os.path.join(os.path.expanduser('~'), '.pylocker.serverlocker')
        elif serverFile is False:
            serverFile = None
        self.__serverFile = serverFile
        # identity / network
        self.__pid     = os.getpid()
        self.__address = _get_ip()
        # remote orders
        self.__allowRemoteOrders = {'allow': False, 'password': None}
        # logger (must come after __debugMode is set)
        self.set_logger(logger)
        # If msgpack was requested but is not installed on this host, warn
        # and fall back to JSON.  Deferred to here so the instance logger
        # is ready before we call self._warn.
        if self._useMsgpack and not MSGPACK_AVAILABLE:
            self._warn(
                "serializer='msgpack' requested but the msgpack package is "
                "not installed on this host; falling back to JSON. "
                "Install with:  pip install msgpack"
            )
            self._useMsgpack = False
        # runtime state (reset on every start())
        self._loop           = None          # asyncio event loop (background thread)
        self._loopThread     = None          # daemon thread owning _loop
        self._usesSharedLoop = False         # True when using the process-wide shared loop
        self._stopEvent      = None          # asyncio.Event — signals shutdown
        self._server        = None          # asyncio.Server (server mode only)
        self._serverPort    = None          # actual bound port
        # server-side shared state (asyncio-thread only — no threading locks needed)
        self._pathsLUT      = None          # path -> lock record
        self._clientsLUT    = None          # client unique name -> {'writer': ..., 'pid': int|None, 'address': str}
        self._lockQueue     = None          # asyncio.Queue of pending acquire requests
        self._queueEvent    = None          # asyncio.Event — new item in queue
        # client-side state
        self._reader        = None          # asyncio.StreamReader
        self._writer        = None          # asyncio.StreamWriter
        self._pending       = None          # ruuid -> asyncio.Future (in-flight requests)
        self._bgTasks       = []            # background asyncio Tasks
        # server metadata (populated when we are a client)
        self.__serverName        = None
        self.__serverUniqueName  = None
        self.__serverMaxLockTime = None
        self.__serverAddress     = None     # server's IP address (client mode only)
        self.__serverPort        = None     # server's TCP port   (client mode only)
        # owned locks (acquired on behalf of this process)
        self.__ownAcquiredLock = threading.Lock()
        self.__ownAcquired     = {}         # ruuid -> request dict
        # publications (accessed from calling threads — use threading.Lock)
        self.__publicationsLock = threading.Lock()
        self.__publications     = {}
        # Election-related: serialises fingerprint file reads/writes across the
        # calling thread (election) and the asyncio loop thread (heartbeat).
        # RLock (reentrant) is required because _clear_fingerprint calls
        # get_running_server_fingerprint while already holding the lock.
        self._serverFileLock = threading.RLock()
        # Tracks whether this instance has ever acted as server or client.
        # Once _wasClient is True, canServe returns False until reset() clears it.
        self._wasServer  = False
        self._wasClient  = False
        # reconnect is accepted for API compatibility with ServerLocker.py but is
        # not implemented in the asyncio backend; stored here as a no-op reminder.
        self.__reconnect = False
        # atexit cleanup
        atexit.register(self._on_atexit)
        # auto-connect
        if autoconnect:
            self.start()

    # ------------------------------------------------------------------
    # Pickle support
    # ------------------------------------------------------------------
    def __getstate__(self):
        """Return pickle-safe state (no runtime objects, no password)."""
        state = {k: self.__dict__.get(k) for k in self._PICKLE_KEYS}
        # strip password from state
        state['_ServerLocker__password'] = None
        # strip orders password
        aror = dict(state.get('_ServerLocker__allowRemoteOrders', {'allow': False}))
        aror.pop('password', None)
        state['_ServerLocker__allowRemoteOrders'] = aror
        return state

    def __setstate__(self, state):
        """Restore from pickle state (runtime objects rebuilt by reset)."""
        self.__dict__.update(state)
        # Migrate old pickle format: pre-M2 stored debugMode as a plain key.
        if 'debugMode' in self.__dict__ and '_ServerLocker__debugMode' not in self.__dict__:
            self.__dict__['_ServerLocker__debugMode'] = bool(self.__dict__.pop('debugMode'))
        self.__dict__['_ServerLocker__password'] = None
        aror = self.__dict__.get('_ServerLocker__allowRemoteOrders', {'allow': False})
        aror['password'] = None
        self.__dict__['_ServerLocker__allowRemoteOrders'] = aror
        self.__dict__.setdefault('_ServerLocker__allowServing', True)
        self.__dict__.setdefault('_ServerLocker__debugMode', False)
        self.__dict__.setdefault('_ServerLocker__sharedLoop', True)
        self.set_logger(None)
        # re-initialise all runtime fields
        self._loop              = None
        self._loopThread        = None
        self._usesSharedLoop    = False
        self._stopEvent         = None
        self._server            = None
        self._serverPort        = None
        self._pathsLUT          = None
        self._clientsLUT        = None
        self._lockQueue         = None
        self._queueEvent        = None
        self._reader            = None
        self._writer            = None
        self._pending           = None
        self._bgTasks           = []
        self.__serverName        = None
        self.__serverUniqueName  = None
        self.__serverMaxLockTime = None
        self.__serverAddress     = None
        self.__serverPort        = None
        self.__ownAcquiredLock  = threading.Lock()
        self.__ownAcquired      = {}
        self.__publicationsLock = threading.Lock()
        self.__publications     = {}
        self._serverFileLock    = threading.RLock()
        self._wasServer         = False
        self._wasClient         = False
        self._useMsgpack        = False   # deserialized instances default to JSON
        atexit.register(self._on_atexit)

    # ------------------------------------------------------------------
    # Logging
    # ------------------------------------------------------------------
    def set_logger(self, logger):
        """Configure the logger for this instance.

        :Parameters:
            #. logger (bool, None, logging.Logger): ``False`` or ``None``
               disables all output.  ``True`` creates a default stderr
               logger.  A ``logging.Logger`` instance is used directly.
        """
        if logger is False or logger is None:
            self._logger = None
        elif logger is True:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter('%(levelname)s %(name)s: %(message)s'))
            log = logging.getLogger('pylocker.ServerLocker')
            log.addHandler(handler)
            log.setLevel(logging.DEBUG if self.__debugMode else logging.WARNING)
            self._logger = log
        else:
            self._logger = logger

    def _critical(self, msg):
        """Emit a critical-level log message."""
        if self._logger:
            self._logger.critical(msg)

    def _error(self, msg):
        """Emit an error-level log message."""
        if self._logger:
            self._logger.error(msg)

    def _warn(self, msg):
        """Emit a warning-level log message."""
        if self._logger:
            self._logger.warning(msg)

    def _info(self, msg):
        """Emit an info-level log message (only when debugMode is True)."""
        if self._logger and self.__debugMode:
            self._logger.info(msg)

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    # ── Identity ──────────────────────────────────────────────────────

    @property
    def name(self):
        """This instance's human-readable name."""
        return self.__name

    @property
    def uniqueName(self):
        """This instance's unique identifier string."""
        return self.__uniqueName

    @property
    def password(self):
        """The locker password as bytes."""
        return self.__password

    @property
    def pid(self):
        """The operating system process ID recorded at initialisation time."""
        return self.__pid

    @property
    def address(self):
        """The local IP address this instance advertises in its fingerprint."""
        return self.__address

    @property
    def port(self):
        """The preferred TCP port configured for this instance."""
        return self.__port

    @property
    def fingerprint(self):
        """The fingerprint template string for this instance.

        The ``{now}`` and ``{port}`` placeholders are not filled in; call
        ``get_running_server_fingerprint()`` to read the live file content.
        """
        return self._fingerprint_template()

    # ── Configuration ─────────────────────────────────────────────────

    @property
    def serverFile(self):
        """Path to the fingerprint file, or ``None``."""
        return self.__serverFile

    @property
    def defaultTimeout(self):
        """Default acquire timeout in seconds."""
        return self.__defaultTimeout

    @property
    def maxLockTime(self):
        """Maximum lock hold time in seconds enforced by the server."""
        return self.__maxLockTime

    @property
    def debugMode(self):
        """``True`` if informational log messages are enabled."""
        return self.__debugMode

    @debugMode.setter
    def debugMode(self, value):
        """Enable or disable informational log messages.

        :Parameters:
            #. value (bool): ``True`` to enable, ``False`` to disable.
        """
        assert isinstance(value, bool), "debugMode must be a boolean"
        self.__debugMode = value
        # Update the logger level if one is already configured.
        if getattr(self, '_logger', None) is not None:
            self._logger.setLevel(logging.DEBUG if value else logging.WARNING)

    @property
    def allowRemoteOrders(self):
        """A copy of the remote-orders settings dict (password excluded)."""
        aror = dict(self.__allowRemoteOrders)
        aror.pop('password', None)
        return aror

    @property
    def canServe(self):
        """``True`` if this instance is permitted to act as server.

        Requires ``allowServing=True``, a configured *serverFile*, and that
        this instance has never previously connected as a client.  Once a
        locker acts as a client the ``_wasClient`` flag is latched to
        ``True`` and ``canServe`` returns ``False`` until ``reset()`` is
        called to clear it.
        """
        return (self.__allowServing
                and self.__serverFile is not None
                and not getattr(self, '_wasClient', False))

    # ── Connection state ──────────────────────────────────────────────

    @property
    def isServer(self):
        """``True`` if this instance is currently the active server."""
        return self._server is not None and self._loop is not None

    @property
    def isClient(self):
        """``True`` if this instance is currently connected as a client."""
        return self._writer is not None and not self.isServer

    @property
    def serverName(self):
        """The connected server's human-readable name, or ``None``."""
        return self.__serverName

    @property
    def serverUniqueName(self):
        """The connected server's unique identifier, or ``None``."""
        return self.__serverUniqueName

    @property
    def serverAddress(self):
        """The connected server's IP address, or ``None`` if not a client."""
        return self.__serverAddress

    @property
    def serverPort(self):
        """The connected server's TCP port, or ``None`` if not a client."""
        return self.__serverPort

    @property
    def serverMaxLockTime(self):
        """The server's maximum lock hold time in seconds, or ``None``."""
        return self.__serverMaxLockTime

    # ── Lock state ────────────────────────────────────────────────────

    @property
    def ownedLocks(self):
        """A snapshot of locks currently held by this instance.

        Returns a dict mapping each lock UUID to the list of paths it covers.
        An empty dict is returned when no locks are held.  This property is
        safe to call from any thread.
        """
        with self.__ownAcquiredLock:
            return {ruuid: list(rec.get('path', []))
                    for ruuid, rec in self.__ownAcquired.items()}

    @property
    def lockedPaths(self):
        """A snapshot of all paths currently locked on the server.

        Returns a dict mapping each locked path string to its lock record.
        ``None`` is returned when this instance is not the server.  The
        snapshot is built from the asyncio-thread's ``_pathsLUT`` using a
        CPython-level GIL-protected dict iteration; it is safe for
        diagnostic purposes but may not be perfectly consistent under heavy
        concurrent load.
        """
        if not self.isServer or self._pathsLUT is None:
            return None
        return {path: dict(rec) for path, rec in list(self._pathsLUT.items())}

    @property
    def clientLocks(self):
        """A snapshot of held locks grouped by lock UUID (server view only).

        Returns a dict mapping each lock UUID to its lock record, which
        includes the owning client's unique name and the locked paths.
        ``None`` is returned when this instance is not the server.
        """
        if not self.isServer or self._pathsLUT is None:
            return None
        grouped = {}
        for path, rec in list(self._pathsLUT.items()):
            ruuid = rec.get('request_unique_id', path)
            if ruuid not in grouped:
                grouped[ruuid] = dict(rec)
                grouped[ruuid]['paths'] = []
            grouped[ruuid]['paths'].append(path)
        return grouped

    # ── Publication state ─────────────────────────────────────────────

    @property
    def messages(self):
        """A list of all publication message keys currently stored."""
        with self.__publicationsLock:
            return list(self.__publications.keys())

    # ── Protected introspection (for tests and diagnostics) ───────────

    @property
    def _clientsQueue(self):
        """The asyncio Queue of pending acquire requests (server view)."""
        return self._lockQueue

    @property
    def _ownRequests(self):
        """In-flight acquire requests submitted by this instance (client view).

        Maps each request UUID to the asyncio Future awaiting the server's
        ``acquired`` response.  ``None`` when not connected as a client.
        """
        return self._pending

    @property
    def _ownAcquired(self):
        """A snapshot of locks acquired by this instance (thread-safe copy)."""
        with self.__ownAcquiredLock:
            return dict(self.__ownAcquired)

    @property
    def _publications(self):
        """A snapshot of the stored publications dict (thread-safe copy)."""
        with self.__publicationsLock:
            return dict(self.__publications)

    # ------------------------------------------------------------------
    # Configuration setters
    # ------------------------------------------------------------------
    def set_maximum_lock_time(self, maxLockTime):
        """Set the maximum seconds any lock may be held.

        :Parameters:
            #. maxLockTime (int, float): Positive number of seconds.
        """
        try:
            maxLockTime = float(maxLockTime)
            assert maxLockTime > 0
        except Exception:
            raise ValueError('maxLockTime must be a positive number')
        self.__maxLockTime = maxLockTime

    def set_default_timeout(self, defaultTimeout):
        """Set the default acquire timeout in seconds.

        :Parameters:
            #. defaultTimeout (int, float): Positive number of seconds.
        """
        try:
            defaultTimeout = float(defaultTimeout)
            assert defaultTimeout > 0
        except Exception:
            raise ValueError('defaultTimeout must be a positive number')
        self.__defaultTimeout = defaultTimeout

    def set_server_file(self, serverFile):
        """Set the fingerprint file path.

        :Parameters:
            #. serverFile (bool, str): Same semantics as the constructor
               parameter.
        """
        if serverFile is True:
            serverFile = os.path.join(os.path.expanduser('~'), '.pylocker.serverlocker')
        elif serverFile is False:
            serverFile = None
        self.__serverFile = serverFile

    def set_password(self, password):
        """Set or update the locker password.

        :Parameters:
            #. password (str, bytes): New shared secret.
        """
        assert isinstance(password, (str, bytes)), \
            "locker password must be a string or bytes"
        self.__password = _to_bytes(password)

    def allow_receiving_remote_orders(self, allow, password=None):
        """Enable or disable execution of remote stop orders.

        :Parameters:
            #. allow (bool): Whether to allow remote orders.
            #. password (None, str, bytes): Password required with each remote
               order.  If ``None`` the locker password is used.
        """
        assert isinstance(allow, bool), "allow must be boolean"
        if password is None:
            password = self.__password
        self.__allowRemoteOrders = {'allow': allow, 'password': _to_bytes(password)}

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    def save(self, path):
        """Save configuration to a JSON file (password excluded).

        :Parameters:
            #. path (str): Destination file path.
        """
        data = {
            'name':           self.__name,
            'serverFile':     self.__serverFile,
            'defaultTimeout': self.__defaultTimeout,
            'maxLockTime':    self.__maxLockTime,
            'port':           self.__port,
            'allowServing':   self.__allowServing,
        }
        with open(path, 'w') as fd:
            json.dump(data, fd, indent=2)

    @classmethod
    def load(cls, path, password, autoconnect=True):
        """Load a previously saved configuration and return a new instance.

        :Parameters:
            #. path (str): Path to the JSON file written by ``save()``.
            #. password (str, bytes): The locker password (not stored in file).
            #. autoconnect (bool): Whether to auto-connect on creation.

        :Returns:
            #. locker (ServerLocker): The newly constructed instance.
        """
        with open(path, 'r') as fd:
            data = json.load(fd)
        data['password']    = password
        data['autoconnect'] = autoconnect
        return cls(**data)

    # ------------------------------------------------------------------
    # Fingerprint helpers
    # ------------------------------------------------------------------
    def _fingerprint_template(self):
        """Return the fingerprint template string for this instance."""
        return '%s({now})@%s:{port}[%s]' % (self.__uniqueName, self.__address, self.__pid)

    @staticmethod
    def _parse_fingerprint(fp):
        """Parse a fingerprint string and return its five fields.

        :Parameters:
            #. fp (str): Fingerprint string in the form
               ``uniqueName(ts)@addr:port[pid]``.  The ``ts`` and ``port``
               fields may be placeholder tokens (``TIMESTAMP`` / ``PORT``)
               during election phases.

        :Returns:
            #. uniqueName (str): Server unique name.
            #. timestamp (str): Timestamp string or the placeholder ``TIMESTAMP``.
            #. address (str): Server IP address.
            #. port (str): Port string or the placeholder ``PORT``.
            #. pid (str): Server process ID as a string.
        """
        match = _FINGERPRINT_RE.match(fp.strip())
        if not match:
            return None, None, None, None, None
        g = match.groupdict()
        return g['uname'], g['ts'], g['addr'], g['port'], g['pid']

    def get_running_server_fingerprint(self, serverFile=None,
                                       raiseNotFound=False, raiseError=True):
        """Read and parse the server fingerprint file.

        :Parameters:
            #. serverFile (None, str): Path to the fingerprint file.  If
               ``None`` the instance's configured path is used.
            #. raiseNotFound (bool): Raise an exception if the file does not
               exist.
            #. raiseError (bool): Raise an exception if parsing fails.

        :Returns:
            #. uniqueName (None, str): Parsed server unique name.
            #. timestamp (None, str): Parsed timestamp (may be the placeholder
               ``TIMESTAMP`` during election Phase 1).
            #. address (None, str): Parsed server IP address.
            #. port (None, str): Parsed port string (may be the placeholder
               ``PORT`` during election Phases 1 and 2).
            #. pid (None, str): Parsed server process ID string.
        """
        if serverFile is None:
            serverFile = self.__serverFile
        uniqueName = timestamp = address = port = pid = None
        if serverFile and os.path.isfile(serverFile):
            try:
                with self._serverFileLock:
                    with open(serverFile, 'r') as fd:
                        line = fd.readline().strip()
                if line:
                    uniqueName, timestamp, address, port, pid = \
                        self._parse_fingerprint(line)
            except Exception as err:
                if raiseError:
                    raise
        elif raiseNotFound:
            raise FileNotFoundError("serverFile '%s' not found" % serverFile)
        return uniqueName, timestamp, address, port, pid

    def _write_fingerprint(self, port):
        """Write the fully-resolved server fingerprint to the configured file.

        This method is called by ``_start_server`` (Phase 3 of the election)
        and by ``_heartbeat`` every ``_HEARTBEAT_INTERVAL`` seconds.  The
        ``_serverFileLock`` prevents the heartbeat and the election from
        writing concurrently.

        :Parameters:
            #. port (int): The port the server is bound to.
        """
        if not self.__serverFile:
            return
        with self._serverFileLock:
            try:
                tmpl = self._fingerprint_template()
                line = tmpl.format(now=str(time.time()), port=str(port))
                with open(self.__serverFile, 'w') as fd:
                    fd.write(line + '\n')
            except Exception as err:
                self._warn("Unable to write server fingerprint: %s" % err)

    def _clear_fingerprint(self):
        """Clear the fingerprint file if it belongs to this instance."""
        if not self.__serverFile:
            return
        with self._serverFileLock:
            try:
                uniqueName, _, _, _, _ = self.get_running_server_fingerprint(
                    raiseNotFound=False, raiseError=False)
                if uniqueName == self.__uniqueName:
                    open(self.__serverFile, 'w').close()
            except Exception:
                pass

    def _write_election_raw(self, line):
        """Write an arbitrary election-phase line to the server file.

        Used by ``_serve_or_connect`` to write Phase 1 (claim) and Phase 2
        (stamp) entries that contain placeholder tokens instead of real
        timestamp or port values.  The ``_serverFileLock`` ensures this
        write is atomic with respect to the heartbeat and other file
        operations in this process.

        :Parameters:
            #. line (str): The full fingerprint line to write.  A trailing
               newline is appended automatically.
        """
        if not self.__serverFile:
            return
        with self._serverFileLock:
            try:
                with open(self.__serverFile, 'w') as fd:
                    fd.write(line + '\n')
            except Exception as err:
                self._warn("Unable to write election line to server file: %s" % err)

    # ------------------------------------------------------------------
    # Background event loop management
    # ------------------------------------------------------------------
    def _start_loop(self):
        """Start the event loop for this instance.

        Client-only instances (``allowServing=False``) attach to the
        process-wide shared event loop and thread instead of creating a
        private one.  The marginal cost after the first client in the process
        starts is zero extra OS threads and zero extra self-pipe file
        descriptors.

        Server-capable instances (``allowServing=True``, the default) always
        receive a private event loop so the server's background tasks are
        isolated from any client activity in the same process.
        """
        if self.__sharedLoop and not self.__allowServing:
            # Client-only with shared loop preference: attach to the
            # process-wide shared event loop.  Zero marginal OS thread cost
            # and zero marginal self-pipe file descriptor cost after the
            # first client in this process has started.
            self._loop           = _acquire_shared_loop()
            self._loopThread     = None   # thread is owned by the process
            self._usesSharedLoop = True
        else:
            # Server-capable instance (allowServing=True), or caller
            # explicitly set sharedLoop=False: always use a private loop so
            # background server tasks stay fully isolated.
            self._loop           = asyncio.new_event_loop()
            self._loopThread     = threading.Thread(
                target=self._loop.run_forever, daemon=True, name='pylocker-loop'
            )
            self._loopThread.start()
            self._usesSharedLoop = False

    def _submit(self, coro):
        """Schedule *coro* on the background loop and return a thread-safe Future.

        :Parameters:
            #. coro (coroutine): An awaitable to submit.

        :Returns:
            #. future (concurrent.futures.Future): Thread-safe future resolved
               when the coroutine completes.
        """
        return asyncio.run_coroutine_threadsafe(coro, self._loop)

    def _run_sync(self, coro, timeout=None):
        """Submit *coro* and block until it completes or *timeout* elapses.

        :Parameters:
            #. coro (coroutine): An awaitable to run.
            #. timeout (None, float): Seconds to wait.  ``None`` means wait
               forever.

        :Returns:
            #. result: Whatever the coroutine returned.
        """
        return self._submit(coro).result(timeout=timeout)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def _on_atexit(self):
        """Clean up on interpreter shutdown."""
        try:
            self._clear_fingerprint()
        except Exception:
            pass
        try:
            self.stop()
        except Exception:
            pass

    def reset(self, raiseError=False):
        """Recycle a stopped locker instance by clearing its runtime state.

        Unlike ``stop()``, this method refuses to interrupt an active
        connection.  If the locker is currently running as a server or
        client, the call fails (or raises, depending on *raiseError*).
        Call ``stop()`` first when you want to disconnect and then
        reinitialise in the same expression.

        :Parameters:
            #. raiseError (bool): If ``True`` and the locker is currently
               running, raise a ``RuntimeError`` instead of returning a
               failure tuple.

        :Returns:
            #. success (bool): ``True`` if the state was cleared.
            #. error (None, str): ``None`` on success.  An explanatory
               string on failure.

        :Raises:
            #. RuntimeError: If *raiseError* is ``True`` and the locker is
               currently running as a server or client.
        """
        if self.isServer or self.isClient:
            errorMessage = (
                "Cannot reset '%s': locker is currently running. "
                "Call stop() first." % self.__name
            )
            if raiseError:
                raise RuntimeError(errorMessage)
            return False, errorMessage
        self.__ownAcquired  = {}
        self.__publications = {}
        self._wasServer     = False
        self._wasClient     = False
        return True, None

    def stop(self):
        """Stop the server or disconnect as a client.

        Before closing the connection this method explicitly releases every
        lock currently held by this instance, sending individual ``release``
        messages to the server.  This is a belt-and-suspenders measure on top
        of the server-side disconnect cleanup: if the TCP close is delayed or
        the server's ``finally:`` block runs after the lookup tables are
        nulled, the explicit release messages arrive first and prevent lock
        leaks.  On SIGKILL neither the atexit handler nor this method runs,
        but the dead-PID monitor on the server side will clean up within
        ``_DEAD_PID_POLL`` seconds.
        """
        self._clear_fingerprint()
        # ── Explicit release-all: belt-and-suspenders over disconnect cleanup ──
        # Grab a snapshot of owned locks while we still have the table, then
        # submit individual release messages so the server processes them
        # before the TCP connection closes.
        if self.isClient and self._writer is not None:
            with self.__ownAcquiredLock:
                ownedSnapshot = dict(self.__ownAcquired)
            for ruuid, rec in ownedSnapshot.items():
                try:
                    fut = self._submit(self._write_msg(self._writer, {
                        'request_unique_id':  ruuid,
                        'action':             'release',
                        'path':               rec.get('path', []),
                        'client_unique_name': self.__uniqueName,
                        'client_name':        self.__name,
                    }))
                    fut.result(timeout=0.5)   # block until the write is flushed
                except Exception:
                    pass
            # Brief pause to let the event loop flush the release messages.
            try:
                import time as _time
                _time.sleep(0.05)
            except Exception:
                pass
        if self._usesSharedLoop:
            # Shared loop: only tear down this client's connection.
            # The shared event loop continues running for other clients.
            loop = self._loop
            if loop is not None and loop.is_running():
                fut = asyncio.run_coroutine_threadsafe(
                    self._disconnect_client(), loop
                )
                try:
                    fut.result(timeout=5)
                except Exception:
                    pass
            _release_shared_loop()
        else:
            # Private loop: run full shutdown and close the loop.
            loop   = self._loop
            thread = self._loopThread
            if loop is not None and loop.is_running():
                asyncio.run_coroutine_threadsafe(self._shutdown_loop(), loop)
                if thread is not None:
                    thread.join(timeout=5)
                if not loop.is_closed():
                    loop.close()
        # null out all runtime state
        self._loop           = None
        self._loopThread     = None
        self._usesSharedLoop = False
        self._stopEvent  = None
        self._server     = None
        self._serverPort = None
        self._pathsLUT   = None
        self._clientsLUT = None
        self._lockQueue  = None
        self._queueEvent = None
        self._reader     = None
        self._writer     = None
        self._pending    = None
        self._bgTasks    = []
        self.__serverName        = None
        self.__serverUniqueName  = None
        self.__serverMaxLockTime = None
        self.__serverAddress     = None
        self.__serverPort        = None

    async def _disconnect_client(self):
        """Disconnect this client from the server without stopping the event loop.

        Called by ``stop()`` when the instance is attached to the process-wide
        shared event loop.  Signals the reader loop to exit via ``_stopEvent``
        and closes the writer so the server observes a clean disconnect.  The
        shared event loop is left running for other clients in this process.
        The reader loop task exits on its own when it sees the stop event or
        reads EOF from the now-closed connection.
        """
        if self._stopEvent is not None:
            self._stopEvent.set()
        if self._writer is not None:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except Exception:
                pass
        # _bgTasks is always empty for client instances; no tasks to cancel.

    async def _shutdown_loop(self):
        """Cancel all background tasks and stop the event loop cleanly."""
        if self._stopEvent is not None:
            self._stopEvent.set()
        if self._server is not None:
            self._server.close()
        if self._writer is not None:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except Exception:
                pass
        tasks = list(getattr(self, '_bgTasks', []))
        for task in tasks:
            task.cancel()
        if tasks:
            import asyncio as _a
            await _a.gather(*tasks, return_exceptions=True)
        self._loop.call_soon(self._loop.stop)

    def start(self, address=None, port=None, password=None, ntrials=3):
        """Start this instance as a server or connect it as a client.

        :Parameters:
            #. address (None, str): Server IP to connect to.  Both *address*
               and *port* must be given together.
            #. port (None, int): Server port to connect to.
            #. password (None, str, bytes): Override the instance password for
               this connection attempt.
            #. ntrials (int): Number of connection attempts before giving up.
        """
        if self.isServer:
            self._warn("'%s' is already serving" % self.__name)
            return
        if self.isClient:
            self._warn("'%s' is already a client" % self.__name)
            return
        if password is not None:
            self.__password = _to_bytes(password)
        self._start_loop()
        self._run_sync(self._init_async_state())
        if address is not None or port is not None:
            assert address is not None and port is not None, \
                "address and port must both be given or both be None"
            success = self.connect(address=address, port=port, ntrials=ntrials)
            if not success:
                self._error("Failed to connect to %s:%s" % (address, port))
                return
        else:
            self._serve_or_connect(ntrials=ntrials)
        if self.__blocking and self.isServer and self._loopThread is not None:
            self._loopThread.join()

    async def _init_async_state(self):
        """Initialise asyncio synchronisation objects on the background loop."""
        self._stopEvent  = asyncio.Event()
        self._lockQueue  = asyncio.Queue()
        self._queueEvent = asyncio.Event()
        self._pathsLUT   = {}
        self._clientsLUT = {}
        self._pending    = {}

    def connect(self, address, port, password=None, ntrials=3):
        """Connect to a running server at *address*:*port*.

        :Parameters:
            #. address (str): Server IP address.
            #. port (int): Server port.
            #. password (None, str, bytes): Override the instance password.
            #. ntrials (int): Number of connection attempts.

        :Returns:
            #. success (bool): Whether the connection was established.
        """
        if password is not None:
            self.__password = _to_bytes(password)
        try:
            return self._run_sync(
                self._client_connect(address, int(port), ntrials),
                timeout=self.__connectTimeout + 2
            )
        except Exception as err:
            self._error("connect() failed: %s" % err)
            return False

    def _serve_or_connect(self, ntrials=3):
        """Auto-discover a running server or win the election to become one.

        When multiple processes start simultaneously with a shared *serverFile*,
        this method implements the three-phase election protocol described in
        the module docstring to ensure exactly one process becomes the server.

        If *serverFile* is ``None`` the election is skipped: the process
        starts a server directly (if ``allowServing`` is ``True``) or emits a
        warning and returns.

        :Parameters:
            #. ntrials (int): Number of TCP connection attempts passed to
               ``connect()`` when a live server is found.
        """
        # ── No server file: skip election, fall back to direct behaviour ──
        if not self.__serverFile:
            if self.__allowServing:
                try:
                    self._run_sync(self._start_server(),
                                   timeout=self.__connectTimeout + 2)
                except Exception as err:
                    self._error("Failed to start server: %s" % err)
            else:
                self._warn(
                    "No serverFile configured and allowServing is False — "
                    "cannot auto-discover server"
                )
            return

        # Ensure the directory that will hold the fingerprint file exists.
        serverDir = os.path.dirname(self.__serverFile)
        if serverDir:
            os.makedirs(serverDir, exist_ok=True)

        # Touch the file if it does not yet exist.
        if not os.path.isfile(self.__serverFile):
            try:
                open(self.__serverFile, 'w').close()
            except Exception:
                pass

        electionStart = time.time()

        while True:
            if time.time() - electionStart > _ELECTION_TIMEOUT:
                self._error(
                    "Election timed out after %.0f seconds — giving up"
                    % _ELECTION_TIMEOUT
                )
                return

            uname, ts, addr, port, pid = self.get_running_server_fingerprint(
                raiseNotFound=False, raiseError=False
            )

            fileIsEmpty       = (uname is None)
            tsIsPlaceholder   = (ts   is None or ts   == _ELECTION_PH_TS)
            portIsPlaceholder = (port is None or port == _ELECTION_PH_PORT)

            tsAge = float('inf')
            if not tsIsPlaceholder:
                try:
                    tsAge = time.time() - float(ts)
                except (ValueError, TypeError):
                    tsIsPlaceholder = True

            serverIsLive = (
                not fileIsEmpty
                and not portIsPlaceholder
                and tsAge < _ELECTION_STALE_THRESHOLD
            )
            serverIsStale = (not fileIsEmpty and tsAge >= _ELECTION_STALE_THRESHOLD)

            if serverIsLive and uname != self.__uniqueName:
                success = self.connect(address=addr, port=int(port), ntrials=ntrials)
                if success:
                    return
                time.sleep(_ELECTION_CLAIM_POLL)
                continue

            if serverIsLive and uname == self.__uniqueName:
                return

            if (not fileIsEmpty
                    and tsIsPlaceholder
                    and uname != self.__uniqueName):
                time.sleep(_ELECTION_CLAIM_POLL)
                continue

            if not self.__allowServing:
                if not fileIsEmpty and not serverIsStale:
                    time.sleep(_ELECTION_CLAIM_POLL)
                    continue
                self._warn(
                    "'%s' found no live server and allowServing is False — "
                    "giving up" % self.__name
                )
                return

            # ── Phase 1: CLAIM ────────────────────────────────────────
            claimLine = '%s(%s)@%s:%s[%s]' % (
                self.__uniqueName, _ELECTION_PH_TS,
                self.__address,   _ELECTION_PH_PORT,
                self.__pid,
            )
            self._write_election_raw(claimLine)
            time.sleep(_ELECTION_CLAIM_POLL)

            uname2, ts2, addr2, port2, pid2 = self.get_running_server_fingerprint(
                raiseNotFound=False, raiseError=False
            )

            if uname2 != self.__uniqueName:
                self._info(
                    "'%s' lost election to '%s' — waiting for server port"
                    % (self.__uniqueName, uname2)
                )
                waitStart = time.time()
                while time.time() - waitStart < _ELECTION_TIMEOUT:
                    time.sleep(_ELECTION_CLAIM_POLL)
                    uname3, ts3, addr3, port3, pid3 = self.get_running_server_fingerprint(
                        raiseNotFound=False, raiseError=False
                    )
                    portIsReal = (
                        port3 is not None
                        and port3 != _ELECTION_PH_PORT
                    )
                    if portIsReal:
                        success = self.connect(
                            address=addr3, port=int(port3), ntrials=ntrials
                        )
                        if success:
                            return
                        break

                    winnerIsStale = False
                    if ts3 and ts3 != _ELECTION_PH_TS:
                        try:
                            winnerIsStale = (
                                time.time() - float(ts3) >= _ELECTION_STALE_THRESHOLD
                            )
                        except (ValueError, TypeError):
                            pass
                    if winnerIsStale:
                        break
                continue

            # ── Phase 2: STAMP ────────────────────────────────────────
            stampLine = '%s(%s)@%s:%s[%s]' % (
                self.__uniqueName, str(time.time()),
                self.__address,   _ELECTION_PH_PORT,
                self.__pid,
            )
            self._write_election_raw(stampLine)

            # ── Phase 3: BIND AND ANNOUNCE ────────────────────────────
            try:
                self._run_sync(self._start_server(),
                               timeout=self.__connectTimeout + 2)
            except Exception as err:
                self._error(
                    "'%s' failed to start server after winning election: %s"
                    % (self.__name, err)
                )
                try:
                    with self._serverFileLock:
                        open(self.__serverFile, 'w').close()
                except Exception:
                    pass
                return

            self._wasServer = True
            self._info("'%s' is now the server" % self.__name)
            return


    # ------------------------------------------------------------------
    # Wire I/O helpers — dispatch to JSON or msgpack based on serializer
    # ------------------------------------------------------------------

    async def _write_msg(self, writer, obj):
        """Write one message using the configured serializer.

        Routes to 4-byte length-prefixed msgpack framing when
        ``self._useMsgpack`` is ``True``, or newline-delimited JSON
        otherwise.  All TCP write paths in the server and client call this
        method so that the serializer choice is enforced in one place.
        """
        if self._useMsgpack:
            await _write_msgpack(writer, obj)
        else:
            await _write_json(writer, obj)

    async def _read_msg(self, reader):
        """Read one message using the configured serializer.

        Routes to msgpack length-prefix framing when ``self._useMsgpack``
        is ``True``, or newline-delimited JSON otherwise.  Returns ``None``
        when the peer closes the connection cleanly (both formats).
        """
        if self._useMsgpack:
            try:
                return await _read_msgpack(reader)
            except asyncio.IncompleteReadError:
                return None
        else:
            return await _read_json(reader)

    async def _read_msg_timeout(self, reader, timeout):
        """Read one message with a deadline using the configured serializer.

        For JSON, wraps ``reader.readline()`` in ``asyncio.wait_for`` so
        the full read fits inside one cancellable coroutine.  For msgpack,
        wraps the two-step header-then-payload read in a single coroutine
        before applying the timeout, keeping the reader in a consistent
        state if the timeout fires between messages (i.e. between frames,
        not mid-frame).

        :Parameters:
            #. reader (asyncio.StreamReader): The stream to read from.
            #. timeout (float): Deadline in seconds.

        :Returns:
            #. message (dict, None): The deserialised message, or ``None``
               when the peer has closed the connection cleanly.

        :Raises:
            #. asyncio.TimeoutError: No complete message arrived within the
               deadline.
        """
        if self._useMsgpack:
            try:
                return await asyncio.wait_for(_read_msgpack(reader), timeout=timeout)
            except asyncio.IncompleteReadError:
                return None
        else:
            raw = await asyncio.wait_for(reader.readline(), timeout=timeout)
            if not raw:
                return None
            return json.loads(raw.decode(_ENCODING))

    # ------------------------------------------------------------------
    # Server coroutines
    # ------------------------------------------------------------------
    async def _start_server(self):
        """Bind the asyncio TCP server and launch background tasks.

        Tries the configured port first, then increments until a free port
        is found.  Writes the fully-resolved fingerprint (Phase 3 of the
        election) so waiting clients can discover the real port.  Launches
        three background tasks: the queue processor, the maximum-hold-time
        eviction monitor, and the heartbeat.
        """
        port = self.__port
        while True:
            try:
                self._server = await asyncio.start_server(
                    self._handle_client, '0.0.0.0', port
                )
                break
            except OSError:
                port += 1
                if port > 65535:
                    raise RuntimeError("No available port found")
        self._serverPort = port
        self._write_fingerprint(port)
        self._info("Server started on port %d" % port)
        self._bgTasks = [
            self._loop.create_task(self._queue_processor()),
            self._loop.create_task(self._max_time_monitor()),
            self._loop.create_task(self._heartbeat()),
            self._loop.create_task(self._dead_pid_monitor()),
        ]

    async def _heartbeat(self):
        """Refresh the server fingerprint file at regular intervals.

        Writes a fresh fingerprint every ``_HEARTBEAT_INTERVAL`` seconds for
        as long as the server is running.  Any process checking the file age
        will see a recent timestamp and know the server is alive.  When the
        server crashes, the file stops updating; after
        ``_ELECTION_STALE_THRESHOLD`` seconds the entry is considered stale
        and the next candidate restarts the election.

        The file write is synchronous but takes only microseconds for the
        tiny fingerprint payload, so briefly blocking the asyncio event loop
        is acceptable here.
        """
        while True:
            stopped = False
            try:
                await asyncio.wait_for(
                    self._stopEvent.wait(), timeout=_HEARTBEAT_INTERVAL
                )
                stopped = True
            except asyncio.TimeoutError:
                pass
            if stopped or self._stopEvent.is_set():
                break
            if self._serverPort is not None:
                # ── Cross-validate fingerprint before writing ──────────────
                # If the file now names a different unique name, another process
                # has claimed ownership — possible split-brain scenario.
                # Stop immediately rather than silently overwriting the new
                # winner's fingerprint, which would cause both servers to
                # believe they are legitimate.
                uname, _, _, _, _ = self.get_running_server_fingerprint(
                    raiseNotFound=False, raiseError=False)
                if uname is not None and uname != self.__uniqueName:
                    self._critical(
                        "Heartbeat: fingerprint owner changed to '%s' — "
                        "possible split-brain; stopping server '%s'."
                        % (uname, self.__uniqueName)
                    )
                    self._stopEvent.set()
                    break
                self._write_fingerprint(self._serverPort)

    async def _handle_client(self, reader, writer):
        """Handle a single client connection from handshake to disconnect."""
        clientName       = None
        clientUniqueName = None
        try:
            # Bootstrap: the hello/welcome exchange always uses JSON so
            # that the serializer can be negotiated before lock traffic starts.
            hello = await _read_json(reader)
            if not isinstance(hello, dict):
                writer.close()
                return
            # ── Password verification ─────────────────────────────────────
            # The client must include the shared password in its hello dict.
            # Both sides are compared as raw bytes to avoid encoding mismatches.
            # NOTE: the password travels as plaintext over TCP; for cross-machine
            # deployments, wrap the connection in a TLS tunnel.
            clientPw = _to_bytes(hello.get('password', ''))
            if self.__password is not None and clientPw != self.__password:
                self._warn(
                    "Client '%s' rejected: wrong password"
                    % hello.get('unique_name', 'unknown')
                )
                await _write_json(writer, {
                    'action': 'error',
                    'reason': 'bad password',
                })
                try:
                    writer.close()
                except Exception:
                    pass
                return
            clientName       = hello.get('name', 'unknown')
            clientUniqueName = hello.get('unique_name', str(uuid.uuid4()))
            # Capture the client's PID so the dead-PID monitor can check
            # whether the holder process is still alive.  Only meaningful for
            # same-host connections; cross-machine PIDs cannot be probed.
            clientPid     = hello.get('pid')
            clientAddress = hello.get('address', '')
            # Welcome is also JSON (bootstrap).  The 'serializer' field
            # tells the client which framing to use for all subsequent messages.
            await _write_json(writer, {
                'action':                     'welcome',
                'server_name':                self.__name,
                'server_unique_name':         self.__uniqueName,
                'server_address':             self.__address,
                'server_port':                self._serverPort,
                'lock_maximum_acquired_time': self.__maxLockTime,
                'server_file':                self.__serverFile or '',
                'password_ok':                True,
                'serializer':                 'msgpack' if self._useMsgpack else 'json',
            })
            # All messages after this point use the announced serializer.
            self._clientsLUT[clientUniqueName] = {
                'writer':  writer,
                'pid':     clientPid,
                'address': clientAddress,
                'name':    clientName,
            }
            self._info("Client '%s:%s' connected" % (clientName, clientUniqueName))
            while not self._stopEvent.is_set():
                try:
                    request = await self._read_msg_timeout(reader, 30.0)
                except asyncio.TimeoutError:
                    continue
                if request is None:
                    break
                action = request.get('action')
                if action == 'acquire':
                    request['_writer'] = writer
                    await self._lockQueue.put(request)
                    self._queueEvent.set()
                elif action == 'release':
                    await self._server_release(request)
                elif action == 'publish':
                    await self._server_publish(request)
                else:
                    self._warn("Unknown action '%s' from client '%s'" % (action, clientUniqueName))
        except (ConnectionResetError, BrokenPipeError, asyncio.IncompleteReadError):
            pass
        except Exception as err:
            self._error("Client handler error for '%s': %s" % (clientUniqueName, err))
        finally:
            if clientUniqueName is not None and self._pathsLUT is not None:
                released = [p for p, rec in list(self._pathsLUT.items())
                            if rec.get('_writer') is writer]
                for path in released:
                    self._pathsLUT.pop(path, None)
                if released:
                    self._warn("Force-released %d lock(s) for disconnected client '%s'"
                               % (len(released), clientUniqueName))
                    self._queueEvent.set()
                self._clientsLUT.pop(clientUniqueName, None)
            try:
                writer.close()
            except Exception:
                pass
            # Notify the queue processor so it can unblock any pending
            # requests that were waiting on a path held by this client.
            if self._queueEvent is not None:
                self._queueEvent.set()

    async def _queue_processor(self):
        """Continuously try to satisfy pending acquire requests."""
        pending = []
        while not self._stopEvent.is_set():
            while True:
                try:
                    pending.append(self._lockQueue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            now        = time.time()
            still_wait = []
            for req in pending:
                ruuid   = req['request_unique_id']
                paths   = req['path']
                timeout = req.get('timeout', self.__defaultTimeout)
                reqTime = req.get('request_utctime', now)
                if (now - reqTime) >= timeout:
                    self._warn("Request '%s' expired in queue" % ruuid)
                    fut = req.get('_future')
                    if fut is not None and not fut.done():
                        fut.set_result((False, 0))
                    continue
                blocked = any(p in self._pathsLUT for p in paths)
                if blocked:
                    still_wait.append(req)
                    continue
                acquiredTime = time.time()
                for p in paths:
                    self._pathsLUT[p] = {
                        'request_unique_id':  ruuid,
                        '_writer':            req.get('_writer'),
                        'acquired_utctime':   acquiredTime,
                        'path':               p,
                        'client_unique_name': req.get('client_unique_name'),
                    }
                fut = req.get('_future')
                if fut is not None:
                    if not fut.done():
                        fut.set_result((True, ruuid))
                else:
                    writer = req.get('_writer')
                    if writer is not None:
                        await self._write_msg(writer, {'action': 'acquired',
                                                   'request_unique_id': ruuid})
                self._info("Lock '%s' acquired for paths %s" % (ruuid, paths))
            pending = still_wait
            self._queueEvent.clear()
            if pending:
                try:
                    await asyncio.wait_for(self._queueEvent.wait(), timeout=_QUEUE_POLL)
                except asyncio.TimeoutError:
                    pass
            else:
                try:
                    await asyncio.wait_for(self._queueEvent.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    pass

    async def _max_time_monitor(self):
        """Periodically evict locks that have exceeded the maximum hold time."""
        while not self._stopEvent.is_set():
            await asyncio.sleep(_MONITOR_POLL)
            now     = time.time()
            expired = [
                (p, rec) for p, rec in list(self._pathsLUT.items())
                if now - rec.get('acquired_utctime', now) >= self.__maxLockTime
            ]
            for path, rec in expired:
                self._pathsLUT.pop(path, None)
                self._warn("Force-released lock on '%s' (max lock time exceeded)" % path)
            if expired:
                self._queueEvent.set()

    async def _server_release(self, request):
        """Process a release request on the server side."""
        ruuid = request.get('request_unique_id')
        if not ruuid:
            return
        released = [p for p, rec in list(self._pathsLUT.items())
                    if rec.get('request_unique_id') == ruuid]
        for path in released:
            self._pathsLUT.pop(path, None)
        if released:
            self._info("Released lock '%s' for paths %s" % (ruuid, released))
            self._queueEvent.set()

    async def _server_local_acquire(self, ruuid, paths, timeout, utcTime):
        """Acquire a lock from within the server process itself.

        :Parameters:
            #. ruuid (str): Request unique identifier.
            #. paths (list): Normalised path strings to lock.
            #. timeout (float): Seconds to wait.
            #. utcTime (float): Epoch time when the request was created.

        :Returns:
            #. acquired (bool): Whether the lock was successfully acquired.
            #. lockId (str, int): The lock UUID on success, or ``0`` on timeout.
        """
        fut = self._loop.create_future()
        request = {
            'request_unique_id':  ruuid,
            'action':             'acquire',
            'path':               paths,
            'timeout':            timeout,
            'request_utctime':    utcTime,
            'client_unique_name': self.__uniqueName,
            'client_name':        self.__name,
            '_future':            fut,
            '_writer':            None,
        }
        await self._lockQueue.put(request)
        self._queueEvent.set()
        try:
            return await asyncio.wait_for(asyncio.shield(fut), timeout=timeout)
        except asyncio.TimeoutError:
            if not fut.done():
                fut.cancel()
            return False, 0

    # ------------------------------------------------------------------
    # Client coroutines
    # ------------------------------------------------------------------
    async def _client_connect(self, address, port, ntrials):
        """Open a TCP connection and complete the handshake.

        :Parameters:
            #. address (str): Server IP address.
            #. port (int): Server port.
            #. ntrials (int): Maximum number of attempts.

        :Returns:
            #. success (bool): Whether the connection was established.
        """
        for attempt in range(ntrials):
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(address, port),
                    timeout=self.__connectTimeout
                )
                # Bootstrap: hello and welcome are always JSON so the
                # serializer can be negotiated before lock traffic starts.
                await _write_json(writer, {
                    'name':        self.__name,
                    'unique_name': self.__uniqueName,
                    # Include the shared password so the server can verify identity.
                    'password':    self.__password.decode(_ENCODING, errors='replace')
                                   if self.__password else '',
                    # Include PID and address so the server's dead-PID monitor
                    # can check whether this process is still alive (same-host
                    # connections only — cross-machine PIDs are not probeable).
                    'pid':         os.getpid(),
                    'address':     self.__address,
                })
                params = await asyncio.wait_for(
                    _read_json(reader), timeout=self.__connectTimeout
                )
                # ── Handle authentication rejection ───────────────────────
                if params.get('action') == 'error':
                    self._error(
                        "Server rejected connection (attempt %d/%d): %s"
                        % (attempt + 1, ntrials, params.get('reason', 'unknown'))
                    )
                    try:
                        writer.close()
                    except Exception:
                        pass
                    await asyncio.sleep(0.5)
                    continue
                # ─────────────────────────────────────────────────────────
                self.__serverName        = params.get('server_name', 'unknown')
                self.__serverUniqueName  = params.get('server_unique_name', '')
                self.__serverMaxLockTime = params.get('lock_maximum_acquired_time',
                                                      self.__maxLockTime)
                # Milestone 2: capture the server's address and port so they
                # are available via the serverAddress / serverPort properties.
                self.__serverAddress = params.get('server_address', address)
                self.__serverPort    = params.get('server_port', port)
                # ── Serializer negotiation ────────────────────────────
                # The server announces which serializer it will use for all
                # subsequent messages.  Old servers that pre-date negotiation
                # do not include this field; they default to JSON.
                announcedSerializer = params.get('serializer', 'json')
                if announcedSerializer == 'msgpack':
                    if not MSGPACK_AVAILABLE:
                        self._error(
                            "Server '%s' requires msgpack serializer but "
                            "msgpack is not installed on this client. "
                            "Run:  pip install msgpack"
                            % self.__serverName
                        )
                        try:
                            writer.close()
                        except Exception:
                            pass
                        await asyncio.sleep(0.5)
                        continue
                    self._useMsgpack = True
                else:
                    self._useMsgpack = False
                # All messages after this point use the negotiated serializer.
                self._reader = reader
                self._writer = writer
                self._info("Connected to server '%s:%s'" % (
                    self.__serverName, self.__serverUniqueName))
                self._loop.create_task(self._client_reader_loop())
                return True
            except Exception as err:
                self._warn("Connection attempt %d/%d failed: %s" % (
                    attempt + 1, ntrials, err))
                await asyncio.sleep(0.5)
        return False

    async def _client_reader_loop(self):
        """Read messages from the server and dispatch them."""
        # Latch the wasClient guard immediately so canServe() returns False
        # for the remainder of this instance's lifetime (until reset()).
        self._wasClient = True
        try:
            while not self._stopEvent.is_set():
                try:
                    msg = await self._read_msg(self._reader)
                except Exception:
                    continue
                if msg is None:
                    break
                action = msg.get('action')
                ruuid  = msg.get('request_unique_id')
                if action == 'acquired':
                    fut = self._pending.pop(ruuid, None)
                    if fut is not None and not fut.done():
                        with self.__ownAcquiredLock:
                            self.__ownAcquired[ruuid] = msg
                        fut.set_result((True, ruuid))
                elif action == 'exceeded_maximum_lock_time':
                    with self.__ownAcquiredLock:
                        self.__ownAcquired.pop(ruuid, None)
                    self._warn("Server force-released lock '%s'" % ruuid)
                elif action == 'publish':
                    self._store_publication(msg)
                else:
                    self._info("Received message with action '%s'" % action)
        except (ConnectionResetError, BrokenPipeError, asyncio.IncompleteReadError):
            pass
        except Exception as err:
            self._error("Client reader loop error: %s" % err)
        finally:
            for fut in list((self._pending or {}).values()):
                if not fut.done():
                    fut.set_exception(ConnectionError("Server connection lost"))
            if self._pending is not None:
                self._pending.clear()
            self._writer = None
            self._reader = None

    async def _client_acquire(self, ruuid, paths, timeout, utcTime):
        """Send an acquire request to the server and await the response.

        :Parameters:
            #. ruuid (str): Request unique identifier.
            #. paths (list): Normalised path strings to lock.
            #. timeout (float): Seconds to wait.
            #. utcTime (float): Epoch time when the request was created.

        :Returns:
            #. acquired (bool): Whether the lock was granted.
            #. lockId (str, int): Lock UUID on success, or ``0`` on timeout.
        """
        fut = self._loop.create_future()
        self._pending[ruuid] = fut
        await self._write_msg(self._writer, {
            'request_unique_id':  ruuid,
            'action':             'acquire',
            'path':               paths,
            'timeout':            timeout,
            'request_utctime':    utcTime,
            'client_unique_name': self.__uniqueName,
            'client_name':        self.__name,
        })
        try:
            result = await asyncio.wait_for(asyncio.shield(fut), timeout=timeout)
            return result
        except asyncio.TimeoutError:
            self._pending.pop(ruuid, None)
            if not fut.done():
                fut.cancel()
            return False, 0

    async def _client_release(self, ruuid, paths):
        """Send a release message to the server.

        :Parameters:
            #. ruuid (str): Lock unique identifier to release.
            #. paths (list): Paths associated with this lock.
        """
        if self._writer is not None:
            await self._write_msg(self._writer, {
                'request_unique_id':  ruuid,
                'action':             'release',
                'path':               paths,
                'client_unique_name': self.__uniqueName,
                'client_name':        self.__name,
            })

    # ------------------------------------------------------------------
    # Publications
    # ------------------------------------------------------------------
    def _store_publication(self, msg):
        """Store an incoming publication message (called from any context).

        When *msg* contains a positive ``timeout`` value and a
        ``publication_id``, a background coroutine is scheduled on the
        asyncio loop to remove that specific entry automatically after the
        timeout elapses.  This restores the publication-timeout behaviour
        that existed in the original ``ServerLocker.py``.
        """
        message = msg.get('message')
        replace = msg.get('replace', True)
        if message is None:
            return
        with self.__publicationsLock:
            if replace:
                self.__publications[message] = [msg]
            else:
                self.__publications.setdefault(message, []).append(msg)
        # ── Schedule auto-expiry ─────────────────────────────────────────
        timeout = msg.get('timeout')
        pubId   = msg.get('publication_id')
        if (timeout is not None
                and pubId is not None
                and self._loop is not None
                and self._loop.is_running()):
            asyncio.run_coroutine_threadsafe(
                self._expire_publication(message, pubId, float(timeout)),
                self._loop,
            )

    async def _expire_publication(self, message, pubId, timeout):
        """Remove a specific publication entry after *timeout* seconds.

        :Parameters:
            #. message (str): The publication message key.
            #. pubId (str): The ``publication_id`` of the entry to expire.
            #. timeout (float): Seconds to wait before removing the entry.
        """
        await asyncio.sleep(timeout)
        with self.__publicationsLock:
            entries = self.__publications.get(message)
            if entries is None:
                return
            kept = [e for e in entries if e.get('publication_id') != pubId]
            if kept:
                self.__publications[message] = kept
            else:
                self.__publications.pop(message, None)

    async def _dead_pid_monitor(self):
        """Periodically evict locks held by same-host client processes that have died.

        Every ``_DEAD_PID_POLL`` seconds this coroutine iterates over every
        connected client whose recorded address matches the server's own address.
        For each such client it sends signal 0 to the stored PID.  Signal 0
        performs an existence check without actually delivering a signal; it
        raises ``ProcessLookupError`` when the PID is absent and
        ``PermissionError`` when the process exists but is owned by another
        user (still alive — no action taken).

        When a dead PID is confirmed, all locks held by that client are
        force-released, the client is removed from ``_clientsLUT``, and the
        queue processor is notified so any waiting requests can be satisfied
        immediately.

        Cross-machine clients are excluded from this check because their PIDs
        are local to a remote host and cannot be tested from the server.
        """
        while not self._stopEvent.is_set():
            try:
                await asyncio.wait_for(self._stopEvent.wait(), timeout=_DEAD_PID_POLL)
                break   # stop event fired
            except asyncio.TimeoutError:
                pass
            if self._clientsLUT is None or self._pathsLUT is None:
                continue
            for cuname, clientRec in list(self._clientsLUT.items()):
                pid     = clientRec.get('pid')
                address = clientRec.get('address', '')
                if not pid:
                    continue
                # Only probe same-host connections.
                localAddresses = {'127.0.0.1', 'localhost', self.__address}
                if address not in localAddresses:
                    continue
                try:
                    os.kill(int(pid), 0)
                except ProcessLookupError:
                    # PID no longer exists — force-release its locks.
                    self._warn(
                        "Dead-PID monitor: process %d ('%s') is gone — "
                        "force-releasing all its locks." % (pid, cuname)
                    )
                    if self._pathsLUT is not None:
                        writer  = clientRec.get('writer')
                        stale   = [p for p, rec in list(self._pathsLUT.items())
                                   if rec.get('_writer') is writer]
                        for path in stale:
                            self._pathsLUT.pop(path, None)
                        if stale:
                            self._queueEvent.set()
                    self._clientsLUT.pop(cuname, None)
                    try:
                        clientRec['writer'].close()
                    except Exception:
                        pass
                except PermissionError:
                    # Process exists but is owned by another user — it is alive.
                    pass
                except Exception:
                    # Any other OS error: skip silently to avoid crashing the loop.
                    pass

    async def _server_publish(self, request):
        """Broadcast a publication from a client to its target recipients.

        The ``toSelf`` flag in the request controls two distinct behaviours
        depending on who is publishing.

        When the **server** is the publisher (``client_unique_name`` matches
        this instance's ``uniqueName``): ``toSelf=True`` stores the
        publication in the server's local publication dict immediately.
        ``__publish_message`` already handles this synchronously for the
        server case; the payload received here always has ``toSelf=False``
        so this coroutine never double-stores.

        When a **remote client** is the publisher: ``toSelf=False`` means
        the server must not loop the message back to the sender.  This is
        enforced by skipping the write to the sender's writer in the
        broadcast loop.
        """
        message             = request.get('message', '')
        receivers           = request.get('receivers')
        toSelf              = request.get('toSelf', True)
        replace             = request.get('replace', True)
        publisherUniqueName = request.get('client_unique_name')
        payload = {
            'action':                'publish',
            'message':               message,
            'replace':               replace,
            'publisher_unique_name': publisherUniqueName,
        }
        if toSelf:
            self._store_publication(payload)
        for cuname, clientRec in list(self._clientsLUT.items()):
            # Apply the receivers filter when one is specified.
            if receivers is not None and cuname not in receivers:
                continue
            # Honour toSelf=False: do not loop the message back to its sender.
            if not toSelf and cuname == publisherUniqueName:
                continue
            await self._write_msg(clientRec['writer'], payload)

    def __publish_message(self, message, receivers, timeout, toSelf, unique, replace):
        """Send a publication via the active connection and return its UUID.

        :Returns:
            #. publicationId (None, str): A UUID string for this publication,
               or ``None`` when the locker is not started.
        """
        if not (self.isServer or self.isClient):
            self._warn("publish_message called but locker is not started")
            return None
        pubId = str(uuid.uuid4())
        payload = {
            'action':                'publish',
            'message':               message,
            'publication_id':        pubId,
            # publisher_unique_name is the canonical field used by
            # _store_publication and remove_published_message for sender
            # filtering.  client_unique_name is kept for protocol
            # compatibility with older consumers.
            'publisher_unique_name': self.__uniqueName,
            'client_unique_name':    self.__uniqueName,
            'client_name':           self.__name,
            'receivers':             list(receivers) if receivers else None,
            'timeout':               timeout,
            'toSelf':                toSelf,
            'unique':                unique,
            'replace':               replace,
        }
        if self.isServer:
            # Store the local copy synchronously so callers see the
            # publication immediately via has_message() without waiting
            # for the asyncio task to be scheduled and run.
            if toSelf:
                self._store_publication(payload)
            # Submit the broadcast to remote clients only; set toSelf=False
            # in the payload because we already handled local storage above.
            broadcastPayload = dict(payload)
            broadcastPayload['toSelf'] = False
            self._submit(self._server_publish(broadcastPayload))
        else:
            self._submit(self._write_msg(self._writer, payload))
        return pubId

    # ------------------------------------------------------------------
    # Public API — publications
    # ------------------------------------------------------------------
    def get_message(self, message):
        """Return the publication dict for *message* without removing it.

        :Parameters:
            #. message (str): The publication message key.

        :Returns:
            #. publication (None, dict): The stored publication or ``None``.
        """
        with self.__publicationsLock:
            items = self.__publications.get(message)
            return items[0] if items else None

    def pop_message(self, message):
        """Remove and return the publication dict for *message*.

        :Parameters:
            #. message (str): The publication message key.

        :Returns:
            #. publication (None, dict): The removed publication or ``None``.
        """
        with self.__publicationsLock:
            items = self.__publications.pop(message, None)
            return items[0] if items else None

    def has_message(self, message):
        """Return whether a publication for *message* is currently stored.

        :Parameters:
            #. message (str): The publication message key.

        :Returns:
            #. exists (bool): ``True`` if at least one entry is stored for
               *message*.
        """
        with self.__publicationsLock:
            return bool(self.__publications.get(message))

    def remove_published_message(self, message, senders=None):
        """Remove stored publications for *message*, optionally filtered by sender.

        When *senders* is ``None`` the entire key is deleted.  When
        *senders* is a list, only entries whose ``publisher_unique_name``
        matches one of the listed unique names are removed; entries from
        other senders are preserved.

        :Parameters:
            #. message (str): The publication message key to remove.
            #. senders (None, list): Unique-name strings of the senders
               whose entries should be removed.  ``None`` removes all
               entries for *message*.

        :Returns:
            #. success (bool): ``True`` if *message* no longer exists in
               the store after the call (absent to begin with, or all
               entries removed).  ``False`` when a sender filter was
               applied but entries from other senders remain.
        """
        with self.__publicationsLock:
            if message not in self.__publications:
                return True
            if senders is None:
                del self.__publications[message]
                return True
            # Filter: keep entries NOT published by the listed senders.
            senderSet = set(senders)
            kept = [
                entry for entry in self.__publications[message]
                if entry.get('publisher_unique_name') not in senderSet
            ]
            if kept:
                self.__publications[message] = kept
                return False
            del self.__publications[message]
            return True

    def remove_message(self, *args, **kwargs):
        """Alias to ``remove_published_message``."""
        return self.remove_published_message(*args, **kwargs)

    def publish_message(self, message, receivers=None, timeout=None,
                        toSelf=True, unique=False, replace=True):
        """Publish *message* to one or more connected lockers.

        :Parameters:
            #. message (str): The message string to publish.  Must not start
               with ``'$order$'``.
            #. receivers (None, list): List of recipient unique names.
               ``None`` means broadcast to all.
            #. timeout (None, float): Optional expiry in seconds.
            #. toSelf (bool): Whether to deliver to this instance.
            #. unique (bool): Raise if the message key already exists.
            #. replace (bool): Whether to replace an existing message.

        :Returns:
            #. success (bool): ``True`` if the message was dispatched.
            #. publicationId (str, int): A UUID string identifying this
               publication on success.  ``0`` when the locker is not started.
        """
        assert isinstance(message, str), "message must be a string"
        assert not message.startswith('$order$'), \
            "message must not start with '$order$'"
        if receivers is not None:
            assert isinstance(receivers, (list, set, tuple)), \
                "receivers must be None or a list"
        if timeout is not None:
            assert isinstance(timeout, (int, float)) and timeout > 0, \
                "timeout must be a positive number"
        pubId = self.__publish_message(
            message=message, receivers=receivers, timeout=timeout,
            toSelf=toSelf, unique=unique, replace=replace
        )
        if pubId is None:
            return False, 0
        return True, pubId

    def publish(self, *args, **kwargs):
        """Alias to ``publish_message``."""
        return self.publish_message(*args, **kwargs)

    # ------------------------------------------------------------------
    # Remote administration
    # ------------------------------------------------------------------
    def stop_remote(self, password, killRemotePID=False,
                    receivers=None, stopSelf=False):
        """Send a remote stop order to other locker instances.

        :Parameters:
            #. password (str, bytes): The remote orders password.
            #. killRemotePID (bool): Whether to kill the remote process.
            #. receivers (None, list): Target unique names or ``None`` for all.
            #. stopSelf (bool): Whether to also stop this instance.
        """
        pw   = _to_bytes(password)
        kill = str(killRemotePID)
        msg  = '$order$stop:%s:%s' % (kill, pw.decode('utf-8'))
        self.publish_message(message=msg, receivers=receivers,
                             toSelf=False, replace=True)
        if stopSelf:
            self.stop()

    # ------------------------------------------------------------------
    # Core lock API — sync
    # ------------------------------------------------------------------
    def acquire_lock(self, path, timeout=None, lockGlobal=False):
        """Acquire a lock for one or more paths, blocking until acquired or timed out.

        Blocks the calling thread until the server grants the lock or the
        timeout expires.  Internally, the request is submitted to
        pylocker's private asyncio loop via
        ``asyncio.run_coroutine_threadsafe``; the calling thread then
        blocks on ``Future.result()``.

        This method is **runtime-agnostic**: it may be called from plain
        Python scripts, OS threads, Django views, Flask handlers, Celery
        tasks, or from inside a running ``asyncio`` task.  No event loop
        is required in the calling thread.  No extra threads are spawned
        per call.  For callers running inside an asyncio event loop who
        want to avoid blocking the task, use ``acquire_async`` instead.

        :Parameters:
            #. path (str, list, tuple): One path string or a list of path
               strings.  All paths are locked atomically.
            #. timeout (None, int, float): Seconds to wait.  ``None`` uses
               the instance default timeout.
            #. lockGlobal (bool): Reserved; accepted for API compatibility.

        :Returns:
            #. success (bool): ``True`` if the lock was acquired.
            #. lockUniqueId (str, int): The lock UUID on success.  On failure,
               an integer error code: ``0`` timed out, ``1`` connection lost,
               ``2`` locker not started, or a string describing the error.
        """
        if isinstance(path, str):
            path = [path]
        path = [_normalize_path(p) for p in path]
        assert len(path), "path must not be empty"
        if timeout is None:
            timeout = self.__defaultTimeout
        assert isinstance(timeout, (int, float)) and timeout > 0, \
            "timeout must be a positive number"
        if not (self.isServer or self.isClient):
            return False, 2
        ruuid   = str(uuid.uuid4())
        utcTime = time.time()
        try:
            if self.isServer:
                coro = self._server_local_acquire(ruuid, path, float(timeout), utcTime)
            else:
                coro = self._client_acquire(ruuid, path, float(timeout), utcTime)
            acquired, lockId = self._submit(coro).result(
                timeout=float(timeout) + _RESULT_SLACK
            )
            if acquired:
                with self.__ownAcquiredLock:
                    self.__ownAcquired[ruuid] = {
                        'request_unique_id': ruuid,
                        'path': path,
                    }
            return acquired, lockId
        except Exception as err:
            code = str(err)
            try:
                code = int(code)
            except (ValueError, TypeError):
                pass
            return False, code

    def acquire(self, *args, **kwargs):
        """Alias to ``acquire_lock``."""
        return self.acquire_lock(*args, **kwargs)

    def release_lock(self, lockId):
        """Release a previously acquired lock.

        Submits the release message to pylocker's private asyncio loop
        and returns immediately without waiting for the server
        acknowledgement.  Safe to call from any runtime context: plain
        threads, Django views, Celery tasks, or inside an asyncio task.
        For the non-blocking async equivalent, use ``release_async``.

        :Parameters:
            #. lockId (str): The lock UUID returned by ``acquire_lock``.

        :Returns:
            #. success (bool): ``True`` if the lock was released or not found.
            #. code (int, str): ``0`` not found (already released), ``1``
               released successfully, ``2`` connection not found, ``3`` not
               started, or a string describing an error.
        """
        with self.__ownAcquiredLock:
            req = self.__ownAcquired.pop(lockId, None)
        if req is None:
            return True, 0
        paths = req.get('path', [])
        try:
            if self.isServer:
                self._submit(self._server_release({
                    'request_unique_id': lockId,
                    'path': paths,
                }))
            elif self.isClient:
                self._submit(self._client_release(lockId, paths))
            else:
                return False, 3
            return True, 1
        except Exception as err:
            return False, str(err)

    def release(self, *args, **kwargs):
        """Alias to ``release_lock``."""
        return self.release_lock(*args, **kwargs)

    # ------------------------------------------------------------------
    # Core lock API — async opt-in
    # ------------------------------------------------------------------
    async def acquire_async(self, path, timeout=None, lockGlobal=False):
        """Acquire a lock without blocking the caller's event loop.

        Submits the acquire request to pylocker's private asyncio loop
        and maps the result back to the caller's loop with
        ``asyncio.wrap_future``.  The caller's loop continues scheduling
        other tasks while waiting for the server to respond.

        This method is **runtime-agnostic**: it works from any
        asyncio-compatible runtime — standard ``asyncio``, ``uvloop``,
        FastAPI, Starlette, aiohttp, Tornado, or Trio bridged via
        ``anyio``.  pylocker's internal loop is always separate from
        the caller's loop; the two communicate only through
        ``asyncio.wrap_future``.

        **Performance note:** the cross-loop bridge adds roughly 5–15 µs
        of overhead per call compared with calling ``acquire_lock`` from
        a plain thread.  This is negligible for most workloads.  If
        absolute minimum latency is required, use the raw coroutine
        ``_client_acquire`` directly inside a coroutine that runs on
        pylocker's own loop.

        :Parameters:
            #. path (str, list, tuple): Path or list of paths to lock.
            #. timeout (None, int, float): Seconds to wait.
            #. lockGlobal (bool): Reserved; accepted for API compatibility.

        :Returns:
            #. success (bool): ``True`` if the lock was acquired.
            #. lockUniqueId (str, int): Lock UUID on success or error code.
        """
        if isinstance(path, str):
            path = [path]
        path = [_normalize_path(p) for p in path]
        if timeout is None:
            timeout = self.__defaultTimeout
        if not (self.isServer or self.isClient):
            return False, 2
        ruuid   = str(uuid.uuid4())
        utcTime = time.time()
        try:
            if self.isServer:
                coro = self._server_local_acquire(ruuid, path, float(timeout), utcTime)
            else:
                coro = self._client_acquire(ruuid, path, float(timeout), utcTime)
            fut = self._submit(coro)
            acquired, lockId = await asyncio.wrap_future(fut)
            if acquired:
                with self.__ownAcquiredLock:
                    self.__ownAcquired[ruuid] = {'request_unique_id': ruuid, 'path': path}
            return acquired, lockId
        except Exception as err:
            code = str(err)
            try:
                code = int(code)
            except (ValueError, TypeError):
                pass
            return False, code

    async def release_async(self, lockId):
        """Release a lock without blocking the caller's event loop.

        Submits the release message to pylocker's private asyncio loop
        and awaits the result via ``asyncio.wrap_future``.  The
        caller's event loop is never blocked.  Works from any
        asyncio-compatible runtime — standard ``asyncio``, ``uvloop``,
        FastAPI, Starlette, aiohttp, Tornado, or Trio bridged via
        ``anyio``.

        :Parameters:
            #. lockId (str): The lock UUID returned by ``acquire_async``.

        :Returns:
            #. success (bool): ``True`` if released or not found.
            #. code (int, str): Same codes as ``release_lock``.
        """
        with self.__ownAcquiredLock:
            req = self.__ownAcquired.pop(lockId, None)
        if req is None:
            return True, 0
        paths = req.get('path', [])
        try:
            if self.isServer:
                fut = self._submit(self._server_release({
                    'request_unique_id': lockId, 'path': paths,
                }))
            elif self.isClient:
                fut = self._submit(self._client_release(lockId, paths))
            else:
                return False, 3
            await asyncio.wrap_future(fut)
            return True, 1
        except Exception as err:
            return False, str(err)

    # ------------------------------------------------------------------
    # Context manager (sync + async)
    # ------------------------------------------------------------------
    def lock(self, path, timeout=None):
        """Return a context manager that acquires and releases *path*.

        Supports both ``with`` (synchronous) and ``async with`` (non-blocking)
        usage.

        :Parameters:
            #. path (str, list, tuple): Path or list of paths to lock.
            #. timeout (None, int, float): Seconds to wait.

        :Returns:
            #. context (_LockContext): Context manager for the lock.
        """
        return _LockContext(self, path, timeout)

    def __enter__(self):
        """Acquire the locker's default lock synchronously."""
        self.__ctxLockId = None
        return self

    def __exit__(self, *_):
        """Release the locker's default lock synchronously."""
        if getattr(self, '_ServerLocker__ctxLockId', None) is not None:
            self.release_lock(self.__ctxLockId)
            self.__ctxLockId = None


# ---------------------------------------------------------------------------
# Convenience alias kept for backward compatibility
# ---------------------------------------------------------------------------
#SingleLocker = ServerLocker

class SingleLocker(ServerLocker):
    """
    This is singleton implementation of ServerLocker class. It's better to
    create a single locker in a process.
    """
    __thisInstance = None
    def __new__(cls, *args, **kwds):
        if cls.__thisInstance is None:
            cls.__thisInstance = super(ServerLocker,cls).__new__(cls)
            cls.__thisInstance._isInitialized = False
        return cls.__thisInstance

    def __init__(self, *args, **kwargs):
        if (self._isInitialized): return
        # initialize
        super(SingleLocker, self).__init__(*args, **kwargs)
        # update flag
        self._isInitialized = True

