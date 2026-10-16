# -*- coding: utf-8 -*-
"""Asyncio-powered ServerLocker implementation for pylocker.

Requires Python 3.9 or later.

This module provides a fully asyncio-based distributed locking engine exposing
the ``ServerLocker``, ``SingleLocker`` and ``FACTORY`` public API.

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
Newline-delimited JSON framing by default.  Passing
``serializer='msgpack'`` to the constructor switches to msgpack
length-prefixed binary framing (falls back to JSON with a warning if
the ``msgpack`` package is not installed).
The serializer is negotiated automatically during the TCP handshake:
the server announces its choice in the welcome message; every client
switches to match.  A client without the ``msgpack`` package cannot
join a msgpack server.  No pickle is used.

Login
-----
The password is never sent.  The server answers a hello message with a random
challenge; the client replies with a Hash-based Message Authentication Code
(HMAC) of both random values, keyed with the password, and the server answers
with its own HMAC so the client knows it reached a server that holds the
password.  ``password=None`` runs without a login.  After a number of failed
logins the server blocks the sender for a while.

Election protocol
-----------------
When multiple processes start simultaneously and share the same *serverFile*,
a three-step file-based election prevents split-brain (two processes both
believing they are the server).

Claim step
    The process takes an exclusive lock on the file, reads it and, when no
    live server or pending claim exists, writes its unique name plus
    placeholder tokens ``TIMESTAMP`` and ``PORT``.  It then releases the lock
    and re-reads after 10 ms to confirm.  Any process taking the lock
    afterwards sees the pending claim and waits.  A claim whose writer is
    gone, or that is older than ``_ELECTION_STALE_THRESHOLD`` seconds, is
    abandoned and may be overwritten.

Stamp step
    The winner replaces the ``TIMESTAMP`` placeholder with the real wall
    clock time.  This starts the staleness clock.  A stamp without a published
    port is a server that is still starting, so other processes wait for it.
    If the process crashes here, the entry ages out after
    ``_ELECTION_STALE_THRESHOLD`` seconds and the next candidate restarts the
    election.

Announce step
    The winner binds the TCP server socket.  ``_start_server`` writes the
    fully-resolved fingerprint (real timestamp + real port) to the file.
    A background ``_heartbeat`` coroutine then refreshes that file every
    ``_HEARTBEAT_INTERVAL`` seconds for as long as the server runs.

Losers poll the file every 10 ms until they see a real port number, then
call ``connect()``.
"""

import os
import re
import sys
import json
import time
import uuid
import atexit
import signal
import socket
import logging
import asyncio
import stat
import struct
import hmac
import hashlib
import secrets
import math
import collections
import threading
import concurrent.futures

# ---------------------------------------------------------------------------
# Python version guard
# ---------------------------------------------------------------------------
if sys.version_info < (3, 9):
    raise RuntimeError(
        f"pylocker requires Python 3.9 or later; found {sys.version}"
    )


# ---------------------------------------------------------------------------
# Optional msgpack support
# ---------------------------------------------------------------------------
# msgpack is an optional dependency.  Install it with:
#   pip install msgpack
# When available, callers may pass serializer='msgpack' to the ServerLocker
# constructor to get binary framing instead of newline-delimited JSON.
# The serializer is negotiated during the handshake: clients follow the
# server's choice, so a client without msgpack cannot join a msgpack server.
try:
    import msgpack as _msgpack
    MSGPACK_AVAILABLE = True
except ImportError:
    _msgpack          = None
    MSGPACK_AVAILABLE = False


try:
    import fcntl      # POSIX (Unix-like systems) file locking
except ImportError:
    fcntl = None
try:
    import msvcrt     # Windows file locking
except ImportError:
    msvcrt = None
try:
    import psutil
except ImportError:
    psutil = None


def _to_bytes(value):
    """Encodes *value* to UTF-8 bytes if it is not already bytes."""
    return value if isinstance(value, bytes) else value.encode('utf-8')


class _MessageTooLargeError(ConnectionError):
    """Signals that a peer announced a message bigger than the allowed size, so the stream cannot be followed."""


def _log_text(value):
    """Turns text that came from another program into one safe, quoted line for a log.

    Quoting and escaping keep a peer from forging log lines with line breaks.

    :Parameters:
        #. value (object): The text, or any value, to show in a log message.

    :Returns:
        #. text (str): The shortened, escaped and quoted text.
    """
    text = str(value)
    suffix = '...' if len(text) > _LOG_TEXT_LENGTH else ''
    return repr(text[:_LOG_TEXT_LENGTH]) + suffix


def _require_positive_int(name, value, minimum=1):
    """Raises ValueError unless value is a whole number of at least the minimum.

    :Parameters:
        #. name (str): Argument name used in the error message.
        #. value (int): The value to check.
        #. minimum (int): The smallest accepted value.

    :Raises:
        #. ValueError: If value is not an integer, or is below the minimum.
    """
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"{name} must be an integer of at least {minimum}")


class _PasswordNotSet:
    """Marks a locker that lost its password, as an unpickled copy does."""

    def __repr__(self):
        """Returns a readable name for debugging."""
        return '<password not set>'


_PASSWORD_NOT_SET = _PasswordNotSet()


def _check_password(name, password):
    """Raises an error unless the password is None, or a string or bytes that is not empty.

    :Parameters:
        #. name (str): What the password is for, used in the error message.
        #. password (None, str, bytes): The value to check.

    :Raises:
        #. TypeError: If the password has another type.
        #. ValueError: If the password is empty, which would give a login that anyone can pass.
    """
    if password is not None and not isinstance(password, (str, bytes)):
        raise TypeError(f"{name} must be None, a string or bytes")
    if password is not None and len(password) == 0:
        raise ValueError(f"{name} must not be empty; use None to run without a login")


def _make_proof(password, role, *parts):
    """Computes the proof that shows knowledge of the password without sending it.

    The role keeps a proof made for one purpose from being accepted for another,
    for example a client's proof as the server's answer.

    :Parameters:
        #. password (bytes): The secret that keys the Hash-based Message Authentication Code (HMAC).
        #. role (str): What the proof is for, e.g. ``client``, ``server`` or ``order``.
        #. parts (str): The values the proof covers, such as the two random challenge values.

    :Returns:
        #. proof (str): The proof as hexadecimal text.
    """
    message = '|'.join((role,) + parts).encode(_ENCODING)
    return hmac.new(password, message, hashlib.sha256).hexdigest()


def _proofs_match(received, expected):
    """Compares a received proof with the expected one in constant time.

    :Parameters:
        #. received (object): The proof as received; anything but a string never matches.
        #. expected (str): The proof that was computed locally.

    :Returns:
        #. isMatch (bool): Whether the two proofs are the same.
    """
    if not isinstance(received, str):
        return False
    return hmac.compare_digest(received.encode(_ENCODING, errors='replace'), expected.encode(_ENCODING))

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

# Placeholder tokens written to the fingerprint file during the election.
_ELECTION_PLACEHOLDER_TIMESTAMP = 'TIMESTAMP'   # Written by the claim step
_ELECTION_PLACEHOLDER_PORT      = 'PORT'        # Written by the claim and stamp steps

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

# What the fingerprint file says about the current server, as read once per
# election round.
_ElectionState = collections.namedtuple(
    '_ElectionState',
    'ownerName address port pid isEmpty isLive isStale isPending'
)

# Longest wait (seconds) for the election lock before continuing without it.
_ELECTION_LOCK_TIMEOUT = 5.0

# Pause (seconds) between attempts to take the election lock.
_ELECTION_LOCK_POLL    = 0.005

# Windows locks a byte range; this offset lies far past the fingerprint text
# so readers of the file are never blocked.
_ELECTION_LOCK_OFFSET  = 1 << 20
_SERVER_FILE_MODE      = 0o600
# Not defined on Windows, where symbolic links are not followed by os.open anyway.
_NO_FOLLOW_FLAG        = getattr(os, 'O_NOFOLLOW', 0)

# Extra headroom added to Future.result(timeout=) so the server-side expiry
# fires before the caller's thread gives up.
_RESULT_SLACK   = 1.0

# Seconds a new connection may take to send its hello message.
_HANDSHAKE_TIMEOUT = 10.0
_AUTH_NONCE_BYTES = 16
_DEFAULT_MAX_AUTH_FAILURES = 3
_DEFAULT_BLOCK_SECONDS = 300.0
_MAX_TRACKED_CLIENTS = 1024
_ORDER_MAX_AGE = 30.0
_MAX_REMEMBERED_ORDERS = 1024
_DEFAULT_MAX_MESSAGE_BYTES = 65536
_DEFAULT_MAX_CLIENTS = 1000
_DEFAULT_MAX_PENDING_PER_CLIENT = 1000
_DEFAULT_MAX_LOCKS_PER_CLIENT = 10000
_DEFAULT_MAX_PUBLICATIONS = 1000
_DEFAULT_MAX_LOGINS_IN_PROGRESS = 100
# The election lock lives in its own private file, so someone who may only read the server file cannot hold it
_LOCK_FILE_SUFFIX = '.lock'
_MIN_MAX_MESSAGE_BYTES = 1024
# Room kept for the fields the server adds to a message it passes on
_MESSAGE_OVERHEAD_BYTES = 4096
_MAX_PATHS_PER_REQUEST = 1000
_MAX_NAME_LENGTH = 200
_LOGIN_REFUSALS = ('bad password', 'blocked', 'unsupported login; update pylocker')
_LOG_TEXT_LENGTH = 100

# Seconds a stopping server waits for its client connections to close before it goes on.
_CLIENT_STOP_TIMEOUT = 2.0

# Seconds a client waits when opening a connection, unless the caller sets connectTimeout.
_DEFAULT_CONNECT_TIMEOUT = 20

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

# Address used when the server serves this machine only.
_LOOPBACK_ADDRESS = '127.0.0.1'

# Address used when the server accepts connections from other machines.
_ANY_ADDRESS      = '0.0.0.0'


class AuthenticationError(Exception):
    """Signals that the server rejected the password during the handshake."""


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
    """Starts or reuses the process-wide shared event loop and increments its reference count.

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
    """Decrements the shared loop reference count and shuts it down when it reaches zero.

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
    """Returns a normalised, slash-unified string for *path*."""
    normalised = os.path.normpath(path).replace('\\', '/')
    # POSIX normpath keeps exactly two leading slashes, but '//x' and '/x' name the same file.
    if os.sep == '/' and normalised.startswith('//'):
        normalised = normalised[1:]
    return normalised


def _to_error_code(error):
    """Converts a caught exception into the error code returned to lock callers.

    :Parameters:
        #. error (Exception): The caught exception.

    :Returns:
        #. code (int, str): The exception message as an integer when it
           holds one, otherwise the message as a string.
    """
    message = str(error)
    try:
        return int(message)
    except ValueError:
        return message


def _format_fingerprint(uniqueName, timestamp, address, port, pid):
    """Builds a fingerprint line in the layout the election and clients parse.

    :Parameters:
        #. uniqueName (str): Unique name of the locker that owns the line.
        #. timestamp (str, float): Epoch time or the election placeholder.
        #. address (str): Address the server advertises.
        #. port (str, int): Server port or the election placeholder.
        #. pid (int, str): Process ID of the owner.

    :Returns:
        #. fingerprint (str): The formatted line without a trailing newline.
    """
    return f'{uniqueName}({timestamp})@{address}:{port}[{pid}]'


def _close_writer(writer):
    """Closes a stream writer and ignores errors from an already broken connection.

    :Parameters:
        #. writer (asyncio.StreamWriter): The writer to close.
    """
    try:
        writer.close()
    except Exception:
        # A writer that is already closed or broken has nothing left to close
        pass


def _require_positive_number(name, value):
    """Raises ValueError unless value is a positive int or float.

    :Parameters:
        #. name (str): Argument name used in the error message.
        #. value (int, float): The value to check.

    :Raises:
        #. ValueError: If value is not a positive number.
    """
    if not isinstance(value, (int, float)) or not value > 0:
        raise ValueError(f"{name} must be a positive number")


def _file_problem(info):
    """Finds why an existing server file cannot be trusted.

    A file that another user owns could have been planted to send clients to a
    program that collects their password. Checks are skipped on Windows, which
    has no user ids to compare.

    :Parameters:
        #. info (os.stat_result): The status of the opened file.

    :Returns:
        #. problem (str, None): What is wrong, or None when the file can be trusted.
    """
    if os.name == 'nt':
        return None
    if not stat.S_ISREG(info.st_mode):
        return 'is not a regular file'
    if info.st_uid not in (os.geteuid(), 0):
        return 'belongs to another user'
    if info.st_mode & stat.S_IWOTH != 0:
        return 'can be written by every user'
    return None


def _folder_problem(path):
    """Finds why the folder of a server file cannot be trusted.

    Any user can delete or replace files in a folder that every user can change,
    unless the folder has the sticky bit, as ``/tmp`` has.

    :Parameters:
        #. path (str): Path of the server file.

    :Returns:
        #. problem (str, None): What is wrong, or None when the folder can be trusted.
    """
    if os.name == 'nt':
        return None
    try:
        info = os.stat(os.path.dirname(os.path.abspath(path)))
    except OSError:
        # A missing or unreadable folder fails later with its own, clearer error
        return None
    isWorldWritable = info.st_mode & stat.S_IWOTH != 0
    hasStickyBit = info.st_mode & stat.S_ISVTX != 0
    if isWorldWritable and not hasStickyBit:
        return 'is in a folder that every user can change'
    return None


def _server_file_problem(path):
    """Finds why a server file, or the folder it is in, cannot be trusted.

    :Parameters:
        #. path (str): Path of the server file.

    :Returns:
        #. problem (str, None): What is wrong, or None when there is nothing wrong or the file does not exist yet.
    """
    problem = _folder_problem(path)
    if problem is not None:
        return problem
    try:
        info = os.lstat(path)
    except OSError:
        return None
    return _file_problem(info)


def _open_server_file(path, flags):
    """Opens the server file after checking that it can be trusted.

    A file that is created here is readable and writable by its owner only. The
    check is made on the open file, so the file cannot be swapped between the
    check and its use.

    :Parameters:
        #. path (str): Path of the server file.
        #. flags (int): ``os.open`` flags. The file is created only if they include ``os.O_CREAT``.

    :Returns:
        #. descriptor (int): The open file descriptor.

    :Raises:
        #. PermissionError: If the file or its folder cannot be trusted.
    """
    problem = _folder_problem(path)
    if problem is not None:
        raise PermissionError(f"serverFile '{path}' {problem}")
    descriptor = os.open(path, flags | _NO_FOLLOW_FLAG, _SERVER_FILE_MODE)
    problem = _file_problem(os.fstat(descriptor))
    if problem is not None:
        os.close(descriptor)
        raise PermissionError(f"serverFile '{path}' {problem}")
    return descriptor


def _read_server_file_line(path):
    """Reads the first line of the server file.

    :Parameters:
        #. path (str): Path of the server file.

    :Returns:
        #. line (str): The first line, with its line break.

    :Raises:
        #. PermissionError: If the file or its folder cannot be trusted.
    """
    with os.fdopen(_open_server_file(path, os.O_RDONLY), 'r') as fileHandle:
        return fileHandle.readline()


def _write_server_file_text(path, text):
    """Replaces the content of the server file, creating the file if needed.

    :Parameters:
        #. path (str): Path of the server file.
        #. text (str): The new content. An empty text empties the file.

    :Raises:
        #. PermissionError: If the file or its folder cannot be trusted.
    """
    with os.fdopen(_open_server_file(path, os.O_WRONLY | os.O_CREAT), 'w') as fileHandle:
        # The file is emptied only now, after the check, so a file that is refused is left as it was
        fileHandle.truncate(0)
        fileHandle.write(text)


def _process_exists(pid):
    """Tells whether a process with the given ID is running on this machine.

    :Parameters:
        #. pid (int, str): Process ID to look up.

    :Returns:
        #. exists (bool): ``True`` if the process is running or cannot be
           ruled out.
    """
    if psutil is not None:
        return psutil.pid_exists(int(pid))
    # os.kill terminates the target on Windows, so it cannot be used as a probe there.
    if os.name == 'nt':
        return True
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return False
    except (PermissionError, ValueError, OverflowError):
        return True
    return True


def _get_ip():
    """Returns the best local IP address for this machine."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.connect(('8.8.8.8', 80))
        return sock.getsockname()[0]
    except Exception:
        return _LOOPBACK_ADDRESS
    finally:
        try:
            sock.close()
        except Exception:
            # The socket may already be closed
            pass


async def _write_json(writer, content):
    """Serialises *content* to JSON and sends it as a newline-terminated line."""
    try:
        writer.write(json.dumps(content).encode(_ENCODING) + b'\n')
        await writer.drain()
    except Exception:
        # The peer may have disconnected; the reader on that connection notices a closed connection
        pass


async def _read_json(reader):
    """Reads one newline-terminated JSON line and deserialises it."""
    raw = await reader.readline()
    if not raw:
        return None
    return json.loads(raw.decode(_ENCODING))



# ── msgpack framing (optional) ───────────────────────────────────────────
# Messages are 4-byte big-endian length-prefixed msgpack frames.
# The length covers only the payload, not the 4-byte header itself.

async def _write_msgpack(writer, content):
    """Serialises *content* with msgpack and sends it with a 4-byte length prefix."""
    try:
        payload = _msgpack.packb(content, use_bin_type=True)
        writer.write(struct.pack('>I', len(payload)) + payload)
        await writer.drain()
    except Exception:
        # The peer may have disconnected; the reader on that connection notices a closed connection
        pass


async def _read_msgpack(reader, maxBytes):
    """Reads one length-prefixed msgpack frame and unpacks it.

    Raises ``asyncio.IncompleteReadError`` for unexpected disconnections
    so the caller's outer handler can distinguish a clean shutdown from
    a network error.

    :Parameters:
        #. reader (asyncio.StreamReader): The stream to read from.
        #. maxBytes (int): The largest frame accepted, checked before anything of it is read.

    :Returns:
        #. message (object): The unpacked message.

    :Raises:
        #. _MessageTooLargeError: If the frame announces more than *maxBytes* bytes.
    """
    header = await reader.readexactly(4)
    length  = struct.unpack('>I', header)[0]
    if length > maxBytes:
        raise _MessageTooLargeError(f"message of {length} bytes is above the limit of {maxBytes} bytes")
    payload = await reader.readexactly(length)
    return _msgpack.unpackb(payload, raw=False)


# ---------------------------------------------------------------------------
# Cross-process election lock
# ---------------------------------------------------------------------------
class _ElectionLock:
    """Holds an exclusive cross-process lock on a file while a block runs.

    The election reads the fingerprint file, decides and writes its claim
    under this lock, so two processes can never both see a vacant file and
    both claim it. The operating system frees the lock if its holder dies.
    Without a locking module, or if the lock stays busy past
    ``_ELECTION_LOCK_TIMEOUT``, the block runs unlocked and a warning is
    emitted.

    :Parameters:
        #. path (str): File to lock. It is opened in append mode.
        #. warn (callable): Function that receives warning messages.
    """

    def __init__(self, path, warn):
        """Stores the file path and the warning callback."""
        self._path     = path
        self._warn     = warn
        self._file     = None
        self._isLocked = False

    def _try_lock(self):
        """Attempts to take the lock once without blocking; raises OSError if busy."""
        if fcntl is not None:
            fcntl.flock(self._file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        else:
            self._file.seek(_ELECTION_LOCK_OFFSET)
            msvcrt.locking(self._file.fileno(), msvcrt.LK_NBLCK, 1)

    def __enter__(self):
        """Takes the lock, or continues unlocked after a warning."""
        if fcntl is None and msvcrt is None:
            return self
        try:
            self._file = os.fdopen(_open_server_file(self._path, os.O_WRONLY | os.O_CREAT | os.O_APPEND), 'a')
        except PermissionError:
            # A lock file that someone else owns or can write must not be used, and must not be skipped quietly
            raise
        except OSError as error:
            self._warn(f"Election lock unavailable ({error}); continuing without it")
            return self
        deadline = time.time() + _ELECTION_LOCK_TIMEOUT
        while True:
            try:
                self._try_lock()
                self._isLocked = True
                return self
            except OSError:
                if time.time() > deadline:
                    self._warn("Election lock busy for too long; continuing without it")
                    return self
                time.sleep(_ELECTION_LOCK_POLL)

    def __exit__(self, *_):
        """Releases the lock and closes the file."""
        if self._file is None:
            return
        try:
            if self._isLocked and fcntl is not None:
                fcntl.flock(self._file, fcntl.LOCK_UN)
            elif self._isLocked:
                self._file.seek(_ELECTION_LOCK_OFFSET)
                msvcrt.locking(self._file.fileno(), msvcrt.LK_UNLCK, 1)
        finally:
            self._file.close()


# ---------------------------------------------------------------------------
# Lock context manager (sync + async)
# ---------------------------------------------------------------------------
class _LockContext:
    """Context manager returned by ``ServerLocker.lock()``.

    Supports both ``with`` and ``async with`` usage transparently.
    """

    def __init__(self, locker, path, timeout):
        """Initialises a lock context for the given locker and path."""
        self._locker  = locker
        self._path    = path
        self._timeout = timeout
        self._lockId  = None

    # -- sync --
    def __enter__(self):
        """Acquires the lock synchronously and returns ``(acquired, lockId)``."""
        acquired, lockId = self._locker.acquire_lock(self._path, timeout=self._timeout)
        if acquired:
            self._lockId = lockId
        return acquired, lockId

    def __exit__(self, *_):
        """Releases the lock synchronously."""
        if self._lockId is not None:
            self._locker.release_lock(self._lockId)
            self._lockId = None

    # -- async --
    async def __aenter__(self):
        """Acquires the lock asynchronously and returns ``(acquired, lockId)``."""
        acquired, lockId = await self._locker.acquire_async(self._path, timeout=self._timeout)
        if acquired:
            self._lockId = lockId
        return acquired, lockId

    async def __aexit__(self, *_):
        """Releases the lock asynchronously."""
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
        """Returns the single shared instance, creating it on the first call.

        Double-checked locking ensures thread safety without paying the
        lock-acquisition cost on every call after the instance exists.
        """
        if cls._instance is None:
            with cls._instanceLock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        """Initialises the cache on the very first instantiation only.

        ``__init__`` is called by Python every time someone writes
        ``_LockerFactory()``, even when ``__new__`` returns the existing
        instance.  The ``_initialised`` guard ensures the cache and lock
        are created exactly once.
        """
        if not hasattr(self, '_initialised'):
            self._lock        = threading.Lock()
            self._cache       = {}
            self._passwords   = {}
            self._initialised = True

    def __call__(self, key, password, regenerate=False, **kwargs):
        """Returns a cached or freshly created ``ServerLocker`` for *key*.

        :Parameters:
            #. key (str): Cache key, typically the server-file path or any
               application-defined string that uniquely identifies a locker.
            #. password (str, bytes): Locker password passed to the
               ``ServerLocker`` constructor on first creation.
            #. regenerate (bool): If ``True``, stop and discard the cached
               instance for *key* and build a fresh one.
            #. kwargs (dict): Additional keyword arguments forwarded verbatim to the
               ``ServerLocker`` constructor when a new instance is created.

        :Returns:
            #. locker (ServerLocker): The cached or newly created instance.

        :Raises:
            #. ValueError: If *key* is already cached with a different password.
        """
        with self._lock:
            if regenerate and key in self._cache:
                try:
                    self._cache[key].stop()
                except Exception:
                    # A locker that cannot be stopped is dropped from the cache anyway
                    pass
                del self._cache[key]
                self._passwords.pop(key, None)
            if key not in self._cache:
                self._cache[key]     = ServerLocker(password=password, **kwargs)
                self._passwords[key] = password
            elif self._passwords[key] != password:
                raise ValueError(
                    f"FACTORY key '{key}' already exists with a different "
                    f"password; pass regenerate=True to replace it"
                )
            return self._cache[key]


FACTORY = _LockerFactory()


# ---------------------------------------------------------------------------
# Main class
# ---------------------------------------------------------------------------
class ServerLocker:
    """Asyncio-powered distributed locker.

    Orchestrates locking and releasing string entities between threads and
    processes on the same machine or across a network.  A private daemon
    thread owns a dedicated ``asyncio`` event loop; all TCP communication runs
    on that loop.  Synchronous callers use the standard blocking API
    unchanged.  Async callers may opt into ``acquire_async`` / ``release_async``
    to avoid blocking their own event loop.

    :Parameters:
        #. password (None, str, bytes): Shared secret used to authenticate
           connections.  It must not be empty.  ``None`` runs without a login: every client that
           reaches the server is accepted, so it cannot be combined with
           *allowRemoteClients* and remote stop orders need their own password.
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
        #. reconnect (bool): Accepted and ignored. Existing callers pass it,
           so removing it would break them.
        #. connectTimeout (int, float): Seconds allowed for the TCP handshake
           to complete.
        #. logger (bool, logging.Logger): ``False`` silences all output.
           ``True`` creates a default stderr logger.  Pass a
           ``logging.Logger`` instance to use your own.
        #. blocking (bool): If ``True`` and this instance becomes the server,
           ``start()`` blocks until ``stop()`` is called.
        #. debugMode (bool): Emit informational log messages even at INFO
           level.
        #. serializer (str): Wire-framing format.  ``'json'`` (default) uses
           newline-delimited UTF-8 JSON and needs no extra package.
           ``'msgpack'`` uses 4-byte length-prefixed binary msgpack frames
           and falls back to JSON with a warning if the ``msgpack`` package
           is not installed.  The serializer is negotiated during the TCP
           handshake: the server announces its choice in the welcome
           message and every client switches to match, so a client without
           msgpack cannot join a msgpack server.  Install msgpack with:
           ``pip install msgpack``.
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
        #. allowRemoteClients (bool): If ``False`` (default) the server only
           accepts connections from this machine and advertises
           ``127.0.0.1``. If ``True`` it accepts connections from other
           machines and advertises this machine's network IP address. Set
           ``True`` only when several computers share the same *serverFile*,
           for example on a network drive.
        #. maxAuthFailures (None, int): Failed logins after which a sender is
           blocked.  ``None`` never blocks.
        #. blockSeconds (int, float): Seconds a blocked sender stays blocked.
        #. maxMessageBytes (int): Largest message accepted from a peer.  A
           bigger JSON line or msgpack frame ends the connection.  At least 1024.
        #. maxClients (int): Most clients the server holds at once, counting
           only those that have logged in.  More are refused.
        #. maxPendingPerClient (int): Most lock requests one client may have
           waiting.  More are dropped with a warning.
        #. maxLocksPerClient (int): Most paths one client may hold locked at
           once.  A request that would go over it waits until locks are released.
        #. maxPublications (int): Most publications a locker keeps stored.  A
           publication that would go over it is dropped with a warning.
        #. maxLoginsInProgress (int): Most connections that may be logging in
           at once.  When it is reached, the one that has waited longest is closed.

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
    # names stored in a pickled locker.
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
        '_ServerLocker__debugMode',
        '_ServerLocker__sharedLoop',     # process-wide shared event loop preference
        '_ServerLocker__allowRemoteClients',
    ]

    def __init__(self, password, name=None, serverFile=True,
                 defaultTimeout=20, maxLockTime=120, port=3000,
                 allowServing=True, autoconnect=True, reconnect=False,
                 connectTimeout=_DEFAULT_CONNECT_TIMEOUT, electionTimeout=None, logger=False,
                 blocking=False, debugMode=False,
                 serializer='json', sharedLoop=True, allowRemoteClients=False,
                 maxAuthFailures=_DEFAULT_MAX_AUTH_FAILURES, blockSeconds=_DEFAULT_BLOCK_SECONDS,
                 maxMessageBytes=_DEFAULT_MAX_MESSAGE_BYTES, maxClients=_DEFAULT_MAX_CLIENTS,
                 maxPendingPerClient=_DEFAULT_MAX_PENDING_PER_CLIENT,
                 maxLocksPerClient=_DEFAULT_MAX_LOCKS_PER_CLIENT, maxPublications=_DEFAULT_MAX_PUBLICATIONS,
                 maxLoginsInProgress=_DEFAULT_MAX_LOGINS_IN_PROGRESS):
        # Set the private backing store directly to avoid the setter running
        # before the logger is initialised (the setter would try to update the
        # logger level on an object that does not yet have _logger).
        self.__debugMode = bool(debugMode)
        # unique identity
        self.__uniqueName = str(uuid.uuid4())
        if name is None:
            name = self.__uniqueName
        if not isinstance(name, str):
            raise TypeError("locker server name must be None or a string")
        if ':' in name:
            raise ValueError("':' not allowed in ServerLocker name")
        self.__name = name
        # password
        _check_password('locker password', password)
        self.__password = None if password is None else _to_bytes(password)
        # flags
        flags = (('blocking', blocking), ('allowServing', allowServing),
                 ('autoconnect', autoconnect),
                 ('allowRemoteClients', allowRemoteClients))
        for flagName, flagValue in flags:
            if not isinstance(flagValue, bool):
                raise TypeError(f"{flagName} must be boolean")
        if allowRemoteClients and password is None:
            raise ValueError("allowRemoteClients requires a password; password=None is for the local computer only")
        self.__blocking     = blocking
        self.__allowServing = allowServing
        # login blocking
        if maxAuthFailures is not None:
            if isinstance(maxAuthFailures, bool) or not isinstance(maxAuthFailures, int) or maxAuthFailures < 1:
                raise ValueError("maxAuthFailures must be None or an integer of at least 1")
        _require_positive_number('blockSeconds', blockSeconds)
        self.__maxAuthFailures = maxAuthFailures
        self.__blockSeconds    = float(blockSeconds)
        # size limits
        _require_positive_int('maxMessageBytes', maxMessageBytes, _MIN_MAX_MESSAGE_BYTES)
        _require_positive_int('maxClients', maxClients)
        _require_positive_int('maxPendingPerClient', maxPendingPerClient)
        _require_positive_int('maxLocksPerClient', maxLocksPerClient)
        _require_positive_int('maxPublications', maxPublications)
        _require_positive_int('maxLoginsInProgress', maxLoginsInProgress)
        self.__maxMessageBytes     = maxMessageBytes
        self.__maxClients          = maxClients
        self.__maxPendingPerClient = maxPendingPerClient
        self.__maxLocksPerClient   = maxLocksPerClient
        self.__maxPublications     = maxPublications
        self.__maxLoginsInProgress = maxLoginsInProgress
        # serializer — controls wire framing for all TCP messages
        if serializer not in ('json', 'msgpack'):
            raise ValueError(f"serializer must be 'json' or 'msgpack'; got '{serializer}'")
        # Availability is checked after set_logger so we can emit a proper
        # warning via self._warn instead of a bare RuntimeError.
        self._useMsgpack = (serializer == 'msgpack')
        # shared event loop preference
        if not isinstance(sharedLoop, bool):
            raise TypeError(
                f"sharedLoop must be a boolean; got '{type(sharedLoop).__name__}'"
            )
        self.__sharedLoop = sharedLoop
        # timing
        _require_positive_number('defaultTimeout', defaultTimeout)
        _require_positive_number('maxLockTime', maxLockTime)
        self.__defaultTimeout = float(defaultTimeout)
        self.__maxLockTime    = float(maxLockTime)
        if not isinstance(port, int) or not port > 0:
            raise ValueError("port must be a positive integer")
        self.__port           = port
        self.__connectTimeout = float(connectTimeout)
        # electionTimeout — None falls back to the module-level _ELECTION_TIMEOUT
        # constant (30 s).  Set a higher value for slow NFS mounts or processes
        # that race at startup under heavy load.  Not persisted through pickle
        # (same pattern as connectTimeout — it's a startup-only parameter).
        if electionTimeout is not None:
            if not isinstance(electionTimeout, (int, float)) or not electionTimeout > 0:
                raise ValueError("electionTimeout must be a positive number or None")
        self.__electionTimeout = (
            float(electionTimeout) if electionTimeout is not None else None
        )
        # server file
        if serverFile is True:
            serverFile = os.path.join(os.path.expanduser('~'), '.pylocker.serverlocker')
        elif serverFile is False:
            serverFile = None
        self.__serverFile = serverFile
        # identity / network
        self.__allowRemoteClients = allowRemoteClients
        self.__pid     = os.getpid()
        # Clients connect to the advertised address, so it must match the
        # address the server listens on (see _start_server).
        self.__address = _get_ip() if allowRemoteClients else _LOOPBACK_ADDRESS
        # remote orders
        self.__allowRemoteOrders = {'allow': True, 'password': None}
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
        self._reset_connection_state()
        # owned locks (acquired on behalf of this process)
        self.__ownAcquiredLock = threading.Lock()
        self.__ownAcquired     = {}         # requestId -> request dict
        # publications (accessed from calling threads — use threading.Lock)
        self.__publicationsLock = threading.Lock()
        self.__publications     = {}
        self.__publicationCount = 0
        # Election-related: serialises fingerprint file reads/writes across the
        # calling thread (election) and the asyncio loop thread (heartbeat).
        # RLock (reentrant) is required because _clear_fingerprint calls
        # get_running_server_fingerprint while already holding the lock.
        self._serverFileLock = threading.RLock()
        self._authFailures   = {}      # client key -> failed login record (server mode only)
        self._authLock       = threading.Lock()
        # Tracks whether this instance has ever acted as server or client.
        # Once _wasClient is True, canServe returns False until reset() clears it.
        self._wasServer  = False
        self._wasClient  = False
        # atexit cleanup — register at-most-once per instance.  The flag
        # is intentionally NOT in _PICKLE_KEYS so that __setstate__ can
        # safely re-register on unpickled instances without double-firing.
        self._atexitRegistered = True
        atexit.register(self._on_atexit)
        # auto-connect
        if autoconnect:
            self.start()

    def _reset_connection_state(self):
        """Resets the event-loop, server, client and server-metadata fields to idle."""
        self._loop           = None   # asyncio event loop (background thread)
        self._loopThread     = None   # daemon thread owning the loop
        self._usesSharedLoop = False  # True when using the process-wide shared loop
        self._stopEvent      = None   # signals shutdown
        self._server         = None   # asyncio.Server (server mode only)
        self._serverPort     = None   # actual bound port
        self._pathsLUT       = None   # path -> lock record
        self._clientsLUT     = None   # client unique name -> connection info
        self._lockQueue      = None   # pending acquire requests
        self._queueEvent     = None   # signals a new queued request
        self._reader         = None
        self._writer         = None
        self._pending        = None   # requestId -> in-flight future
        self._bgTasks        = []
        self._seenOrders     = collections.OrderedDict()  # order ids already obeyed
        self._pendingCounts  = {}     # connection writer -> acquire requests waiting (server mode only)
        self._heldCounts     = {}     # connection writer -> paths it holds locked (server mode only)
        self._loginWriters   = collections.OrderedDict()  # login task -> writer, oldest first (server mode only)
        self._clientTasks    = set()  # one task for each connected client (server mode only)
        self.__serverName        = None
        self.__serverUniqueName  = None
        self.__serverMaxLockTime = None
        self.__serverAddress     = None   # client mode only
        self.__serverPort        = None   # client mode only

    # ------------------------------------------------------------------
    # Pickle support
    # ------------------------------------------------------------------
    def __getstate__(self):
        """Returns pickle-safe state (no runtime objects, no password)."""
        state = {k: self.__dict__.get(k) for k in self._PICKLE_KEYS}
        # strip password from state
        state['_ServerLocker__password'] = None
        # strip orders password
        ordersSettings = dict(state.get('_ServerLocker__allowRemoteOrders', {'allow': False}))
        ordersSettings.pop('password', None)
        state['_ServerLocker__allowRemoteOrders'] = ordersSettings
        return state

    def __setstate__(self, state):
        """Restores from pickle state (runtime objects rebuilt by reset)."""
        self.__dict__.update(state)
        # A copy has no password until the caller sets one, and it refuses to start before that.
        self.__dict__['_ServerLocker__password'] = _PASSWORD_NOT_SET
        ordersSettings = self.__dict__.get('_ServerLocker__allowRemoteOrders', {'allow': False})
        ordersSettings['password'] = None
        self.__dict__['_ServerLocker__allowRemoteOrders'] = ordersSettings
        self.__dict__.setdefault('_ServerLocker__allowServing', True)
        self.__dict__.setdefault('_ServerLocker__debugMode', False)
        self.__dict__.setdefault('_ServerLocker__sharedLoop', True)
        self.__dict__.setdefault('_ServerLocker__allowRemoteClients', False)
        # Startup-only settings are not pickled, so restore the constructor defaults.
        self.__dict__.setdefault('_ServerLocker__connectTimeout', float(_DEFAULT_CONNECT_TIMEOUT))
        self.__dict__.setdefault('_ServerLocker__electionTimeout', None)
        self.__dict__.setdefault('_ServerLocker__maxAuthFailures', _DEFAULT_MAX_AUTH_FAILURES)
        self.__dict__.setdefault('_ServerLocker__blockSeconds', _DEFAULT_BLOCK_SECONDS)
        self.__dict__.setdefault('_ServerLocker__maxMessageBytes', _DEFAULT_MAX_MESSAGE_BYTES)
        self.__dict__.setdefault('_ServerLocker__maxClients', _DEFAULT_MAX_CLIENTS)
        self.__dict__.setdefault('_ServerLocker__maxPendingPerClient', _DEFAULT_MAX_PENDING_PER_CLIENT)
        self.__dict__.setdefault('_ServerLocker__maxLocksPerClient', _DEFAULT_MAX_LOCKS_PER_CLIENT)
        self.__dict__.setdefault('_ServerLocker__maxPublications', _DEFAULT_MAX_PUBLICATIONS)
        self.__dict__.setdefault('_ServerLocker__maxLoginsInProgress', _DEFAULT_MAX_LOGINS_IN_PROGRESS)
        # A copy is another locker, and a server refuses two clients with the same unique name.
        self.__dict__['_ServerLocker__uniqueName'] = str(uuid.uuid4())
        # The pickled process id belongs to the pickling process, not this one.
        self.__dict__['_ServerLocker__pid'] = os.getpid()
        self.set_logger(None)
        # re-initialise all runtime fields
        self._reset_connection_state()
        self.__ownAcquiredLock  = threading.Lock()
        self.__ownAcquired      = {}
        self.__publicationsLock = threading.Lock()
        self.__publications     = {}
        self.__publicationCount = 0
        self._serverFileLock    = threading.RLock()
        self._authFailures      = {}
        self._authLock          = threading.Lock()
        self._wasServer         = False
        self._wasClient         = False
        self._useMsgpack        = False   # deserialized instances default to JSON
        # Guard against double-registration when the same instance is
        # restored via __setstate__ (e.g. fullrmc pickle round-trips).
        if not getattr(self, '_atexitRegistered', False):
            self._atexitRegistered = True
            atexit.register(self._on_atexit)

    # ------------------------------------------------------------------
    # Logging
    # ------------------------------------------------------------------
    def set_logger(self, logger):
        """Configures the logger for this instance.

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

    def _critical(self, message):
        """Emits a critical-level log message."""
        if self._logger:
            self._logger.critical(message)

    def _error(self, message):
        """Emits an error-level log message."""
        if self._logger:
            self._logger.error(message)

    def _warn(self, message):
        """Emits a warning-level log message."""
        if self._logger:
            self._logger.warning(message)

    def _info(self, message):
        """Emits an info-level log message (only when debugMode is True)."""
        if self._logger and self.__debugMode:
            self._logger.info(message)

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
        """The locker password as bytes, or None when there is none."""
        return None if self.__password is _PASSWORD_NOT_SET else self.__password

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
    def allowRemoteClients(self):
        """``True`` if the server accepts connections from other machines."""
        return self.__allowRemoteClients

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
        """Enables or disables informational log messages.

        :Parameters:
            #. value (bool): ``True`` to enable, ``False`` to disable.

        :Raises:
            #. TypeError: If value is not a boolean.
        """
        if not isinstance(value, bool):
            raise TypeError("debugMode must be a boolean")
        self.__debugMode = value
        # Update the logger level if one is already configured.
        if getattr(self, '_logger', None) is not None:
            self._logger.setLevel(logging.DEBUG if value else logging.WARNING)

    @property
    def allowRemoteOrders(self):
        """A copy of the remote-orders settings dict (password excluded)."""
        ordersSettings = dict(self.__allowRemoteOrders)
        ordersSettings.pop('password', None)
        return ordersSettings

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
            return {requestId: list(record.get('path', []))
                    for requestId, record in self.__ownAcquired.items()}

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
        return {path: dict(record) for path, record in list(self._pathsLUT.items())}

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
        for path, record in list(self._pathsLUT.items()):
            requestId = record.get('request_unique_id', path)
            if requestId not in grouped:
                grouped[requestId] = dict(record)
                grouped[requestId]['paths'] = []
            grouped[requestId]['paths'].append(path)
        return grouped

    # ── Publication state ─────────────────────────────────────────────

    @property
    def messages(self):
        """A list of all publication message keys currently stored."""
        with self.__publicationsLock:
            self._purge_expired_publications(time.time())
            return list(self.__publications.keys())

    # ── Protected introspection (for tests and diagnostics) ───────────

    @property
    def _publications(self):
        """A snapshot of the stored publications dict (thread-safe copy)."""
        with self.__publicationsLock:
            self._purge_expired_publications(time.time())
            return dict(self.__publications)

    # ------------------------------------------------------------------
    # Configuration setters
    # ------------------------------------------------------------------
    def set_maximum_lock_time(self, maxLockTime):
        """Sets the maximum seconds any lock may be held.

        :Parameters:
            #. maxLockTime (int, float): Positive number of seconds.

        :Raises:
            #. ValueError: If maxLockTime is not a positive number.
        """
        try:
            maxLockTime = float(maxLockTime)
        except (TypeError, ValueError):
            raise ValueError('maxLockTime must be a positive number')
        if not maxLockTime > 0:
            raise ValueError('maxLockTime must be a positive number')
        self.__maxLockTime = maxLockTime

    def set_default_timeout(self, defaultTimeout):
        """Sets the default acquire timeout in seconds.

        :Parameters:
            #. defaultTimeout (int, float): Positive number of seconds.

        :Raises:
            #. ValueError: If defaultTimeout is not a positive number.
        """
        try:
            defaultTimeout = float(defaultTimeout)
        except (TypeError, ValueError):
            raise ValueError('defaultTimeout must be a positive number')
        if not defaultTimeout > 0:
            raise ValueError('defaultTimeout must be a positive number')
        self.__defaultTimeout = defaultTimeout

    def set_server_file(self, serverFile):
        """Sets the fingerprint file path.

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
        """Sets or updates the locker password.

        :Parameters:
            #. password (None, str, bytes): New shared secret, or None to run without a login.

        :Raises:
            #. TypeError: If password is not None, a string or bytes.
            #. ValueError: If password is empty, or None while remote clients are allowed.
        """
        _check_password('locker password', password)
        if password is None and self.__allowRemoteClients:
            raise ValueError("allowRemoteClients requires a password; password=None is for the local computer only")
        self.__password = None if password is None else _to_bytes(password)

    def allow_receiving_remote_orders(self, allow, password=None):
        """Enables or disables execution of remote stop orders.

        :Parameters:
            #. allow (bool): Whether to allow remote orders.
            #. password (None, str, bytes): Password required with each remote
               order.  If ``None`` the locker password is used.

        :Raises:
            #. TypeError: If allow is not a boolean, or password is not None, a string or bytes.
            #. ValueError: If password is empty, or orders are allowed but neither an orders password nor a
               locker password exists.
        """
        if not isinstance(allow, bool):
            raise TypeError("allow must be boolean")
        _check_password('orders password', password)
        if allow and password is None and self.__password in (None, _PASSWORD_NOT_SET):
            raise ValueError("remote orders need a password: pass one, or give the locker a password")
        self.__allowRemoteOrders = {'allow': allow, 'password': None if password is None else _to_bytes(password)}

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    def save(self, path):
        """Saves configuration to a JSON file (password excluded).

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
            'allowRemoteClients': self.__allowRemoteClients,
        }
        with open(path, 'w') as fileHandle:
            json.dump(data, fileHandle, indent=2)

    @classmethod
    def load(cls, path, password, autoconnect=True):
        """Loads a previously saved configuration and returns a new instance.

        :Parameters:
            #. path (str): Path to the JSON file written by ``save()``.
            #. password (str, bytes): The locker password (not stored in file).
            #. autoconnect (bool): Whether to auto-connect on creation.

        :Returns:
            #. locker (ServerLocker): The newly constructed instance.
        """
        with open(path, 'r') as fileHandle:
            data = json.load(fileHandle)
        data['password']    = password
        data['autoconnect'] = autoconnect
        return cls(**data)

    # ------------------------------------------------------------------
    # Fingerprint helpers
    # ------------------------------------------------------------------
    def _fingerprint_template(self):
        """Returns the fingerprint template string for this instance."""
        return _format_fingerprint(self.__uniqueName, '{now}', self.__address,
                                   '{port}', self.__pid)

    @staticmethod
    def _parse_fingerprint(fingerprint):
        """Parses a fingerprint string and returns its five fields.

        :Parameters:
            #. fingerprint (str): Fingerprint string in the form
               ``uniqueName(ts)@addr:port[pid]``.  The ``ts`` and ``port``
               fields may be placeholder tokens (``TIMESTAMP`` / ``PORT``)
               during the election.

        :Returns:
            #. uniqueName (str): Server unique name.
            #. timestamp (str): Timestamp string or the placeholder ``TIMESTAMP``.
            #. address (str): Server IP address.
            #. port (str): Port string or the placeholder ``PORT``.
            #. pid (str): Server process ID as a string.
        """
        match = _FINGERPRINT_RE.match(fingerprint.strip())
        if not match:
            return None, None, None, None, None
        fields = match.groupdict()
        return fields['uname'], fields['ts'], fields['addr'], fields['port'], fields['pid']

    def get_running_server_fingerprint(self, serverFile=None,
                                       raiseNotFound=False, raiseError=True):
        """Reads and parses the server fingerprint file.

        :Parameters:
            #. serverFile (None, str): Path to the fingerprint file.  If
               ``None`` the instance's configured path is used.
            #. raiseNotFound (bool): Raise an exception if the file does not
               exist.
            #. raiseError (bool): Raise an exception if the file cannot be
               read or cannot be trusted (see ``PermissionError``).  A file
               whose first line is not a fingerprint is not an error: it gives
               ``None`` for every field, like an empty file.

        :Returns:
            #. uniqueName (None, str): Parsed server unique name.
            #. timestamp (None, str): Parsed timestamp (may be the placeholder
               ``TIMESTAMP`` during the claim step).
            #. address (None, str): Parsed server IP address.
            #. port (None, str): Parsed port string (may be the placeholder
               ``PORT`` during the claim and stamp steps).
            #. pid (None, str): Parsed server process ID string.
        """
        if serverFile is None:
            serverFile = self.__serverFile
        uniqueName = timestamp = address = port = pid = None
        if serverFile and os.path.isfile(serverFile):
            try:
                with self._serverFileLock:
                    line = _read_server_file_line(serverFile).strip()
                if line:
                    uniqueName, timestamp, address, port, pid = \
                        self._parse_fingerprint(line)
            except Exception as error:
                if raiseError:
                    raise
        elif raiseNotFound:
            raise FileNotFoundError(f"serverFile '{serverFile}' not found")
        return uniqueName, timestamp, address, port, pid

    def _write_fingerprint(self, port):
        """Writes the fully-resolved server fingerprint to the configured file.

        This method is called by ``_start_server`` (announce step of the election)
        and by ``_heartbeat`` every ``_HEARTBEAT_INTERVAL`` seconds.  The
        ``_serverFileLock`` prevents the heartbeat and the election from
        writing concurrently.

        :Parameters:
            #. port (int): The port the server is bound to.
        """
        if not self.__serverFile:
            return
        with self._election_lock():
            with self._serverFileLock:
                try:
                    template = self._fingerprint_template()
                    line = template.format(now=str(time.time()), port=str(port))
                    _write_server_file_text(self.__serverFile, line + '\n')
                except Exception as error:
                    self._warn(f"Unable to write server fingerprint: {error}")

    def _clear_fingerprint(self):
        """Clears the fingerprint file if it belongs to this instance."""
        if not self.__serverFile:
            return
        with self._serverFileLock:
            try:
                uniqueName, _, _, _, _ = self.get_running_server_fingerprint(
                    raiseNotFound=False, raiseError=False)
                if uniqueName == self.__uniqueName:
                    _write_server_file_text(self.__serverFile, '')
            except Exception as error:
                self._warn(f"Unable to clear the server file: {error}")

    def _write_server_file(self, line):
        """Writes a line to the server file without taking the election lock.

        The caller must already hold the election lock. The
        ``_serverFileLock`` keeps this write atomic with respect to the
        heartbeat and other file operations in this process.

        :Parameters:
            #. line (str): The full fingerprint line to write.  A trailing
               newline is appended automatically.
        """
        if not self.__serverFile:
            return
        with self._serverFileLock:
            try:
                _write_server_file_text(self.__serverFile, line + '\n')
            except Exception as error:
                self._warn(f"Unable to write election line to server file: {error}")

    def _write_election_raw(self, line):
        """Writes an election-step line to the server file under the election lock.

        Used by ``_serve_or_connect`` to write the stamp entry that contains
        placeholder tokens instead of real timestamp or port values.

        :Parameters:
            #. line (str): The full fingerprint line to write.  A trailing
               newline is appended automatically.
        """
        with self._election_lock():
            self._write_server_file(line)

    # ------------------------------------------------------------------
    # Background event loop management
    # ------------------------------------------------------------------
    def _start_loop(self):
        """Starts the event loop for this instance.

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
        """Schedules *coro* on the background loop and returns a thread-safe Future.

        :Parameters:
            #. coro (coroutine): An awaitable to submit.

        :Returns:
            #. future (concurrent.futures.Future): Thread-safe future resolved
               when the coroutine completes.
        """
        return asyncio.run_coroutine_threadsafe(coro, self._loop)

    def _run_sync(self, coro, timeout=None):
        """Submits *coro* and blocks until it completes or *timeout* elapses.

        :Parameters:
            #. coro (coroutine): An awaitable to run.
            #. timeout (None, float): Seconds to wait.  ``None`` means wait
               forever.

        :Returns:
            #. result (object): Whatever the coroutine returned.
        """
        return self._submit(coro).result(timeout=timeout)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def _on_atexit(self):
        """Cleans up on interpreter shutdown."""
        try:
            self._clear_fingerprint()
        except Exception:
            # The interpreter is exiting, so there is nobody left to report an error to
            pass
        try:
            self.stop()
        except Exception:
            # The interpreter is exiting, so there is nobody left to report an error to
            pass

    def reset(self, raiseError=False):
        """Recycles a stopped locker instance by clearing its runtime state.

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
                f"Cannot reset '{self.__name}': locker is currently running. "
                "Call stop() first."
            )
            if raiseError:
                raise RuntimeError(errorMessage)
            return False, errorMessage
        self.__ownAcquired  = {}
        self.__publications = {}
        self.__publicationCount = 0
        self._wasServer     = False
        self._wasClient     = False
        return True, None

    def stop(self):
        """Stops the server or disconnects as a client.

        Before closing the connection this method explicitly releases every
        lock currently held by this instance, sending individual ``release``
        messages to the server.  This is a belt-and-suspenders measure on top
        of the server-side disconnect cleanup: if the TCP close is delayed or
        the server's ``finally:`` block runs after the lookup tables are
        nulled, the explicit release messages arrive first and prevent lock
        leaks.  On SIGKILL (a signal that ends a process at once) neither the atexit handler nor this method runs,
        but the dead-PID monitor on the server side will clean up within
        ``_DEAD_PID_POLL`` seconds.
        """
        self._clear_fingerprint()
        self._release_owned_locks()
        self._close_event_loop()
        self._reset_connection_state()

    def _release_owned_locks(self):
        """Sends an explicit release message for every lock this client holds.

        Belt-and-suspenders on top of the server-side disconnect cleanup: the
        release messages are flushed before the TCP connection closes, so no
        lock can leak if the server's cleanup runs after its tables are gone.
        Servers hold their locks in memory and need no messages.
        """
        # Grab a snapshot of owned locks while we still have the table, then
        # submit individual release messages so the server processes them
        # before the TCP connection closes.
        if self.isClient and self._writer is not None:
            with self.__ownAcquiredLock:
                ownedSnapshot = dict(self.__ownAcquired)
            for requestId, record in ownedSnapshot.items():
                try:
                    future = self._submit(self._write_message(self._writer, {
                        'request_unique_id':  requestId,
                        'action':             'release',
                        'path':               record.get('path', []),
                        'client_unique_name': self.__uniqueName,
                        'client_name':        self.__name,
                    }))
                    future.result(timeout=0.5)   # block until the write is flushed
                except Exception as error:
                    self._warn(f"Unable to release a lock while stopping: {error}")
            # Brief pause to let the event loop flush the release messages.
            time.sleep(0.05)

    def _close_event_loop(self):
        """Disconnects from the shared loop, or shuts down and closes the private loop."""
        if self._usesSharedLoop:
            # Shared loop: only tear down this client's connection.
            # The shared event loop continues running for other clients.
            loop = self._loop
            if loop is not None and loop.is_running():
                future = asyncio.run_coroutine_threadsafe(
                    self._disconnect_client(), loop
                )
                try:
                    future.result(timeout=5)
                except Exception:
                    # Closing is best effort: the loop may already be stopped
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

    async def _disconnect_client(self):
        """Disconnects this client from the server without stopping the event loop.

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
                # Closing is best effort: the connection may already be gone
                pass
        # _bgTasks is always empty for client instances; no tasks to cancel.

    async def _shutdown_loop(self):
        """Cancels all background tasks and stops the event loop cleanly."""
        if self._stopEvent is not None:
            self._stopEvent.set()
        if self._server is not None:
            self._server.close()
        # Cancelling a client's task closes its connection, so the client learns at once that the server is gone.
        clientTasks = list(self._clientTasks)
        for clientTask in clientTasks:
            clientTask.cancel()
        if clientTasks:
            await asyncio.wait(clientTasks, timeout=_CLIENT_STOP_TIMEOUT)
        if self._writer is not None:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except Exception:
                # Closing is best effort: the connection may already be gone
                pass
        tasks = list(getattr(self, '_bgTasks', []))
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._loop.call_soon(self._loop.stop)

    def _require_password_decided(self):
        """Refuses to start a locker that lost its password.

        :Raises:
            #. AuthenticationError: If the locker is an unpickled copy that was not given a password.
        """
        if self.__password is _PASSWORD_NOT_SET:
            raise AuthenticationError(
                "This locker has no password: call set_password(...) first, "
                "or set_password(None) to run without one"
            )

    def start(self, address=None, port=None, password=None, connectAttempts=3):
        """Starts this instance as a server or connects it as a client.

        :Parameters:
            #. address (None, str): Server IP to connect to.  Both *address*
               and *port* must be given together.
            #. port (None, int): Server port to connect to.
            #. password (None, str, bytes): Override the instance password for
               this connection attempt.
            #. connectAttempts (int): Number of connection attempts before giving up.

        :Raises:
            #. ValueError: If only one of address and port is given, or password is empty.
            #. RuntimeError: If the election for the server file times out.
            #. AuthenticationError: If the server rejects the password.
        """
        if self.isServer:
            self._warn(f"'{self.__name}' is already serving")
            return
        if self.isClient:
            self._warn(f"'{self.__name}' is already a client")
            return
        if password is not None:
            _check_password('password', password)
            self.__password = _to_bytes(password)
        self._require_password_decided()
        self._start_loop()
        self._run_sync(self._init_async_state())
        if address is not None or port is not None:
            if address is None or port is None:
                raise ValueError("address and port must both be given or both be None")
            success = self.connect(address=address, port=port, connectAttempts=connectAttempts)
            if not success:
                self._error(f"Failed to connect to {address}:{port}")
                return
        else:
            self._serve_or_connect(connectAttempts=connectAttempts)
        if self.__blocking and self.isServer and self._loopThread is not None:
            self._loopThread.join()

    async def _init_async_state(self):
        """Initialises asyncio synchronisation objects on the background loop."""
        self._stopEvent  = asyncio.Event()
        self._lockQueue  = asyncio.Queue()
        self._queueEvent = asyncio.Event()
        self._pathsLUT   = {}
        self._clientsLUT = {}
        self._pending    = {}

    def connect(self, address, port, password=None, connectAttempts=3):
        """Connects to a running server at *address*:*port*.

        :Parameters:
            #. address (str): Server IP address.
            #. port (int): Server port.
            #. password (None, str, bytes): Override the instance password.
            #. connectAttempts (int): Number of connection attempts.

        :Returns:
            #. success (bool): Whether the connection was established.

        :Raises:
            #. ValueError: If password is empty.
            #. AuthenticationError: If the server rejects the password.
        """
        if password is not None:
            _check_password('password', password)
            self.__password = _to_bytes(password)
        self._require_password_decided()
        try:
            return self._run_sync(
                self._client_connect(address, int(port), connectAttempts),
                timeout=self.__connectTimeout + 2
            )
        except AuthenticationError:
            raise
        except Exception as error:
            self._error(f"connect() failed: {error}")
            return False

    def _serve_or_connect(self, connectAttempts=3):
        """Auto-discovers a running server or wins the election to become one.

        When multiple processes start simultaneously with a shared *serverFile*,
        this method implements the three-step election protocol described in
        the module docstring to ensure exactly one process becomes the server.

        If *serverFile* is ``None`` the election is skipped: the process
        starts a server directly (if ``allowServing`` is ``True``) or emits a
        warning and returns.

        :Parameters:
            #. connectAttempts (int): Number of TCP connection attempts passed to
               ``connect()`` when a live server is found.
        """
        # Resolve the effective election timeout: prefer the per-instance
        # override, fall back to the module-level constant.
        electionTimeout = (
            self.__electionTimeout
            if self.__electionTimeout is not None
            else _ELECTION_TIMEOUT
        )
        # No server file: skip the election and fall back to direct behaviour.
        if not self.__serverFile:
            self._serve_without_file()
            return
        self._prepare_server_file()
        electionStart = time.time()
        while True:
            if time.time() - electionStart > electionTimeout:
                raise RuntimeError(
                    f"ServerLocker '{self.__name}' election timed out after {electionTimeout:.0f} seconds."
                    " Multiple processes may be racing for the same serverFile"
                    " or a crashed process left a stale placeholder that cannot"
                    " be cleared.  Check for dead fingerprint files."
                )
            # Read the file, decide and write the claim as one step so two
            # processes can never both see a vacant file and both claim it.
            with self._election_lock():
                state = self._read_election_state()
                canClaim = self.__allowServing and not state.isLive and not state.isPending
                if canClaim:
                    self._write_claim()
            if state.isLive and state.ownerName != self.__uniqueName:
                if self.connect(address=state.address, port=int(state.port), connectAttempts=connectAttempts):
                    return
                time.sleep(_ELECTION_CLAIM_POLL)
                continue
            if state.isLive:
                return
            if state.isPending:
                time.sleep(_ELECTION_CLAIM_POLL)
                continue
            if not self.__allowServing:
                if not state.isEmpty and not state.isStale:
                    time.sleep(_ELECTION_CLAIM_POLL)
                    continue
                self._warn(
                    f"'{self.__name}' found no live server and allowServing is False — "
                    "giving up"
                )
                return
            if not self._confirm_claim():
                if self._wait_for_winner(connectAttempts, electionTimeout):
                    return
                continue
            self._announce_server()
            return

    def _serve_without_file(self):
        """Starts a server directly when no server file is configured."""
        if self.__allowServing:
            try:
                self._run_sync(self._start_server(),
                               timeout=self.__connectTimeout + 2)
            except Exception as error:
                self._error(f"Failed to start server: {error}")
        else:
            self._warn(
                "No serverFile configured and allowServing is False — "
                "cannot auto-discover server"
            )

    def _prepare_server_file(self):
        """Creates the directory and the empty file that will hold the fingerprint.

        :Raises:
            #. PermissionError: If the file or its folder cannot be trusted, so the election never runs through a file another user controls.
        """
        serverDir = os.path.dirname(self.__serverFile)
        if serverDir:
            os.makedirs(serverDir, exist_ok=True)
        if not os.path.lexists(self.__serverFile):
            try:
                _write_server_file_text(self.__serverFile, '')
            except PermissionError:
                raise
            except Exception as error:
                self._warn(f"Unable to create the server file: {error}")
        problem = _server_file_problem(self.__serverFile)
        if problem is not None:
            raise PermissionError(f"serverFile '{self.__serverFile}' {problem}")

    def _read_election_state(self):
        """Reads the fingerprint file and classifies what it says about the server.

        :Returns:
            #. state (_ElectionState): The owner, address, port and process ID
               written in the file, plus whether the file is empty, describes
               a live server, a stale one, or a server that is still starting.
        """
        ownerName, timestamp, address, port, pid = self.get_running_server_fingerprint(
            raiseNotFound=False, raiseError=False
        )
        isEmpty           = ownerName is None
        isPlaceholder     = timestamp is None or timestamp == _ELECTION_PLACEHOLDER_TIMESTAMP
        isPortPlaceholder = port is None or port == _ELECTION_PLACEHOLDER_PORT
        timestampAge = float('inf')
        if not isPlaceholder:
            try:
                timestampAge = time.time() - float(timestamp)
            except (ValueError, TypeError):
                isPlaceholder = True
        isLive  = not isEmpty and not isPortPlaceholder and timestampAge < _ELECTION_STALE_THRESHOLD
        isStale = not isEmpty and timestampAge >= _ELECTION_STALE_THRESHOLD
        isForeign = not isEmpty and ownerName != self.__uniqueName
        if isPlaceholder:
            isClaimAbandoned = self._is_claim_abandoned(pid, address)
        else:
            isClaimAbandoned = isStale
        # A foreign claim or stamp without a published port is a server that
        # is still starting, unless its writer has abandoned it.
        isPending = isForeign and isPortPlaceholder and not isClaimAbandoned
        return _ElectionState(ownerName, address, port, pid,
                              isEmpty, isLive, isStale, isPending)

    def _is_claim_abandoned(self, pid, address):
        """Tells whether a claim that was never stamped has been abandoned by its writer.

        :Parameters:
            #. pid (None, str): Process ID written next to the claim.
            #. address (None, str): Address written next to the claim.

        :Returns:
            #. isAbandoned (bool): ``True`` if the claim is older than
               ``_ELECTION_STALE_THRESHOLD``, or its writer runs on this
               machine and is gone.
        """
        try:
            claimAge = time.time() - os.path.getmtime(self.__serverFile)
        except OSError:
            return False
        if claimAge >= _ELECTION_STALE_THRESHOLD:
            return True
        isLocal = address in (self.__address, _LOOPBACK_ADDRESS)
        return isLocal and pid is not None and not _process_exists(pid)

    def _election_lock(self):
        """Returns the cross-process lock that serialises election decisions."""
        return _ElectionLock(self.__serverFile + _LOCK_FILE_SUFFIX, self._warn)

    def _write_claim(self):
        """Writes this locker's claim line, with placeholder time and port, to the server file."""
        self._write_server_file(_format_fingerprint(
            self.__uniqueName, _ELECTION_PLACEHOLDER_TIMESTAMP,
            self.__address, _ELECTION_PLACEHOLDER_PORT, self.__pid
        ))

    def _confirm_claim(self):
        """Checks after one poll interval that this locker's claim is still in the file.

        :Returns:
            #. hasWon (bool): ``True`` if the claim is still in the file,
               ``False`` if another locker overwrote it.
        """
        time.sleep(_ELECTION_CLAIM_POLL)
        winnerName = self.get_running_server_fingerprint(
            raiseNotFound=False, raiseError=False
        )[0]
        if winnerName != self.__uniqueName:
            self._info(
                f"'{self.__uniqueName}' lost election to '{winnerName}' — "
                "waiting for server port"
            )
            return False
        return True

    def _wait_for_winner(self, connectAttempts, electionTimeout):
        """Waits for the election winner to publish its port, then connects to it.

        :Parameters:
            #. connectAttempts (int): Number of TCP connection attempts.
            #. electionTimeout (float): Seconds to wait for the winner.

        :Returns:
            #. isConnected (bool): ``True`` if connected to the winner,
               ``False`` if the winner went stale, the wait timed out, or the
               connection failed.
        """
        waitStart = time.time()
        while time.time() - waitStart < electionTimeout:
            time.sleep(_ELECTION_CLAIM_POLL)
            _, timestamp, address, port, pid = self.get_running_server_fingerprint(
                raiseNotFound=False, raiseError=False
            )
            hasRealPort = port is not None and port != _ELECTION_PLACEHOLDER_PORT
            if hasRealPort:
                return self.connect(address=address, port=int(port), connectAttempts=connectAttempts)
            isWinnerStale = False
            if timestamp == _ELECTION_PLACEHOLDER_TIMESTAMP:
                # A claim that was never stamped has no time to age by, so its file age and process decide.
                isWinnerStale = self._is_claim_abandoned(pid, address)
            elif timestamp:
                try:
                    isWinnerStale = (
                        time.time() - float(timestamp) >= _ELECTION_STALE_THRESHOLD
                    )
                except (ValueError, TypeError):
                    # A timestamp that is not a number leaves the winner counted as alive
                    pass
            if isWinnerStale:
                return False
        return False

    def _announce_server(self):
        """Stamps the fingerprint with a real time, binds the server and announces it."""
        stampLine = _format_fingerprint(
            self.__uniqueName, time.time(),
            self.__address, _ELECTION_PLACEHOLDER_PORT, self.__pid
        )
        self._write_election_raw(stampLine)
        try:
            self._run_sync(self._start_server(),
                           timeout=self.__connectTimeout + 2)
        except Exception as error:
            self._error(
                f"'{self.__name}' failed to start server after winning "
                f"election: {error}"
            )
            try:
                with self._serverFileLock:
                    _write_server_file_text(self.__serverFile, '')
            except Exception as clearError:
                self._warn(f"Unable to clear the server file: {clearError}")
            return
        self._wasServer = True
        self._info(f"'{self.__name}' is now the server")

    # ------------------------------------------------------------------
    # Wire I/O helpers — dispatch to JSON or msgpack based on serializer
    # ------------------------------------------------------------------

    async def _write_message(self, writer, content):
        """Writes one message using the configured serializer.

        Routes to 4-byte length-prefixed msgpack framing when
        ``self._useMsgpack`` is ``True``, or newline-delimited JSON
        otherwise.  All TCP write paths in the server and client call this
        method so that the serializer choice is enforced in one place.
        """
        if self._useMsgpack:
            await _write_msgpack(writer, content)
        else:
            await _write_json(writer, content)

    async def _read_message(self, reader):
        """Reads one message using the configured serializer.

        Routes to msgpack length-prefix framing when ``self._useMsgpack``
        is ``True``, or newline-delimited JSON otherwise.  Returns ``None``
        when the peer closes the connection cleanly (both formats).
        """
        if self._useMsgpack:
            try:
                return await _read_msgpack(reader, self.__maxMessageBytes)
            except asyncio.IncompleteReadError:
                return None
        else:
            return await _read_json(reader)

    async def _read_message_timeout(self, reader, timeout):
        """Reads one message with a deadline using the configured serializer.

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
                return await asyncio.wait_for(_read_msgpack(reader, self.__maxMessageBytes), timeout=timeout)
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
        """Binds the asyncio TCP server and launches background tasks.

        Tries the configured port first, then increments until a free port
        is found.  Writes the fully-resolved fingerprint (announce step of the
        election) so waiting clients can discover the real port.  Launches
        three background tasks: the queue processor, the maximum-hold-time
        eviction monitor, and the heartbeat.
        """
        port = self.__port
        listenAddress = _ANY_ADDRESS if self.__allowRemoteClients else _LOOPBACK_ADDRESS
        while True:
            try:
                self._server = await asyncio.start_server(
                    self._handle_client, listenAddress, port, limit=self.__maxMessageBytes
                )
                break
            except OSError:
                port += 1
                if port > 65535:
                    raise RuntimeError("No available port found")
        self._serverPort = port
        self._write_fingerprint(port)
        self._info(f"Server started on port {port}")
        self._bgTasks = [
            self._loop.create_task(self._queue_processor()),
            self._loop.create_task(self._max_time_monitor()),
            self._loop.create_task(self._heartbeat()),
            self._loop.create_task(self._dead_pid_monitor()),
        ]

    async def _heartbeat(self):
        """Refreshes the server fingerprint file at regular intervals.

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
                # The timeout is the normal wake-up when nothing happened
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
                        f"Heartbeat: fingerprint owner changed to '{uname}' — "
                        f"possible split-brain; stopping server '{self.__uniqueName}'."
                    )
                    self._stopEvent.set()
                    break
                self._write_fingerprint(self._serverPort)

    async def _authenticate_client(self, reader, writer):
        """Runs the server side of the login and refuses clients that fail it.

        A refused connection is closed here, so the caller only has to stop.

        :Parameters:
            #. reader (asyncio.StreamReader): The connection's reader.
            #. writer (asyncio.StreamWriter): The connection's writer.

        :Returns:
            #. login (tuple, None): The client's hello message and the server's proof for the
               welcome message (None without a password), or None when the connection was
               refused and closed.
        """
        # Bootstrap: the login and the welcome always use JSON so
        # that the serializer can be negotiated before lock traffic starts.
        try:
            hello = await asyncio.wait_for(_read_json(reader), timeout=_HANDSHAKE_TIMEOUT)
        except asyncio.TimeoutError:
            self._warn(f"Connection dropped: no hello message within {_HANDSHAKE_TIMEOUT} s")
            _close_writer(writer)
            return None
        if not isinstance(hello, dict):
            writer.close()
            return None
        clientKey = self._block_key(writer, hello)
        secondsLeft = self._block_seconds_left(clientKey)
        if secondsLeft > 0:
            self._warn(f"Connection from {clientKey} refused: blocked for another {secondsLeft:.0f} s")
            await self._refuse_login(writer, 'blocked')
            return None
        if self.__password is None:
            await _write_json(writer, {'action': 'challenge', 'auth_required': False})
            return hello, None
        return await self._verify_client_proof(reader, writer, hello, clientKey)

    async def _verify_client_proof(self, reader, writer, hello, clientKey):
        """Challenges a client and checks that it knows the password.

        :Parameters:
            #. reader (asyncio.StreamReader): The connection's reader.
            #. writer (asyncio.StreamWriter): The connection's writer.
            #. hello (dict): The client's hello message, which holds its random value.
            #. clientKey (str): What identifies the client in the failure count.

        :Returns:
            #. login (tuple, None): The hello message and the server's proof, or None when the
               login failed and the connection was closed.
        """
        clientNonce = hello.get('client_nonce')
        if not isinstance(clientNonce, str) or not 8 <= len(clientNonce) <= 128:
            await self._reject_login(writer, clientKey, 'unsupported login; update pylocker')
            return None
        serverNonce = secrets.token_hex(_AUTH_NONCE_BYTES)
        await _write_json(writer, {'action': 'challenge', 'auth_required': True, 'server_nonce': serverNonce})
        try:
            answer = await asyncio.wait_for(_read_json(reader), timeout=_HANDSHAKE_TIMEOUT)
        except asyncio.TimeoutError:
            self._warn(f"Connection from {clientKey} dropped: no login answer within {_HANDSHAKE_TIMEOUT} s")
            _close_writer(writer)
            return None
        proof = answer.get('proof') if isinstance(answer, dict) else None
        expected = _make_proof(self.__password, 'client', clientNonce, serverNonce)
        if not _proofs_match(proof, expected):
            await self._reject_login(writer, clientKey, 'bad password')
            return None
        self._clear_auth_failures(clientKey)
        return hello, _make_proof(self.__password, 'server', clientNonce, serverNonce)

    async def _refuse_login(self, writer, reason):
        """Tells a client why it is refused and closes the connection.

        :Parameters:
            #. writer (asyncio.StreamWriter): The connection's writer.
            #. reason (str): The reason sent to the client.
        """
        await _write_json(writer, {'action': 'error', 'reason': reason})
        _close_writer(writer)

    async def _reject_login(self, writer, clientKey, reason):
        """Counts a failed login, blocks the client when it failed too often, and refuses it.

        :Parameters:
            #. writer (asyncio.StreamWriter): The connection's writer.
            #. clientKey (str): What identifies the client in the failure count.
            #. reason (str): The reason sent to the client.
        """
        isNowBlocked = self._record_auth_failure(clientKey)
        self._warn(f"Login from {clientKey} rejected: {reason}")
        if isNowBlocked:
            self._error(f"{clientKey} blocked for {self.__blockSeconds:.0f} s "
                        f"after {self.__maxAuthFailures} failed logins")
        await self._refuse_login(writer, reason)

    @staticmethod
    def _block_key(writer, hello):
        """Names the client whose failed logins are counted.

        A connection from another computer is identified by its address, which the
        sender cannot fake. A connection from this computer always has the same
        address, so it is identified by the process id and the unique name it
        reports. Nobody else knows the unique name of a locker, so a program cannot
        block another one; but a program that changes its name is not stopped.

        :Parameters:
            #. writer (asyncio.StreamWriter): The connection's writer.
            #. hello (dict): The client's hello message.

        :Returns:
            #. clientKey (str): A short text that identifies the client.
        """
        peer = writer.get_extra_info('peername')
        peerAddress = peer[0] if peer else 'unknown'
        if peerAddress == _LOOPBACK_ADDRESS:
            return f"local process {str(hello.get('pid'))[:20]!r} {str(hello.get('unique_name'))[:50]!r}"
        return f"address {peerAddress}"

    def _block_seconds_left(self, clientKey):
        """Tells how long a client stays blocked.

        :Parameters:
            #. clientKey (str): What identifies the client in the failure count.

        :Returns:
            #. secondsLeft (float): Seconds until the block ends, 0 when the client is not blocked.
        """
        with self._authLock:
            record = self._authFailures.get(clientKey)
            if record is None:
                return 0.0
            return max(record['blockedUntil'] - time.time(), 0.0)

    def _record_auth_failure(self, clientKey):
        """Adds one failed login to a client's count and blocks the client at the limit.

        :Parameters:
            #. clientKey (str): What identifies the client in the failure count.

        :Returns:
            #. isNowBlocked (bool): Whether this failure made the client blocked.
        """
        if self.__maxAuthFailures is None:
            return False
        now = time.time()
        with self._authLock:
            self._forget_old_failures(now)
            record = self._authFailures.setdefault(clientKey, {'count': 0, 'last': now, 'blockedUntil': 0.0})
            record['count'] += 1
            record['last'] = now
            if record['count'] < self.__maxAuthFailures:
                return False
            record['count'] = 0
            record['blockedUntil'] = now + self.__blockSeconds
            return True

    def _forget_old_failures(self, now):
        """Drops the failure records that are no longer needed. The caller holds the lock.

        :Parameters:
            #. now (float): The current epoch time.
        """
        for clientKey, record in list(self._authFailures.items()):
            isOver = record['blockedUntil'] <= now and now - record['last'] >= self.__blockSeconds
            if isOver:
                del self._authFailures[clientKey]
        # A flood of different senders must not grow the table without limit
        while len(self._authFailures) >= _MAX_TRACKED_CLIENTS:
            oldest = min(self._authFailures, key=lambda clientKey: self._authFailures[clientKey]['last'])
            del self._authFailures[oldest]

    def _clear_auth_failures(self, clientKey):
        """Forgets the failed logins of a client that logged in correctly.

        :Parameters:
            #. clientKey (str): What identifies the client in the failure count.
        """
        with self._authLock:
            self._authFailures.pop(clientKey, None)

    @property
    def blockedClients(self):
        """The clients that are blocked now, as a dictionary of client key to seconds left."""
        now = time.time()
        with self._authLock:
            return {clientKey: record['blockedUntil'] - now for clientKey, record in self._authFailures.items()
                    if record['blockedUntil'] > now}

    def unblock_client(self, clientKey=None):
        """Lifts the block and clears the failed-login count of one client or of all clients.

        :Parameters:
            #. clientKey (None, str): The client key as shown by ``blockedClients``, or None for every client.
        """
        with self._authLock:
            if clientKey is None:
                self._authFailures.clear()
            else:
                self._authFailures.pop(clientKey, None)

    async def _dispatch_request(self, request, writer, clientUniqueName):
        """Routes one client request to its handler.

        :Parameters:
            #. request (dict): The decoded client message.
            #. writer (asyncio.StreamWriter): The writer of the client connection.
            #. clientUniqueName (str): Unique name of the client, used in warnings.
        """
        action = request.get('action')
        # The sender's name comes from the login, not from the message, so a client cannot speak for another
        request['client_unique_name'] = clientUniqueName
        if action == 'acquire':
            problem = self._acquire_request_problem(request)
            if problem is not None:
                self._warn(f"Dropped acquire request from client {_log_text(clientUniqueName)}: {problem}")
                return
            if self._pendingCounts.get(writer, 0) >= self.__maxPendingPerClient:
                self._warn(f"Dropped acquire request from client {_log_text(clientUniqueName)}: "
                           f"{self.__maxPendingPerClient} requests are already waiting")
                return
            self._pendingCounts[writer] = self._pendingCounts.get(writer, 0) + 1
            # Only the server itself attaches a future; one sent by a client would break the queue processor.
            request.pop('_future', None)
            request['_writer'] = writer
            # Without a time the request would count as new on every pass and never expire
            request.setdefault('request_utctime', time.time())
            await self._lockQueue.put(request)
            self._queueEvent.set()
        elif action == 'release':
            await self._server_release(request, writer)
        elif action == 'publish':
            problem = self._publication_problem(request)
            if problem is not None:
                self._warn(f"Dropped publication from client {_log_text(clientUniqueName)}: {problem}")
                return
            await self._server_publish(request)
        elif action == 'order':
            problem = self._order_message_problem(request)
            if problem is not None:
                self._warn(f"Dropped order from client {_log_text(clientUniqueName)}: {problem}")
                return
            await self._server_order(request)
        else:
            self._warn(f"Unknown action {_log_text(action)} from client {_log_text(clientUniqueName)}")

    def _admit_to_login_pool(self, clientTask, writer):
        """Adds a new connection to the ones that are logging in, closing the oldest if there are too many.

        A program that opens connections and says nothing is the oldest in the pool, so it is the one
        that is closed when real clients arrive, and they are never turned away because of it.

        :Parameters:
            #. clientTask (asyncio.Task): The task that handles the new connection.
            #. writer (asyncio.StreamWriter): The writer of the new connection.
        """
        self._loginWriters[clientTask] = writer
        while len(self._loginWriters) > self.__maxLoginsInProgress:
            oldestTask = next(iter(self._loginWriters))
            _close_writer(self._loginWriters.pop(oldestTask))

    @staticmethod
    def _hello_problem(hello):
        """Finds what is wrong with the identity a client announced in its hello message.

        :Parameters:
            #. hello (dict): The client's hello message.

        :Returns:
            #. problem (str, None): A short description, or None when the identity is valid.
        """
        for key in ('name', 'unique_name'):
            value = hello.get(key, '')
            if not isinstance(value, str) or len(value) > _MAX_NAME_LENGTH:
                return 'invalid hello'
        return None

    @staticmethod
    def _acquire_request_problem(request):
        """Finds what is wrong with an acquire request received from a client.

        :Parameters:
            #. request (dict): The decoded acquire message.

        :Returns:
            #. problem (str, None): A description of the first problem found, or None when the request is valid.
        """
        if not isinstance(request.get('request_unique_id'), str):
            return 'request_unique_id must be a string'
        paths = request.get('path')
        isPathList = isinstance(paths, list) and len(paths) > 0
        if not isPathList or not all(isinstance(lockPath, str) for lockPath in paths):
            return 'path must be a non-empty list of strings'
        if len(paths) > _MAX_PATHS_PER_REQUEST:
            return f'path must not have more than {_MAX_PATHS_PER_REQUEST} entries'
        for key in ('timeout', 'request_utctime'):
            if key not in request:
                continue
            value = request[key]
            isNumber = isinstance(value, (int, float)) and not isinstance(value, bool)
            if not isNumber or not math.isfinite(value):
                return f'{key} must be a finite number'
        if 'timeout' in request and not request['timeout'] > 0:
            return 'timeout must be positive'
        if not isinstance(request.get('client_unique_name', ''), str):
            return 'client_unique_name must be a string'
        return None

    def _cleanup_client(self, writer, clientUniqueName):
        """Frees everything a disconnected client held and closes its connection.

        :Parameters:
            #. writer (asyncio.StreamWriter): The writer of the client connection.
            #. clientUniqueName (str, None): Unique name of the client, or
               None when the handshake never completed.
        """
        if clientUniqueName is not None and self._pathsLUT is not None:
            released = [lockPath for lockPath, record in list(self._pathsLUT.items())
                        if record.get('_writer') is writer]
            for path in released:
                self._drop_lock(path)
            if released:
                self._warn(f"Force-released {len(released)} lock(s) for "
                           f"disconnected client {_log_text(clientUniqueName)}")
                self._queueEvent.set()
            record = self._clientsLUT.get(clientUniqueName)
            if record is not None and record['writer'] is writer:
                del self._clientsLUT[clientUniqueName]
        _close_writer(writer)
        # Notify the queue processor so it can unblock any pending
        # requests that were waiting on a path held by this client.
        if self._queueEvent is not None:
            self._queueEvent.set()

    async def _handle_client(self, reader, writer):
        """Handles a single client connection from handshake to disconnect."""
        clientName       = None
        clientUniqueName = None
        clientTask       = asyncio.current_task()
        self._clientTasks.add(clientTask)
        try:
            self._admit_to_login_pool(clientTask, writer)
            login = await self._authenticate_client(reader, writer)
            self._loginWriters.pop(clientTask, None)
            if login is None:
                return
            hello, serverProof = login
            problem = self._hello_problem(hello)
            if problem is not None:
                self._warn(f"Connection refused: {problem}")
                await self._refuse_login(writer, problem)
                return
            clientName       = hello.get('name', 'unknown')
            clientUniqueName = hello.get('unique_name', str(uuid.uuid4()))
            if clientUniqueName == self.__uniqueName or clientUniqueName in self._clientsLUT:
                self._warn(f"Connection refused: the name {_log_text(clientUniqueName)} is already in use")
                await self._refuse_login(writer, 'name in use')
                return
            if len(self._clientsLUT) >= self.__maxClients:
                self._warn(f"Connection refused: the limit of {self.__maxClients} clients is reached")
                await self._refuse_login(writer, 'server full')
                return
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
                'server_proof':               serverProof,
                'serializer':                 'msgpack' if self._useMsgpack else 'json',
            })
            # All messages after this point use the announced serializer.
            self._clientsLUT[clientUniqueName] = {
                'writer':  writer,
                'pid':     clientPid,
                'address': clientAddress,
                'name':    clientName,
            }
            self._info(f"Client {_log_text(clientName)}:{_log_text(clientUniqueName)} connected")
            while not self._stopEvent.is_set():
                try:
                    request = await self._read_message_timeout(reader, 30.0)
                except asyncio.TimeoutError:
                    # No message arrived in the wait; check the stop flag and read again
                    continue
                if request is None:
                    break
                await self._dispatch_request(request, writer, clientUniqueName)
        except _MessageTooLargeError as error:
            self._warn(f"Client {_log_text(clientUniqueName)} disconnected: {error}")
        except (ConnectionResetError, BrokenPipeError, asyncio.IncompleteReadError):
            # The peer disconnected, which is the normal end of a connection
            pass
        except Exception as error:
            self._error(f"Client handler error for {_log_text(clientUniqueName)}: {error}")
        finally:
            self._clientTasks.discard(clientTask)
            self._loginWriters.pop(clientTask, None)
            self._cleanup_client(writer, clientUniqueName)

    async def _queue_processor(self):
        """Continuously tries to satisfy pending acquire requests."""
        pending = []
        while not self._stopEvent.is_set():
            while True:
                try:
                    pending.append(self._lockQueue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            now        = time.time()
            stillWaiting = []
            for request in pending:
                try:
                    isWaiting = await self._process_pending_request(request, now)
                except Exception as error:
                    # One unexpected request must not end the task that serves every client.
                    self._error(f"Dropped request {_log_text(request.get('request_unique_id'))} "
                                f"after an unexpected error: {error}")
                    self._free_request_slot(request)
                    continue
                if isWaiting:
                    stillWaiting.append(request)
                else:
                    self._free_request_slot(request)
            pending = stillWaiting
            self._queueEvent.clear()
            if pending:
                try:
                    await asyncio.wait_for(self._queueEvent.wait(), timeout=_QUEUE_POLL)
                except asyncio.TimeoutError:
                    # The timeout is the normal wake-up when nothing happened
                    pass
            else:
                try:
                    await asyncio.wait_for(self._queueEvent.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    # The timeout is the normal wake-up when nothing happened
                    pass

    def _drop_lock(self, path):
        """Removes a lock from the table and counts it as no longer held by its connection.

        :Parameters:
            #. path (str): The locked path.
        """
        record = self._pathsLUT.pop(path, None)
        owner = None if record is None else record.get('_writer')
        if owner is None:
            return
        remaining = self._heldCounts.get(owner, 0) - 1
        if remaining > 0:
            self._heldCounts[owner] = remaining
        else:
            self._heldCounts.pop(owner, None)

    def _free_request_slot(self, request):
        """Counts a request as no longer waiting, so its connection may send another.

        :Parameters:
            #. request (dict): The request that was granted, dropped or expired.
        """
        requestWriter = request.get('_writer')
        if requestWriter is None:
            return
        remaining = self._pendingCounts.get(requestWriter, 0) - 1
        if remaining > 0:
            self._pendingCounts[requestWriter] = remaining
        else:
            self._pendingCounts.pop(requestWriter, None)

    async def _process_pending_request(self, request, now):
        """Grants one queued acquire request, drops it, or leaves it waiting.

        :Parameters:
            #. request (dict): The queued acquire request.
            #. now (float): The current epoch time.

        :Returns:
            #. isWaiting (bool): Whether the request must stay in the queue because a path is held.
        """
        requestId   = request['request_unique_id']
        paths   = request['path']
        timeout = request.get('timeout', self.__defaultTimeout)
        requestTime = request.get('request_utctime', now)
        if (now - requestTime) >= timeout:
            self._warn(f"Request {_log_text(requestId)} expired in queue")
            future = request.get('_future')
            if future is not None and not future.done():
                future.set_result((False, 0))
            return False
        # The client may have disconnected while waiting; granting it
        # the lock would hold the path until maxLockTime.
        requestWriter = request.get('_writer')
        if requestWriter is not None and requestWriter.is_closing():
            self._warn(f"Request {_log_text(requestId)} dropped: client disconnected")
            return False
        blocked = any(lockPath in self._pathsLUT for lockPath in paths)
        if blocked:
            return True
        # The lock table keeps one entry for each distinct path, so a repeated path counts once
        pathCount = len(set(paths))
        if requestWriter is not None and self._heldCounts.get(requestWriter, 0) + pathCount > self.__maxLocksPerClient:
            # The request waits, as for a held path, until the client releases some of its locks
            if not request.get('_isOverBudget'):
                request['_isOverBudget'] = True
                self._warn(f"Request {_log_text(requestId)} waits: its client holds "
                           f"{self._heldCounts.get(requestWriter, 0)} of {self.__maxLocksPerClient} allowed locks")
            return True
        acquiredTime = time.time()
        for lockPath in paths:
            self._pathsLUT[lockPath] = {
                'request_unique_id':  requestId,
                '_writer':            request.get('_writer'),
                'acquired_utctime':   acquiredTime,
                'path':               lockPath,
                'client_unique_name': request.get('client_unique_name'),
            }
        if requestWriter is not None:
            self._heldCounts[requestWriter] = self._heldCounts.get(requestWriter, 0) + pathCount
        future = request.get('_future')
        if future is not None:
            if not future.done():
                future.set_result((True, requestId))
        else:
            writer = request.get('_writer')
            if writer is not None:
                await self._write_message(writer, {'action': 'acquired',
                                           'request_unique_id': requestId})
        self._info(f"Lock {_log_text(requestId)} acquired for paths {paths}")
        return False

    async def _max_time_monitor(self):
        """Periodically evicts locks that have exceeded the maximum hold time."""
        while not self._stopEvent.is_set():
            await asyncio.sleep(_MONITOR_POLL)
            now     = time.time()
            expired = [
                (lockPath, record) for lockPath, record in list(self._pathsLUT.items())
                if now - record.get('acquired_utctime', now) >= self.__maxLockTime
            ]
            evictedRequests = {}
            for path, record in expired:
                self._drop_lock(path)
                self._warn(f"Force-released lock on {_log_text(path)} (max lock time exceeded)")
                evictedRequests[record.get('request_unique_id')] = record
            for requestId, record in evictedRequests.items():
                await self._notify_eviction(requestId, record)
            if expired:
                self._queueEvent.set()

    async def _notify_eviction(self, requestId, record):
        """Tells the holder of an evicted lock that the server took the lock back.

        :Parameters:
            #. requestId (str): The unique identifier of the evicted lock.
            #. record (dict): The server's record of the lock, which names the connection of the holder.
        """
        writer = record.get('_writer')
        if writer is None:
            # The server held the lock itself, so there is no connection to tell.
            with self.__ownAcquiredLock:
                self.__ownAcquired.pop(requestId, None)
            return
        if writer.is_closing():
            return
        await self._write_message(writer, {'action': 'exceeded_maximum_lock_time',
                                           'request_unique_id': requestId})

    async def _server_release(self, request, writer=None):
        """Processes a release request on the server side.

        :Parameters:
            #. request (dict): The release request.
            #. writer (None, asyncio.StreamWriter): The connection that sent the request, so only
               its own locks are released; None when the server releases a lock itself.
        """
        requestId = request.get('request_unique_id')
        if not requestId:
            return
        matching = [lockPath for lockPath, record in list(self._pathsLUT.items())
                    if record.get('request_unique_id') == requestId]
        released = [lockPath for lockPath in matching
                    if writer is None or self._pathsLUT[lockPath].get('_writer') is writer]
        for path in released:
            self._drop_lock(path)
        if len(matching) > len(released):
            self._warn(f"Release of lock {_log_text(requestId)} refused: it belongs to another connection")
        if released:
            self._info(f"Released lock {_log_text(requestId)} for paths {released}")
            self._queueEvent.set()

    async def _server_local_acquire(self, requestId, paths, timeout, utcTime):
        """Acquires a lock from within the server process itself.

        :Parameters:
            #. requestId (str): Request unique identifier.
            #. paths (list): Normalised path strings to lock.
            #. timeout (float): Seconds to wait.
            #. utcTime (float): Epoch time when the request was created.

        :Returns:
            #. acquired (bool): Whether the lock was successfully acquired.
            #. lockId (str, int): The lock UUID on success, or ``0`` on timeout.
        """
        future = self._loop.create_future()
        request = {
            'request_unique_id':  requestId,
            'action':             'acquire',
            'path':               paths,
            'timeout':            timeout,
            'request_utctime':    utcTime,
            'client_unique_name': self.__uniqueName,
            'client_name':        self.__name,
            '_future':            future,
            '_writer':            None,
        }
        await self._lockQueue.put(request)
        self._queueEvent.set()
        try:
            return await asyncio.wait_for(asyncio.shield(future), timeout=timeout)
        except asyncio.TimeoutError:
            if not future.done():
                future.cancel()
            return False, 0

    # ------------------------------------------------------------------
    # Client coroutines
    # ------------------------------------------------------------------
    def _hello_message(self, clientNonce):
        """Builds the hello message a client sends to open a connection.

        :Parameters:
            #. clientNonce (str): The client's random value for this login.

        :Returns:
            #. hello (dict): The name, unique name, random value, process id and address.
        """
        return {
            'name':         self.__name,
            'unique_name':  self.__uniqueName,
            'client_nonce': clientNonce,
            # Include PID and address so the server's dead-PID monitor
            # can check whether this process is still alive (same-host
            # connections only — cross-machine PIDs are not probeable).
            'pid':          os.getpid(),
            'address':      self.__address,
        }

    async def _client_login(self, reader, writer):
        """Runs the client side of the login and returns the welcome message.

        :Parameters:
            #. reader (asyncio.StreamReader): The connection's reader.
            #. writer (asyncio.StreamWriter): The connection's writer.

        :Returns:
            #. welcome (dict): The welcome message sent by the server.

        :Raises:
            #. AuthenticationError: If the server refuses the login, or cannot prove that it knows the password.
        """
        clientNonce = secrets.token_hex(_AUTH_NONCE_BYTES)
        await _write_json(writer, self._hello_message(clientNonce))
        challenge = await asyncio.wait_for(_read_json(reader), timeout=self.__connectTimeout)
        self._raise_if_refused(challenge)
        if challenge.get('action') != 'challenge':
            raise AuthenticationError("Server answered with an unexpected message; it runs an incompatible version")
        isRequired = challenge.get('auth_required') is True
        if isRequired and self.__password is None:
            raise AuthenticationError("Server requires a password and this locker has none")
        if not isRequired and self.__password is not None:
            # A server that does not ask for the password cannot prove that it holds it
            raise AuthenticationError("Server does not ask for a password but this locker has one; not connecting")
        serverNonce = challenge.get('server_nonce')
        if isRequired:
            if not isinstance(serverNonce, str):
                raise AuthenticationError("Server sent an invalid challenge")
            await _write_json(writer, {'action': 'auth',
                                       'proof': _make_proof(self.__password, 'client', clientNonce, serverNonce)})
        welcome = await asyncio.wait_for(_read_json(reader), timeout=self.__connectTimeout)
        self._raise_if_refused(welcome)
        if isRequired:
            expected = _make_proof(self.__password, 'server', clientNonce, serverNonce)
            if not _proofs_match(welcome.get('server_proof'), expected):
                raise AuthenticationError("Server did not prove that it knows the password")
        return welcome

    @staticmethod
    def _raise_if_refused(message):
        """Raises AuthenticationError when the server answered with a refusal.

        :Parameters:
            #. message (dict): A message received from the server during the login.

        :Raises:
            #. AuthenticationError: If the login was refused.
            #. ConnectionRefusedError: If the server refused the connection for another reason, such as being full.
        """
        if message.get('action') != 'error':
            return
        reason = message.get('reason', 'unknown')
        if reason in _LOGIN_REFUSALS:
            raise AuthenticationError(f"Server rejected connection: {reason}")
        raise ConnectionRefusedError(f"Server refused the connection: {reason}")

    def _apply_welcome(self, params, address, port):
        """Stores the server details announced in the welcome message.

        :Parameters:
            #. params (dict): The welcome message sent by the server.
            #. address (str): Address the client connected to, used when the
               server does not announce one.
            #. port (int): Port the client connected to, used when the server
               does not announce one.
        """
        self.__serverName        = params.get('server_name', 'unknown')
        self.__serverUniqueName  = params.get('server_unique_name', '')
        self.__serverMaxLockTime = params.get('lock_maximum_acquired_time',
                                              self.__maxLockTime)
        # Capture the server's address and port so they are available
        # via the serverAddress / serverPort properties.
        self.__serverAddress = params.get('server_address', address)
        self.__serverPort    = params.get('server_port', port)

    async def _client_connect(self, address, port, connectAttempts):
        """Opens a TCP connection and completes the handshake.

        :Parameters:
            #. address (str): Server IP address.
            #. port (int): Server port.
            #. connectAttempts (int): Maximum number of attempts.

        :Returns:
            #. success (bool): Whether the connection was established.
        """
        for attempt in range(connectAttempts):
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(address, port, limit=self.__maxMessageBytes),
                    timeout=self.__connectTimeout
                )
                # Bootstrap: the login and the welcome are always JSON so the
                # serializer can be negotiated before lock traffic starts.
                try:
                    params = await self._client_login(reader, writer)
                except AuthenticationError:
                    _close_writer(writer)
                    raise
                self._apply_welcome(params, address, port)
                # ── Serializer negotiation ────────────────────────────
                # The server announces which serializer it will use for all
                # subsequent messages.  Old servers that pre-date negotiation
                # do not include this field; they default to JSON.
                announcedSerializer = params.get('serializer', 'json')
                if announcedSerializer == 'msgpack':
                    if not MSGPACK_AVAILABLE:
                        self._error(
                            f"Server '{self.__serverName}' requires msgpack serializer but "
                            "msgpack is not installed on this client. "
                            "Run:  pip install msgpack"
                        )
                        _close_writer(writer)
                        await asyncio.sleep(0.5)
                        continue
                    self._useMsgpack = True
                else:
                    self._useMsgpack = False
                # All messages after this point use the negotiated serializer.
                self._reader = reader
                self._writer = writer
                self._info("Connected to server "
                           f"'{self.__serverName}:{self.__serverUniqueName}'")
                self._loop.create_task(self._client_reader_loop())
                return True
            except AuthenticationError:
                # A rejected password never succeeds on retry.
                raise
            except Exception as error:
                self._warn(f"Connection attempt {attempt + 1}/{connectAttempts} failed: {error}")
                await asyncio.sleep(0.5)
        return False

    def _handle_server_message(self, message):
        """Handles one message that the server sent to this client.

        :Parameters:
            #. message (dict): The decoded server message.
        """
        action = message.get('action')
        requestId  = message.get('request_unique_id')
        if action == 'acquired':
            future = self._pending.pop(requestId, None)
            if future is not None and not future.done():
                with self.__ownAcquiredLock:
                    self.__ownAcquired[requestId] = message
                future.set_result((True, requestId))
            elif future is None:
                # Stale grant: the client timed out waiting for this
                # lock but the server eventually granted it anyway.
                # Release it immediately so the server slot is not
                # held until maxLockTime expires.
                self._loop.create_task(
                    self._client_release(requestId, [])
                )
                self._warn(
                    f"Stale grant for {_log_text(requestId)} auto-released"
                )
        elif action == 'exceeded_maximum_lock_time':
            with self.__ownAcquiredLock:
                self.__ownAcquired.pop(requestId, None)
            self._warn(f"Server force-released lock {_log_text(requestId)}")
        elif action == 'publish':
            self._store_publication(message)
        elif action == 'order':
            self._handle_order(message)
        else:
            self._info(f"Received message with action {_log_text(action)}")

    async def _client_reader_loop(self):
        """Reads messages from the server and dispatches them."""
        # Latch the wasClient guard immediately so canServe() returns False
        # for the remainder of this instance's lifetime (until reset()).
        self._wasClient = True
        try:
            while not self._stopEvent.is_set():
                try:
                    message = await self._read_message(self._reader)
                except (asyncio.IncompleteReadError, _MessageTooLargeError,
                        ConnectionResetError, BrokenPipeError):
                    # Genuine disconnect, or a stream that can no longer be followed — break so the finally block runs
                    # and cleans up _pending futures, _writer, and _reader.
                    break
                except Exception as error:
                    # Transient framing / deserialization error — keep reading.
                    self._warn(f"Ignored a message that could not be read: {error}")
                    continue
                if message is None:
                    break
                try:
                    self._handle_server_message(message)
                except Exception as error:
                    # One message that cannot be handled must not end the connection to the server
                    self._error(f"Ignored a message from the server: {error}")
        except (ConnectionResetError, BrokenPipeError, asyncio.IncompleteReadError):
            # The peer disconnected, which is the normal end of a connection
            pass
        except Exception as error:
            self._error(f"Client reader loop error: {error}")
        finally:
            for future in list((self._pending or {}).values()):
                if not future.done():
                    future.set_exception(ConnectionError("Server connection lost"))
            if self._pending is not None:
                self._pending.clear()
            if self._writer is not None:
                _close_writer(self._writer)
            self._writer = None
            self._reader = None

    async def _client_acquire(self, requestId, paths, timeout, utcTime):
        """Sends an acquire request to the server and awaits the response.

        :Parameters:
            #. requestId (str): Request unique identifier.
            #. paths (list): Normalised path strings to lock.
            #. timeout (float): Seconds to wait.
            #. utcTime (float): Epoch time when the request was created.

        :Returns:
            #. acquired (bool): Whether the lock was granted.
            #. lockId (str, int): Lock UUID on success, or ``0`` on timeout.
        """
        future = self._loop.create_future()
        self._pending[requestId] = future
        await self._write_message(self._writer, {
            'request_unique_id':  requestId,
            'action':             'acquire',
            'path':               paths,
            'timeout':            timeout,
            'request_utctime':    utcTime,
            'client_unique_name': self.__uniqueName,
            'client_name':        self.__name,
        })
        try:
            result = await asyncio.wait_for(asyncio.shield(future), timeout=timeout)
            return result
        except asyncio.TimeoutError:
            self._pending.pop(requestId, None)
            if not future.done():
                future.cancel()
            return False, 0

    async def _client_release(self, requestId, paths):
        """Sends a release message to the server.

        :Parameters:
            #. requestId (str): Lock unique identifier to release.
            #. paths (list): Paths associated with this lock.
        """
        if self._writer is not None:
            await self._write_message(self._writer, {
                'request_unique_id':  requestId,
                'action':             'release',
                'path':               paths,
                'client_unique_name': self.__uniqueName,
                'client_name':        self.__name,
            })

    # ------------------------------------------------------------------
    # Publications
    # ------------------------------------------------------------------
    @staticmethod
    def _is_finite_number(value):
        """Tells whether a value is a real, finite number (a boolean is not).

        :Parameters:
            #. value (object): The value to test.

        :Returns:
            #. isNumber (bool): Whether the value is an int or float that is not infinite or not-a-number.
        """
        return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)

    @staticmethod
    def _is_name_list(value):
        """Tells whether a value is None or a list of unique names.

        :Parameters:
            #. value (object): The value to test.

        :Returns:
            #. isNameList (bool): Whether the value is None or a list that holds only strings.
        """
        return value is None or (isinstance(value, list) and all(isinstance(name, str) for name in value))

    @classmethod
    def _publication_problem(cls, publication):
        """Finds what is wrong with a publication received from another program.

        :Parameters:
            #. publication (dict): The publish message.

        :Returns:
            #. problem (str, None): A description of the first problem found, or None when the publication is valid.
        """
        if not isinstance(publication.get('message'), str):
            return 'message must be a string'
        for key in ('replace', 'toSelf'):
            if not isinstance(publication.get(key, True), bool):
                return f'{key} must be a boolean'
        if not cls._is_name_list(publication.get('receivers')):
            return 'receivers must be None or a list of strings'
        timeout = publication.get('timeout')
        if timeout is not None and not (cls._is_finite_number(timeout) and timeout > 0):
            return 'timeout must be a positive finite number'
        for key in ('publication_id', 'publisher_unique_name'):
            if not isinstance(publication.get(key, ''), (str, type(None))):
                return f'{key} must be a string'
        return None

    @classmethod
    def _order_message_problem(cls, order):
        """Finds what is wrong with an order received from a client before it is passed on.

        :Parameters:
            #. order (dict): The order message.

        :Returns:
            #. problem (str, None): A description of the first problem found, or None when the order is well formed.
        """
        for key in ('order', 'order_id', 'proof'):
            if not isinstance(order.get(key), str):
                return f'{key} must be a string'
        if not cls._is_finite_number(order.get('sent_at')):
            return 'sent_at must be a finite number'
        args = order.get('args')
        if args is not None and not isinstance(args, list):
            return 'args must be None or a list'
        if not cls._is_name_list(order.get('receivers')):
            return 'receivers must be None or a list of strings'
        return None

    def _set_publications(self, message, entries):
        """Replaces the stored entries of one message and keeps the total count right.

        The caller holds the publications lock.

        :Parameters:
            #. message (str): The publication message key.
            #. entries (list): The entries to store; an empty list removes the key.
        """
        previous = len(self.__publications.get(message, ()))
        if entries:
            self.__publications[message] = entries
        else:
            self.__publications.pop(message, None)
        self.__publicationCount += len(entries) - previous

    def _live_publications(self, message, now):
        """Returns the entries of one message that have not expired, and forgets the ones that have.

        The caller holds the publications lock.

        :Parameters:
            #. message (str): The publication message key.
            #. now (float): The current epoch time.

        :Returns:
            #. entries (list): The entries that are still valid.
        """
        entries = self.__publications.get(message)
        if not entries:
            return []
        live = [entry for entry in entries if entry.get('expires_at', now) >= now]
        if len(live) != len(entries):
            self._set_publications(message, live)
        return live

    def _purge_expired_publications(self, now):
        """Forgets every publication that has expired. The caller holds the publications lock.

        :Parameters:
            #. now (float): The current epoch time.
        """
        for message in list(self.__publications):
            self._live_publications(message, now)

    def _store_publication(self, publication):
        """Stores an incoming publication message (called from any context).

        A publication that is not well formed is ignored with a warning, so
        it cannot break the reader loop that called this method.  A publication
        with a positive ``timeout`` expires that many seconds after it arrived;
        expired entries are forgotten when they are looked up or when room is needed.
        A publication that would take the store over ``maxPublications`` is dropped.
        """
        problem = self._publication_problem(publication)
        if problem is not None:
            self._warn(f"Ignored a publication: {problem}")
            return
        message = publication.get('message')
        replace = publication.get('replace', True)
        now = time.time()
        entry = dict(publication)
        if publication.get('timeout') is not None:
            entry['expires_at'] = now + float(publication['timeout'])
        with self.__publicationsLock:
            for attempt in range(2):
                existing = self._live_publications(message, now)
                entries = [entry] if replace else existing + [entry]
                if self.__publicationCount + len(entries) - len(existing) <= self.__maxPublications:
                    self._set_publications(message, entries)
                    return
                # Room may be taken by entries that expired but were not looked up yet
                self._purge_expired_publications(now)
            self._warn(f"Dropped a publication: {self.__maxPublications} publications are already stored")

    async def _dead_pid_monitor(self):
        """Periodically evicts locks held by same-host client processes that have died.

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
                # The timeout is the normal wake-up when nothing happened
                pass
            if self._clientsLUT is None or self._pathsLUT is None:
                continue
            for clientUniqueName, clientRecord in list(self._clientsLUT.items()):
                pid     = clientRecord.get('pid')
                address = clientRecord.get('address', '')
                if not pid:
                    continue
                # Only probe same-host connections.
                localAddresses = {_LOOPBACK_ADDRESS, 'localhost', self.__address}
                try:
                    # Include the hostname-resolved address so that on machines with
                    # several network cards (network interface cards) the client and
                    # server recognise each other even when _get_ip() chose different cards.
                    localAddresses.add(socket.gethostbyname(socket.gethostname()))
                except OSError:
                    # The host name may not resolve; the other addresses are still compared
                    pass
                if address not in localAddresses:
                    continue
                try:
                    isGone = not _process_exists(pid)
                except Exception:
                    # An unreadable process ID: skip silently to avoid crashing the loop.
                    continue
                if isGone:
                    # PID no longer exists — force-release its locks.
                    self._warn(
                        f"Dead-PID monitor: process {pid} ('{clientUniqueName}') is gone — "
                        "force-releasing all its locks."
                    )
                    if self._pathsLUT is not None:
                        writer  = clientRecord.get('writer')
                        stale   = [lockPath for lockPath, record in list(self._pathsLUT.items())
                                   if record.get('_writer') is writer]
                        for path in stale:
                            self._drop_lock(path)
                        if stale:
                            self._queueEvent.set()
                    self._clientsLUT.pop(clientUniqueName, None)
                    _close_writer(clientRecord['writer'])

    async def _server_publish(self, request):
        """Broadcasts a publication from a client to its target recipients.

        The server stores the message itself when it is a recipient, meaning
        ``receivers`` is None or lists the server's unique name. When the server
        is also the publisher, ``__publish_message`` has already stored its copy.
        ``toSelf=False`` means the sender does not get its own message back; this
        is enforced by skipping the sender in the broadcast loop.

        :Parameters:
            #. request (dict): The publish message from a client, or the
               broadcast payload built by the server itself.
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
            'publication_id':        request.get('publication_id'),
            'timeout':               request.get('timeout'),
        }
        # The server receives the message like any client when it is a target.
        # A publishing server already stored its own copy in __publish_message.
        isServerTarget = receivers is None or self.__uniqueName in receivers
        if isServerTarget and publisherUniqueName != self.__uniqueName:
            self._store_publication(payload)
        for clientUniqueName, clientRecord in list(self._clientsLUT.items()):
            # Apply the receivers filter when one is specified.
            if receivers is not None and clientUniqueName not in receivers:
                continue
            # Honour toSelf=False: do not loop the message back to its sender.
            if not toSelf and clientUniqueName == publisherUniqueName:
                continue
            await self._write_message(clientRecord['writer'], payload)

    async def _server_order(self, request):
        """Relays an order to its target clients and obeys it if the server is a target.

        :Parameters:
            #. request (dict): The order message sent by a client or built by
               the server itself. It holds the order name, its arguments, its
               proof, the optional receivers list and the sender's
               unique name.
        """
        receivers        = request.get('receivers')
        senderUniqueName = request.get('client_unique_name')
        payload = {
            'action':   'order',
            'order':    request.get('order'),
            'args':     request.get('args'),
            'order_id': request.get('order_id'),
            'sent_at':  request.get('sent_at'),
            'proof':    request.get('proof'),
        }
        for clientUniqueName, clientRecord in list(self._clientsLUT.items()):
            if clientUniqueName == senderUniqueName:
                continue
            if receivers is not None and clientUniqueName not in receivers:
                continue
            await self._write_message(clientRecord['writer'], payload)
        isTarget = receivers is None or self.__uniqueName in receivers
        if isTarget and senderUniqueName != self.__uniqueName:
            self._handle_order(payload)

    def __publish_message(self, message, receivers, timeout, toSelf, replace):
        """Sends a publication via the active connection and returns its UUID.

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
            self._submit(self._write_message(self._writer, payload))
        return pubId

    # ------------------------------------------------------------------
    # Public API — publications
    # ------------------------------------------------------------------
    def get_message(self, message):
        """Returns the publication dict for *message* without removing it.

        :Parameters:
            #. message (str): The publication message key.

        :Returns:
            #. publication (None, dict): The stored publication or ``None``.
        """
        with self.__publicationsLock:
            items = self._live_publications(message, time.time())
            return items[0] if items else None

    def pop_message(self, message):
        """Removes and returns the publication dict for *message*.

        :Parameters:
            #. message (str): The publication message key.

        :Returns:
            #. publication (None, dict): The removed publication or ``None``.
        """
        with self.__publicationsLock:
            items = self._live_publications(message, time.time())
            self._set_publications(message, [])
            return items[0] if items else None

    def has_message(self, message):
        """Returns whether a publication for *message* is currently stored.

        :Parameters:
            #. message (str): The publication message key.

        :Returns:
            #. exists (bool): ``True`` if at least one entry is stored for
               *message*.
        """
        with self.__publicationsLock:
            return len(self._live_publications(message, time.time())) > 0

    def remove_published_message(self, message, senders=None):
        """Removes stored publications for *message*, optionally filtered by sender.

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
            entries = self._live_publications(message, time.time())
            if not entries:
                return True
            if senders is None:
                self._set_publications(message, [])
                return True
            # Filter: keep entries NOT published by the listed senders.
            senderSet = set(senders)
            kept = [entry for entry in entries if entry.get('publisher_unique_name') not in senderSet]
            self._set_publications(message, kept)
            return len(kept) == 0

    def remove_message(self, *args, **kwargs):
        """Alias to ``remove_published_message``."""
        return self.remove_published_message(*args, **kwargs)

    def publish_message(self, message, receivers=None, timeout=None,
                        toSelf=True, unique=False, replace=True):
        """Publishes *message* to one or more connected lockers.

        :Parameters:
            #. message (str): The message string to publish.
            #. receivers (None, list): List of recipient unique names.
               ``None`` means broadcast to all.
            #. timeout (None, float): Optional expiry in seconds.
            #. toSelf (bool): Whether to deliver to this instance.
            #. unique (bool): If ``True``, raise when this instance already
               stores a message with the same key. Messages held by other
               lockers are not checked.
            #. replace (bool): Whether to replace an existing message.

        :Returns:
            #. success (bool): ``True`` if the message was dispatched.
            #. publicationId (str, int): A UUID string identifying this
               publication on success.  ``0`` when the locker is not started.

        :Raises:
            #. TypeError: If message is not a string or receivers is not
               None, a list, a set or a tuple.
            #. ValueError: If timeout is not a positive number, the message is
               longer than ``maxMessageBytes`` allows, or unique is True and
               message already exists.
        """
        if not isinstance(message, str):
            raise TypeError("message must be a string")
        if receivers is not None and not isinstance(receivers, (list, set, tuple)):
            raise TypeError("receivers must be None or a list")
        if receivers is not None and not all(isinstance(name, str) for name in receivers):
            raise TypeError("receivers must hold unique names as strings")
        for flagName, flagValue in (('toSelf', toSelf), ('unique', unique), ('replace', replace)):
            if not isinstance(flagValue, bool):
                raise TypeError(f"{flagName} must be boolean")
        if timeout is not None:
            if not isinstance(timeout, (int, float)) or not timeout > 0:
                raise ValueError("timeout must be a positive number")
        # Room is kept for the fields that are added when the message travels
        longestMessage = self.__maxMessageBytes - min(_MESSAGE_OVERHEAD_BYTES, self.__maxMessageBytes // 2)
        if len(message.encode(_ENCODING)) > longestMessage:
            raise ValueError(f"message must not be longer than {longestMessage} bytes")
        if unique and self.has_message(message):
            raise ValueError(f"Message '{message}' already exists")
        pubId = self.__publish_message(
            message=message, receivers=receivers, timeout=timeout,
            toSelf=toSelf, replace=replace
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
    @staticmethod
    def _order_proof(ordersPassword, orderName, args, orderId, sentAt):
        """Computes the proof that an order comes from someone who knows the orders password.

        :Parameters:
            #. ordersPassword (bytes): The orders password.
            #. orderName (str): The order name, e.g. ``stop``.
            #. args (list): The order arguments.
            #. orderId (str): A random identifier that makes the order unique.
            #. sentAt (float): Epoch time when the order was made.

        :Returns:
            #. proof (str): The proof as hexadecimal text.
        """
        return _make_proof(ordersPassword, 'order', str(orderName), json.dumps(args, sort_keys=True),
                           orderId, repr(sentAt))

    def _order_problem(self, order, ordersPassword):
        """Finds why a received order must not be obeyed.

        :Parameters:
            #. order (dict): The received order message.
            #. ordersPassword (bytes): The orders password of this locker.

        :Returns:
            #. problem (str, None): What is wrong, or None when the order is genuine and new.
        """
        orderId, sentAt, proof = order.get('order_id'), order.get('sent_at'), order.get('proof')
        isNumber = isinstance(sentAt, (int, float)) and not isinstance(sentAt, bool)
        hasValidTypes = isinstance(orderId, str) and isNumber and isinstance(proof, str)
        if not hasValidTypes:
            return 'it carries no valid proof'
        expected = self._order_proof(ordersPassword, order.get('order'), order.get('args'), orderId, sentAt)
        if not _proofs_match(proof, expected):
            return 'it has a wrong password'
        if abs(time.time() - sentAt) > _ORDER_MAX_AGE:
            return 'it is too old or comes from the future'
        if orderId in self._seenOrders:
            return 'it was already received'
        self._seenOrders[orderId] = True
        while len(self._seenOrders) > _MAX_REMEMBERED_ORDERS:
            self._seenOrders.popitem(last=False)
        return None

    def _handle_order(self, order):
        """Validates a received order and runs it on a separate thread.

        :Parameters:
            #. order (dict): The received order message.
        """
        orderName = order.get('order')
        if not self.__allowRemoteOrders['allow']:
            self._warn(f"Received order {_log_text(orderName)} but executing orders is not allowed")
            return
        # None means the orders password follows the locker password, even
        # when the locker password was changed after construction.
        ordersPassword = self.__allowRemoteOrders['password']
        if ordersPassword is None:
            ordersPassword = self.__password
        if ordersPassword is None or ordersPassword is _PASSWORD_NOT_SET:
            self._error(f"Received order {_log_text(orderName)} but this locker has no orders password; ignored")
            return
        problem = self._order_problem(order, ordersPassword)
        if problem is not None:
            self._error(f"Received order {_log_text(orderName)} but {problem}; ignored")
            return
        if orderName != 'stop':
            self._error(f"Received unknown order {_log_text(orderName)}")
            return
        args    = order.get('args') or [False]
        killPid = args[0] is True
        # stop() waits for the event loop thread, so it cannot run on it.
        threading.Thread(target=self._run_stop_order, args=(killPid,),
                         daemon=True, name='pylocker-order').start()

    def _run_stop_order(self, killPid):
        """Stops this locker because a remote stop order was received.

        :Parameters:
            #. killPid (bool): If ``True`` the whole process is killed after
               the locker has stopped and released its locks.
        """
        self._info("Received order to stop from remote")
        self.stop()
        if killPid:
            os.kill(os.getpid(), SIGKILL)

    def stop_remote(self, password, killRemotePID=False,
                    receivers=None, stopSelf=False):
        """Sends a stop order to other locker instances.

        :Parameters:
            #. password (str, bytes): The orders password the receivers expect.
            #. killRemotePID (bool): Whether the receivers also kill their
               own process after stopping.
            #. receivers (None, list): Unique names of the target lockers or
               None for all connected lockers.
            #. stopSelf (bool): Whether to also stop this instance.

        :Raises:
            #. TypeError: If password is not a string or bytes, killRemotePID
               is not a boolean, or receivers is not None, a list, a set or
               a tuple.
            #. ValueError: If password is empty.
        """
        if not isinstance(password, (str, bytes)):
            raise TypeError("password must be a string or bytes")
        if len(password) == 0:
            raise ValueError("password must not be empty")
        if not isinstance(killRemotePID, bool):
            raise TypeError("killRemotePID must be boolean")
        if receivers is not None and not isinstance(receivers, (list, set, tuple)):
            raise TypeError("receivers must be None or a list")
        orderId, sentAt = secrets.token_hex(8), time.time()
        order = {
            'action':             'order',
            'order':              'stop',
            'args':               [killRemotePID],
            'order_id':           orderId,
            'sent_at':            sentAt,
            'proof':              self._order_proof(_to_bytes(password), 'stop', [killRemotePID], orderId, sentAt),
            'receivers':          list(receivers) if receivers else None,
            'client_unique_name': self.__uniqueName,
            'client_name':        self.__name,
        }
        if self.isServer:
            self._run_sync(self._server_order(order), timeout=self.__connectTimeout)
        elif self.isClient:
            self._run_sync(self._write_message(self._writer, order),
                           timeout=self.__connectTimeout)
        else:
            self._warn("stop_remote called but locker is not started")
        if stopSelf:
            self.stop()

    # ------------------------------------------------------------------
    # Core lock API — sync
    # ------------------------------------------------------------------
    def _prepare_acquire(self, path, timeout):
        """Normalises and validates the path and timeout of an acquire call.

        :Parameters:
            #. path (str, list, tuple): One path string or a list of path
               strings.
            #. timeout (None, int, float): Seconds to wait.  None uses the
               instance default timeout.

        :Returns:
            #. paths (list): The normalised paths.
            #. timeout (float): The seconds to wait.

        :Raises:
            #. ValueError: If path is empty or has too many entries, or timeout is not positive.
        """
        if isinstance(path, str):
            path = [path]
        paths = [_normalize_path(lockPath) for lockPath in path]
        if len(paths) == 0:
            raise ValueError("path must not be empty")
        if len(paths) > _MAX_PATHS_PER_REQUEST:
            raise ValueError(f"path must not have more than {_MAX_PATHS_PER_REQUEST} entries")
        if timeout is None:
            timeout = self.__defaultTimeout
        _require_positive_number('timeout', timeout)
        return paths, float(timeout)

    def _acquire_coroutine(self, requestId, paths, timeout):
        """Builds the acquire coroutine matching this instance's role.

        :Parameters:
            #. requestId (str): Unique identifier of the acquire request.
            #. paths (list): Normalised paths to lock.
            #. timeout (float): Seconds to wait.

        :Returns:
            #. coro (coroutine): The server-local or client acquire coroutine.
        """
        # The request time is taken here so queue expiry counts from the
        # moment the caller asked for the lock.
        requestTime = time.time()
        if self.isServer:
            return self._server_local_acquire(requestId, paths, timeout, requestTime)
        return self._client_acquire(requestId, paths, timeout, requestTime)

    def _remember_acquired(self, requestId, paths):
        """Records an acquired lock so it can be released by id later.

        :Parameters:
            #. requestId (str): Unique identifier of the acquire request.
            #. paths (list): The paths that were locked.
        """
        with self.__ownAcquiredLock:
            self.__ownAcquired[requestId] = {
                'request_unique_id': requestId,
                'path': paths,
            }

    def acquire_lock(self, path, timeout=None, retries=3, retryDelay=0.5):
        """Acquires a lock for one or more paths, blocking until acquired or timed out.

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
            #. timeout (None, int, float): Seconds to wait per attempt.
               ``None`` uses the instance default timeout.
            #. retries (int): Number of additional attempts after the first
               one fails with Code 0 (contention timeout).  ``0`` means a
               single attempt with no retry.  The default is ``3``.
               Non-zero values make the total budget
               ``(retries + 1) * timeout`` seconds worst-case.  Retries are NOT performed for error codes
               other than 0 (connection lost, not started, etc.).
            #. retryDelay (int, float): Seconds to wait between attempts.
               ``0`` retries immediately.  The default is ``0.5``.

        :Returns:
            #. success (bool): ``True`` if the lock was acquired.
            #. lockUniqueId (str, int): The lock UUID on success.  On failure,
               an integer error code: ``0`` timed out, ``1`` connection lost,
               ``2`` locker not started, or a string describing the error.

        :Raises:
            #. ValueError: If path is empty, or timeout, retries or
               retryDelay is out of range.
        """
        path, timeout = self._prepare_acquire(path, timeout)
        if not isinstance(retries, int) or not retries >= 0:
            raise ValueError("retries must be a non-negative integer")
        if not isinstance(retryDelay, (int, float)) or not retryDelay >= 0:
            raise ValueError("retryDelay must be a non-negative number")
        if not (self.isServer or self.isClient):
            return False, 2
        for attempt in range(retries + 1):
            if attempt > 0 and retryDelay > 0:
                time.sleep(retryDelay)
            requestId = str(uuid.uuid4())
            try:
                coro = self._acquire_coroutine(requestId, path, timeout)
                acquired, lockId = self._submit(coro).result(
                    timeout=timeout + _RESULT_SLACK
                )
                if acquired:
                    self._remember_acquired(requestId, path)
                    return True, lockId
                # Code 0 — contention; retry if attempts remain.
            except concurrent.futures.TimeoutError:
                # concurrent.futures.Future.result(timeout=...) fired before
                # the asyncio coroutine could respond.  Treat identically to
                # a Code-0 contention timeout — retry if budget remains.
                pass
            except Exception as error:
                # Any non-timeout error (connection lost, serialiser error,
                # etc.) is not retriable — return immediately.
                return False, _to_error_code(error)
        return False, 0

    def acquire(self, *args, **kwargs):
        """Alias to ``acquire_lock``."""
        return self.acquire_lock(*args, **kwargs)

    def release_lock(self, lockId):
        """Releases a previously acquired lock.

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
            request = self.__ownAcquired.pop(lockId, None)
        if request is None:
            return True, 0
        paths = request.get('path', [])
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
        except Exception as error:
            return False, str(error)

    def release(self, *args, **kwargs):
        """Alias to ``release_lock``."""
        return self.release_lock(*args, **kwargs)

    # ------------------------------------------------------------------
    # Core lock API — async opt-in
    # ------------------------------------------------------------------
    async def acquire_async(self, path, timeout=None):
        """Acquires a lock without blocking the caller's event loop.

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

        :Returns:
            #. success (bool): ``True`` if the lock was acquired.
            #. lockUniqueId (str, int): Lock UUID on success or error code.

        :Raises:
            #. ValueError: If path is empty or timeout is not positive.
        """
        path, timeout = self._prepare_acquire(path, timeout)
        if not (self.isServer or self.isClient):
            return False, 2
        requestId = str(uuid.uuid4())
        try:
            coro = self._acquire_coroutine(requestId, path, timeout)
            future = self._submit(coro)
            acquired, lockId = await asyncio.wrap_future(future)
            if acquired:
                self._remember_acquired(requestId, path)
            return acquired, lockId
        except Exception as error:
            return False, _to_error_code(error)

    async def release_async(self, lockId):
        """Releases a lock without blocking the caller's event loop.

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
            request = self.__ownAcquired.pop(lockId, None)
        if request is None:
            return True, 0
        paths = request.get('path', [])
        try:
            if self.isServer:
                future = self._submit(self._server_release({
                    'request_unique_id': lockId, 'path': paths,
                }))
            elif self.isClient:
                future = self._submit(self._client_release(lockId, paths))
            else:
                return False, 3
            await asyncio.wrap_future(future)
            return True, 1
        except Exception as error:
            return False, str(error)

    # ------------------------------------------------------------------
    # Context manager (sync + async)
    # ------------------------------------------------------------------
    def lock(self, path, timeout=None):
        """Returns a context manager that acquires and releases *path*.

        Supports both ``with`` (synchronous) and ``async with`` (non-blocking)
        usage.

        :Parameters:
            #. path (str, list, tuple): Path or list of paths to lock.
            #. timeout (None, int, float): Seconds to wait.

        :Returns:
            #. context (_LockContext): Context manager for the lock.
        """
        return _LockContext(self, path, timeout)


class SingleLocker(ServerLocker):
    """
    Represents the singleton form of ServerLocker. Creating a single locker
    per process is recommended.
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

