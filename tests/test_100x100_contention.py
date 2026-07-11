"""
Side-by-side comparison: 100 clients x 100 rounds on one single shared lock.

This is the maximum-contention sustained-load test.  Every client tries to
acquire exactly the same path ('competition/single-file') 100 times in a row
without any pause between rounds.  The lock queue never drains — at any moment
roughly 99 clients are waiting for the one client that holds the lock.

Seven execution variants are compared in one shared results table:

  NEW-SYNC   -- Locker.py, synchronous acquire_lock / release_lock.
                Each client runs in its own OS thread.  The thread submits a
                coroutine to a background asyncio event loop via
                run_coroutine_threadsafe and blocks on the resulting Future.

  NEW-ASYNC  -- Locker.py, acquire_async / release_async called from within a
                single asyncio.run() event loop via asyncio.gather.
                The coroutine is still bridged to a PER-CLIENT background loop
                through asyncio.wrap_future(run_coroutine_threadsafe(...)), so
                the self-pipe wakeup cost still exists.

  RAW-ASYNC  -- 100 raw TCP coroutines sharing ONE asyncio event loop with NO
                ServerLocker client wrapper.  Each coroutine opens its own TCP
                connection, completes the handshake, and sends acquire / release
                JSON messages directly.  No background event loop, no self-pipe,
                no concurrent.futures.Future bridging.  This is what 'native
                async' actually means and proves the hypothesis stated in the
                analysis comments.

  MP-SYNC    -- Locker.py, synchronous acquire_lock / release_lock, msgpack clients.
                Same bridge pattern as NEW-SYNC but the wire bytes are msgpack.  Shows
                whether the serialiser choice changes sync latency at all.

  MP-ASYNC   -- Locker.py, acquire_async / release_async, msgpack clients.  Same
                bridge as NEW-ASYNC but with binary framing.

  MP-RAW     -- 100 raw TCP coroutines, ONE event loop, msgpack binary framing, NO
                wrapper at all.  The theoretical maximum throughput of this server.

  OLD-SYNC   -- ServerLocker.py (legacy), synchronous acquire_lock / release_lock.
                One OS thread per client.  Direct socket write + blocking recv.
                One OS thread per client on the server side too.

Wire protocol (Locker.py, for the RAW-ASYNC implementation):
  All messages are newline-terminated UTF-8 JSON.

  Handshake client->server:
    {"name": <str>, "unique_name": <str>, "password": <str>,
     "pid": <int>, "address": <str>}

  Server welcome response:
    {"action": "welcome", ...}

  Acquire request client->server:
    {"request_unique_id": <str>, "action": "acquire",
     "path": [<str>, ...], "timeout": <float>,
     "request_utctime": <float>,
     "client_unique_name": <str>, "client_name": <str>}

  Server acquire response:
    {"action": "acquired", "request_unique_id": <str>}

  Release request client->server:
    {"request_unique_id": <str>, "action": "release",
     "path": [<str>, ...],
     "client_unique_name": <str>, "client_name": <str>}

Correctness:
  A background monitor polls each server's lockedPaths every 20 ms throughout
  the run.  Any path simultaneously held by more than one lock UUID is a
  mutual-exclusion violation and causes the test to exit non-zero.

Usage:
  python tests/test_100x100_contention.py
"""

import asyncio
import collections
import json
import os
import resource
import statistics
import sys
import tempfile
import threading
import time
import uuid
import struct
try:
    import msgpack as _msgpack
except ImportError:
    _msgpack = None

# ---------------------------------------------------------------------------
# Raise the file-descriptor limit before any imports open sockets.
# ---------------------------------------------------------------------------
_ORIGINAL_SOFT, _HARD_LIMIT = resource.getrlimit(resource.RLIMIT_NOFILE)
resource.setrlimit(resource.RLIMIT_NOFILE, (4096, _HARD_LIMIT))

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import Locker as _new_module                                           # noqa: E402
import ServerLocker as _old_module                                     # noqa: E402
NewLocker = _new_module.ServerLocker
OldLocker = _old_module.ServerLocker

# ---------------------------------------------------------------------------
# Test parameters
# ---------------------------------------------------------------------------
PASSWORD        = 'stress-test-secret'
N_CLIENTS       = 100
N_ROUNDS        = 500
LOCK_PATH       = 'competition/single-file'
ACQUIRE_TIMEOUT = 120
MAX_LOCK_TIME   = 600
PROGRESS_EVERY  = 5.0
_ENCODING       = 'utf-8'

# ---------------------------------------------------------------------------
# Table formatting — IMPL column prepended
# ---------------------------------------------------------------------------
_COL_W = (12, 10, 7, 7, 7, 7, 7, 7, 8)
_HDRS  = ('IMPL', 'METHOD', 'OPS', 'OPS/S', 'p50ms', 'p95ms', 'p99ms', 'MAXms', 'LUT')
_SEP   = '=' * 80
_THIN  = '-' * 80


def _row_fmt(*values):
    """Format one results row using the fixed column widths."""
    parts = []
    for val, width in zip(values, _COL_W):
        parts.append(str(val).rjust(width))
    return '  ' + '  '.join(parts)


def print_table_header():
    """Print the results table banner and column headings."""
    print(_SEP)
    print(_row_fmt(*_HDRS))
    print(_THIN)


def print_table_row(impl_label, method_label, total_ops,
                    elapsed, latencies, lut_clean):
    """Print one data row in the shared results table."""
    throughput = total_ops / elapsed if elapsed > 0 else 0.0
    lats = sorted(latencies)
    n = len(lats)
    if n:
        p50   = statistics.median(lats) * 1000
        p95   = lats[min(int(n * 0.95), n - 1)] * 1000
        p99   = lats[min(int(n * 0.99), n - 1)] * 1000
        maxms = lats[-1] * 1000
    else:
        p50 = p95 = p99 = maxms = float('nan')

    lut_status = 'clean' if lut_clean else 'LEAKED'
    print(_row_fmt(
        impl_label,
        method_label,
        total_ops,
        '%.0f' % throughput,
        '%.1f' % p50,
        '%.1f' % p95,
        '%.1f' % p99,
        '%.1f' % maxms,
        lut_status,
    ))


# ---------------------------------------------------------------------------
# Correctness monitor — works for both old and new server instances
# ---------------------------------------------------------------------------
class CorrectnessMonitor:
    """Background thread that polls a server's lockedPaths every 20 ms.

    Handles both the new Locker.py (string keys) and the legacy
    ServerLocker.py (bytes keys) by normalising all keys to str.

    .. code-block:: python

        monitor = CorrectnessMonitor(server)
        # ... run workload ...
        violations = monitor.stop()
    """

    def __init__(self, server):
        """Initialise the monitor and start the background polling thread."""
        self._server     = server
        self._stop_event = threading.Event()
        self._violations = 0
        self._mutex      = threading.Lock()
        self._seen       = set()
        self._thread     = threading.Thread(target=self._run, daemon=True,
                                            name='CorrectnessMonitor')
        self._thread.start()

    def stop(self):
        """Stop polling and return the total violation count.

        :Returns:
            #. violationCount (int): Number of unique mutual-exclusion
               violations detected during the monitored interval.
        """
        self._stop_event.set()
        self._thread.join(timeout=1)
        return self._violations

    def _run(self):
        """Poll lockedPaths and flag any path held by more than one lock UUID."""
        while not self._stop_event.is_set():
            locked = self._server.lockedPaths or {}
            buckets = collections.defaultdict(list)
            for path, rec in locked.items():
                path_key = path.decode('utf-8', 'replace') if isinstance(path, bytes) else path
                lock_id  = rec.get('request_unique_id') or rec.get('lock_unique_id', '?')
                buckets[path_key].append(lock_id)
            for path, ids in buckets.items():
                if len(ids) > 1:
                    key = (path, tuple(sorted(ids)))
                    if key not in self._seen:
                        self._seen.add(key)
                        with self._mutex:
                            self._violations += 1
                        print(
                            "\n  !! VIOLATION: '%s' held by %d IDs simultaneously"
                            % (path, len(ids))
                        )
            time.sleep(0.02)


# ---------------------------------------------------------------------------
# New Locker.py — server + client lifecycle helpers
# ---------------------------------------------------------------------------
def setup_new(tmp_path):
    """Start a new Locker.py server and connect N_CLIENTS wrapper clients.

    :Parameters:
        #. tmp_path (str): Fingerprint file path for server election.

    :Returns:
        #. server (NewLocker): The running server instance.
        #. clients (list): N_CLIENTS connected NewLocker client instances.
    """
    server = NewLocker(
        password=PASSWORD, serverFile=tmp_path, logger=False,
        autoconnect=True, defaultTimeout=ACQUIRE_TIMEOUT,
        maxLockTime=MAX_LOCK_TIME,
    )
    time.sleep(0.4)
    assert server.isServer, "New Locker.py server failed to start"
    print("  [new]    server up on 127.0.0.1:%d" % server._serverPort)

    clients = []
    for i in range(N_CLIENTS):
        cl = NewLocker(
            password=PASSWORD, serverFile=None, logger=False,
            autoconnect=False, defaultTimeout=ACQUIRE_TIMEOUT,
        )
        cl.start(address='127.0.0.1', port=server._serverPort)
        clients.append(cl)
        if (i + 1) % 20 == 0:
            alive = sum(1 for c in clients if c.isClient)
            print("  [new]    %3d/%d connected (%d live)" % (i + 1, N_CLIENTS, alive))
            time.sleep(0.05)

    connected = sum(1 for c in clients if c.isClient)
    assert connected == N_CLIENTS, "Only %d/%d new clients connected" % (connected, N_CLIENTS)
    print("  [new]    %d/%d wrapper clients ready\n" % (connected, N_CLIENTS))
    return server, clients


def teardown_new(server, clients, tmp_path):
    """Stop all new clients and the server.

    :Parameters:
        #. server (NewLocker): The server to stop.
        #. clients (list): Client instances to stop.
        #. tmp_path (str): Fingerprint file to remove.
    """
    for cl in clients:
        cl.stop()
    time.sleep(0.3)
    server.stop()
    try:
        os.remove(tmp_path)
    except OSError:
        pass


# ---------------------------------------------------------------------------
# Legacy ServerLocker.py — server + client lifecycle helpers
# ---------------------------------------------------------------------------
def setup_old(tmp_path):
    """Start a legacy ServerLocker.py server and connect N_CLIENTS clients.

    :Parameters:
        #. tmp_path (str): Fingerprint file path for server election.

    :Returns:
        #. server (OldLocker): The running legacy server instance.
        #. clients (list): N_CLIENTS connected OldLocker client instances.
    """
    server = OldLocker(
        password=PASSWORD, serverFile=tmp_path, logger=False,
        autoconnect=True, defaultTimeout=ACQUIRE_TIMEOUT,
        maxLockTime=MAX_LOCK_TIME,
    )
    time.sleep(0.8)
    assert server.isServer, "Legacy ServerLocker.py server failed to start"
    print("  [legacy] server up on %s:%d" % (server.address, server.port))

    srv_address = server.address
    srv_port    = server.port

    clients = []
    for i in range(N_CLIENTS):
        cl = OldLocker(
            password=PASSWORD, serverFile=False, logger=False,
            autoconnect=False, defaultTimeout=ACQUIRE_TIMEOUT,
        )
        cl.connect(address=srv_address, port=srv_port)
        clients.append(cl)
        if (i + 1) % 20 == 0:
            alive = sum(1 for c in clients if c.isClient)
            print("  [legacy] %3d/%d connected (%d live)" % (i + 1, N_CLIENTS, alive))
            time.sleep(0.1)

    connected = sum(1 for c in clients if c.isClient)
    assert connected == N_CLIENTS, "Only %d/%d legacy clients connected" % (connected, N_CLIENTS)
    print("  [legacy] %d/%d clients ready\n" % (connected, N_CLIENTS))
    return server, clients


def teardown_old(server, clients, tmp_path):
    """Stop all legacy clients and the server.

    :Parameters:
        #. server (OldLocker): The legacy server to stop.
        #. clients (list): Legacy client instances to stop.
        #. tmp_path (str): Fingerprint file to remove.
    """
    for cl in clients:
        try:
            cl.stop()
        except Exception:
            pass
    time.sleep(0.5)
    try:
        server.stop()
    except Exception:
        pass
    try:
        os.remove(tmp_path)
    except OSError:
        pass


# ---------------------------------------------------------------------------
# Synchronous runner — used identically for both new and legacy clients
# ---------------------------------------------------------------------------
def run_sync(server, clients, acquire_fn, release_fn, label):
    """Run N_CLIENTS threads each doing N_ROUNDS acquire-release cycles.

    All threads compete for LOCK_PATH simultaneously.

    :Parameters:
        #. server: Server instance for the LUT health check after the run.
        #. clients (list): Connected client instances.
        #. acquire_fn (callable): Signature ``(client, path, timeout) -> (bool, id)``.
        #. release_fn (callable): Signature ``(client, lock_id) -> None``.
        #. label (str): Short string printed in progress lines.

    :Returns:
        #. totalOps (int): Total successful acquire-release cycles completed.
        #. elapsed (float): Wall-clock seconds.
        #. latencies (list): Per-operation times in seconds.
        #. errorCount (int): Number of failed acquire calls.
        #. lutClean (bool): True if lockedPaths is empty after the run.
    """
    per_client_lats = {}
    errors          = []
    total_expected  = N_CLIENTS * N_ROUNDS

    def worker(idx, cl):
        lats = []
        for _ in range(N_ROUNDS):
            t0 = time.perf_counter()
            ok, lock_id = acquire_fn(cl, LOCK_PATH, ACQUIRE_TIMEOUT)
            if not ok:
                errors.append("%s client-%d acquire failed (code=%s)" % (label, idx, lock_id))
                continue
            release_fn(cl, lock_id)
            lats.append(time.perf_counter() - t0)
        per_client_lats[idx] = lats

    threads = [
        threading.Thread(target=worker, args=(i, cl), daemon=True)
        for i, cl in enumerate(clients)
    ]

    t_start  = time.perf_counter()
    for t in threads:
        t.start()

    remaining = list(threads)
    deadline  = t_start + 300.0
    next_tick = t_start + PROGRESS_EVERY
    while remaining and time.perf_counter() < deadline:
        batch_end = min(time.perf_counter() + 0.1, next_tick)
        still_alive = []
        for t in remaining:
            wait = max(0.0, batch_end - time.perf_counter())
            t.join(timeout=wait)
            if t.is_alive():
                still_alive.append(t)
        remaining = still_alive
        if remaining and time.perf_counter() >= next_tick:
            done = sum(len(v) for v in per_client_lats.values())
            print("  [%s] SYNC  %.1fs  %d/%d ops  %d threads active" % (
                label, time.perf_counter() - t_start,
                done, total_expected, len(remaining),
            ))
            next_tick = time.perf_counter() + PROGRESS_EVERY

    elapsed  = time.perf_counter() - t_start
    all_lats = [lat for lats in per_client_lats.values() for lat in lats]

    time.sleep(0.5)
    lut_clean = not bool(server.lockedPaths)
    return len(all_lats), elapsed, all_lats, len(errors), lut_clean


# ---------------------------------------------------------------------------
# NEW-ASYNC runner — acquire_async bridged through per-client background loop
# ---------------------------------------------------------------------------
def run_async_wrapped(server, clients, label):
    """Run N_CLIENTS coroutines via acquire_async / release_async.

    All coroutines share one asyncio.run() event loop, but each call to
    acquire_async still bridges to a PER-CLIENT background loop through
    asyncio.wrap_future(run_coroutine_threadsafe(...)).  The self-pipe
    wakeup overhead therefore still exists even though the caller is async.

    :Parameters:
        #. server (NewLocker): The running server (LUT health check).
        #. clients (list): Connected NewLocker wrapper client instances.
        #. label (str): Short string printed in progress lines.

    :Returns:
        #. totalOps (int): Total successful acquire-release cycles completed.
        #. elapsed (float): Wall-clock seconds.
        #. latencies (list): Per-operation times in seconds.
        #. errorCount (int): Number of failed acquire calls.
        #. lutClean (bool): True if lockedPaths is empty after the run.
    """
    per_client_lats = {}
    errors          = []
    total_expected  = N_CLIENTS * N_ROUNDS

    async def worker(idx, cl):
        # In an async application this is the natural call site.
        # acquire_async wraps the coroutine into the client's PRIVATE
        # background loop, so one self-pipe write still happens per acquire.
        lats = []
        for _ in range(N_ROUNDS):
            t0 = time.perf_counter()
            ok, lock_id = await cl.acquire_async(LOCK_PATH, timeout=ACQUIRE_TIMEOUT)
            if not ok:
                errors.append("%s client-%d acquire failed (code=%s)" % (label, idx, lock_id))
                continue
            await cl.release_async(lock_id)
            lats.append(time.perf_counter() - t0)
        per_client_lats[idx] = lats

    async def progress_watcher(t_start):
        """Print periodic progress until all async clients finish."""
        while True:
            await asyncio.sleep(PROGRESS_EVERY)
            done   = sum(len(v) for v in per_client_lats.values())
            active = N_CLIENTS - sum(
                1 for v in per_client_lats.values() if len(v) >= N_ROUNDS
            )
            print("  [%s] ASYNC %.1fs  %d/%d ops  %d tasks active" % (
                label, time.perf_counter() - t_start, done, total_expected, active,
            ))
            if done >= total_expected:
                break

    async def run_all():
        t_start = time.perf_counter()
        watcher = asyncio.create_task(progress_watcher(t_start))
        workers = [asyncio.create_task(worker(i, cl)) for i, cl in enumerate(clients)]
        await asyncio.gather(*workers)
        watcher.cancel()
        try:
            await watcher
        except asyncio.CancelledError:
            pass

    t_start = time.perf_counter()
    asyncio.run(run_all())
    elapsed = time.perf_counter() - t_start

    all_lats = [lat for lats in per_client_lats.values() for lat in lats]
    time.sleep(0.5)
    lut_clean = not bool(server.lockedPaths)
    return len(all_lats), elapsed, all_lats, len(errors), lut_clean


# ---------------------------------------------------------------------------
# RAW-ASYNC runner — pure coroutines, one shared event loop, zero bridging
# ---------------------------------------------------------------------------
def run_async_raw(server_port, server, label):
    """Run N_CLIENTS raw TCP coroutines natively in one shared asyncio event loop.

    This variant bypasses the ServerLocker client class entirely.  Each
    coroutine opens its own TCP connection to the server, completes the
    JSON handshake, and then sends acquire / release messages directly.

    There is NO background event loop, NO run_coroutine_threadsafe call,
    NO concurrent.futures.Future bridging, and NO self-pipe wakeup.
    All 100 coroutines are scheduled cooperatively by a single asyncio
    event loop — the same one that calls this function via asyncio.run().

    This is the 'native async' scenario: the cost is exactly one TCP round
    trip (write + readline) per acquire and one for the release.  No extra
    kernel crossings.

    :Parameters:
        #. server_port (int): TCP port the Locker.py server is listening on.
        #. server (NewLocker): Server instance for the LUT health check.
        #. label (str): Short string printed in progress lines.

    :Returns:
        #. totalOps (int): Total successful acquire-release cycles completed.
        #. elapsed (float): Wall-clock seconds.
        #. latencies (list): Per-operation times in seconds.
        #. errorCount (int): Number of failed acquire calls.
        #. lutClean (bool): True if lockedPaths is empty after the run.
    """
    per_client_lats = {}
    errors          = []
    total_expected  = N_CLIENTS * N_ROUNDS
    # path is sent as a list of strings, matching what _client_acquire sends
    lock_paths      = [os.path.normpath(LOCK_PATH).replace('\\', '/')]

    async def raw_client(idx):
        """Open one TCP connection and run N_ROUNDS acquire-release cycles."""
        client_name        = 'raw-client-%d' % idx
        client_unique_name = str(uuid.uuid4())
        lats               = []

        try:
            reader, writer = await asyncio.open_connection('127.0.0.1', server_port)
        except Exception as err:
            errors.append("raw client-%d connect failed: %s" % (idx, err))
            per_client_lats[idx] = lats
            return

        # ── Handshake ────────────────────────────────────────────────────
        hello = json.dumps({
            'name':        client_name,
            'unique_name': client_unique_name,
            'password':    PASSWORD,
            'pid':         os.getpid(),
            'address':     '127.0.0.1',
        }).encode(_ENCODING) + b'\n'
        writer.write(hello)
        await writer.drain()

        welcome_raw = await asyncio.wait_for(reader.readline(), timeout=10.0)
        welcome = json.loads(welcome_raw.decode(_ENCODING))
        if welcome.get('action') != 'welcome':
            errors.append(
                "raw client-%d: expected 'welcome', got '%s'"
                % (idx, welcome.get('action'))
            )
            writer.close()
            per_client_lats[idx] = lats
            return

        # ── Acquire / release loop ────────────────────────────────────────
        # Each iteration is two JSON messages and zero extra syscalls beyond
        # the TCP send and recv.  No self-pipe, no thread wakeup, no Future.
        for _ in range(N_ROUNDS):
            ruuid   = str(uuid.uuid4())
            utcTime = time.time()

            t0 = time.perf_counter()

            # Acquire request
            acquire_msg = json.dumps({
                'request_unique_id':  ruuid,
                'action':             'acquire',
                'path':               lock_paths,
                'timeout':            float(ACQUIRE_TIMEOUT),
                'request_utctime':    utcTime,
                'client_unique_name': client_unique_name,
                'client_name':        client_name,
            }).encode(_ENCODING) + b'\n'
            writer.write(acquire_msg)
            await writer.drain()

            # Wait for the server's 'acquired' notification.
            # Under max contention this wait can be up to ~N_CLIENTS * per-lock-time.
            try:
                resp_raw = await asyncio.wait_for(
                    reader.readline(), timeout=float(ACQUIRE_TIMEOUT)
                )
            except asyncio.TimeoutError:
                errors.append("raw client-%d acquire timed out after %ds" % (idx, ACQUIRE_TIMEOUT))
                break
            if not resp_raw:
                errors.append("raw client-%d: server closed connection" % idx)
                break

            resp = json.loads(resp_raw.decode(_ENCODING))
            if resp.get('action') != 'acquired':
                errors.append(
                    "raw client-%d: expected 'acquired', got '%s'"
                    % (idx, resp.get('action'))
                )
                break

            # Release
            release_msg = json.dumps({
                'request_unique_id':  ruuid,
                'action':             'release',
                'path':               lock_paths,
                'client_unique_name': client_unique_name,
                'client_name':        client_name,
            }).encode(_ENCODING) + b'\n'
            writer.write(release_msg)
            await writer.drain()

            lats.append(time.perf_counter() - t0)

        # ── Clean disconnect ──────────────────────────────────────────────
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass
        per_client_lats[idx] = lats

    async def progress_watcher(t_start):
        """Print periodic progress until all raw coroutines finish."""
        while True:
            await asyncio.sleep(PROGRESS_EVERY)
            done   = sum(len(v) for v in per_client_lats.values())
            active = N_CLIENTS - len(per_client_lats)
            print("  [%s] RAW-ASYNC %.1fs  %d/%d ops  %d coroutines active" % (
                label, time.perf_counter() - t_start, done, total_expected, active,
            ))
            if len(per_client_lats) >= N_CLIENTS:
                break

    async def run_all():
        t_start = time.perf_counter()
        watcher = asyncio.create_task(progress_watcher(t_start))
        # All 100 coroutines share THIS event loop — no background loops.
        # asyncio.gather schedules them cooperatively on the same thread.
        workers = [asyncio.create_task(raw_client(i)) for i in range(N_CLIENTS)]
        await asyncio.gather(*workers)
        watcher.cancel()
        try:
            await watcher
        except asyncio.CancelledError:
            pass

    t_start = time.perf_counter()
    asyncio.run(run_all())
    elapsed = time.perf_counter() - t_start

    all_lats = [lat for lats in per_client_lats.values() for lat in lats]
    time.sleep(0.5)
    lut_clean = not bool(server.lockedPaths)
    return len(all_lats), elapsed, all_lats, len(errors), lut_clean



# ---------------------------------------------------------------------------
# NEW-MSGPACK runner — raw coroutines + msgpack binary framing
# ---------------------------------------------------------------------------
def setup_new_server_only(tmp_path):
    """Start a new Locker.py server configured for msgpack framing.

    No wrapper clients are created — the msgpack raw test opens its own
    direct TCP connections using raw msgpack-framed coroutines.

    :Parameters:
        #. tmp_path (str): Fingerprint file path for server election.

    :Returns:
        #. server (NewLocker): The running msgpack server instance.
    """
    server = NewLocker(
        password=PASSWORD, serverFile=tmp_path, logger=False,
        autoconnect=True, defaultTimeout=ACQUIRE_TIMEOUT,
        maxLockTime=MAX_LOCK_TIME, serializer='msgpack',
    )
    import time as _time
    _time.sleep(0.4)
    assert server.isServer, "Msgpack Locker.py server failed to start"
    print("  [msgpack] server up on 127.0.0.1:%d" % server._serverPort)
    return server


def run_async_raw_msgpack(server_port, server, label):
    """Run N_CLIENTS raw TCP coroutines using msgpack binary framing.

    Identical architecture to run_async_raw but every message is encoded
    with msgpack and framed with a 4-byte big-endian length prefix instead
    of newline-terminated JSON.  This isolates the pure serialiser speed
    gain from all other factors.

    :Parameters:
        #. server_port (int): TCP port the msgpack Locker.py server listens on.
        #. server (NewLocker): Server instance for the LUT health check.
        #. label (str): Short string printed in progress lines.

    :Returns:
        #. totalOps (int): Total successful acquire-release cycles completed.
        #. elapsed (float): Wall-clock seconds.
        #. latencies (list): Per-operation times in seconds.
        #. errorCount (int): Number of failed acquire calls.
        #. lutClean (bool): True if lockedPaths is empty after the run.
    """
    if _msgpack is None:
        print("  SKIP: msgpack not installed")
        return 0, 1.0, [], 0, True

    per_client_lats = {}
    errors          = []
    total_expected  = N_CLIENTS * N_ROUNDS
    lock_paths      = [os.path.normpath(LOCK_PATH).replace('\\', '/')]

    async def write_mp(writer, obj):
        """Pack obj with msgpack and send it with a 4-byte length prefix."""
        payload = _msgpack.packb(obj, use_bin_type=True)
        writer.write(struct.pack('>I', len(payload)) + payload)
        await writer.drain()

    async def read_mp(reader, timeout=None):
        """Read one 4-byte-prefixed msgpack frame and unpack it."""
        if timeout is not None:
            header = await asyncio.wait_for(reader.readexactly(4), timeout=timeout)
        else:
            header = await reader.readexactly(4)
        length  = struct.unpack('>I', header)[0]
        payload = await reader.readexactly(length)
        return _msgpack.unpackb(payload, raw=False)

    async def raw_msgpack_client(idx):
        """Open one msgpack TCP connection and run N_ROUNDS acquire-release cycles."""
        clientName        = 'mp-client-%d' % idx
        clientUniqueName  = str(uuid.uuid4())
        lats              = []

        try:
            reader, writer = await asyncio.open_connection('127.0.0.1', server_port)
        except Exception as err:
            errors.append("mp client-%d connect failed: %s" % (idx, err))
            per_client_lats[idx] = lats
            return

        # ── Handshake ─────────────────────────────────────────────────
        await write_mp(writer, {
            'name':        clientName,
            'unique_name': clientUniqueName,
            'password':    PASSWORD,
            'pid':         os.getpid(),
            'address':     '127.0.0.1',
        })
        try:
            welcome = await read_mp(reader, timeout=10.0)
        except Exception as err:
            errors.append("mp client-%d handshake failed: %s" % (idx, err))
            writer.close()
            per_client_lats[idx] = lats
            return

        if welcome.get('action') != 'welcome':
            errors.append(
                "mp client-%d: expected 'welcome', got '%s'"
                % (idx, welcome.get('action'))
            )
            writer.close()
            per_client_lats[idx] = lats
            return

        # ── Acquire / release loop ─────────────────────────────────────
        for _ in range(N_ROUNDS):
            ruuid   = str(uuid.uuid4())
            utcTime = time.time()

            t0 = time.perf_counter()

            await write_mp(writer, {
                'request_unique_id':  ruuid,
                'action':             'acquire',
                'path':               lock_paths,
                'timeout':            float(ACQUIRE_TIMEOUT),
                'request_utctime':    utcTime,
                'client_unique_name': clientUniqueName,
                'client_name':        clientName,
            })

            try:
                resp = await asyncio.wait_for(
                    read_mp(reader), timeout=float(ACQUIRE_TIMEOUT)
                )
            except asyncio.TimeoutError:
                errors.append("mp client-%d acquire timed out" % idx)
                break
            except Exception as err:
                errors.append("mp client-%d read error: %s" % (idx, err))
                break

            if resp.get('action') != 'acquired':
                errors.append(
                    "mp client-%d: expected 'acquired', got '%s'"
                    % (idx, resp.get('action'))
                )
                break

            await write_mp(writer, {
                'request_unique_id':  ruuid,
                'action':             'release',
                'path':               lock_paths,
                'client_unique_name': clientUniqueName,
                'client_name':        clientName,
            })

            lats.append(time.perf_counter() - t0)

        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass
        per_client_lats[idx] = lats

    async def progress_watcher(t_start):
        """Print periodic progress until all msgpack coroutines finish."""
        while True:
            await asyncio.sleep(PROGRESS_EVERY)
            done   = sum(len(v) for v in per_client_lats.values())
            active = N_CLIENTS - len(per_client_lats)
            print("  [%s] MSGPACK %.1fs  %d/%d ops  %d coroutines active" % (
                label, time.perf_counter() - t_start, done, total_expected, active,
            ))
            if len(per_client_lats) >= N_CLIENTS:
                break

    async def run_all():
        t_start = time.perf_counter()
        watcher = asyncio.create_task(progress_watcher(t_start))
        workers = [asyncio.create_task(raw_msgpack_client(i)) for i in range(N_CLIENTS)]
        await asyncio.gather(*workers)
        watcher.cancel()
        try:
            await watcher
        except asyncio.CancelledError:
            pass

    t_start = time.perf_counter()
    asyncio.run(run_all())
    elapsed = time.perf_counter() - t_start

    all_lats = [lat for lats in per_client_lats.values() for lat in lats]
    time.sleep(0.5)
    lut_clean = not bool(server.lockedPaths)
    return len(all_lats), elapsed, all_lats, len(errors), lut_clean




# ---------------------------------------------------------------------------
# Msgpack Locker.py — full server + wrapper client lifecycle helpers
# ---------------------------------------------------------------------------
def setup_new_mp(tmp_path):
    """Start a msgpack Locker.py server and connect N_CLIENTS msgpack wrapper clients.

    :Parameters:
        #. tmp_path (str): Fingerprint file path for server election.

    :Returns:
        #. server (NewLocker): The running msgpack server instance.
        #. clients (list): N_CLIENTS connected msgpack NewLocker client instances.
    """
    server = NewLocker(
        password=PASSWORD, serverFile=tmp_path, logger=False,
        autoconnect=True, defaultTimeout=ACQUIRE_TIMEOUT,
        maxLockTime=MAX_LOCK_TIME, serializer='msgpack',
    )
    time.sleep(0.4)
    assert server.isServer, "Msgpack Locker.py server failed to start"
    print("  [mp]     server up on 127.0.0.1:%d" % server._serverPort)

    clients = []
    for i in range(N_CLIENTS):
        cl = NewLocker(
            password=PASSWORD, serverFile=None, logger=False,
            autoconnect=False, defaultTimeout=ACQUIRE_TIMEOUT,
            serializer='msgpack',
        )
        cl.start(address='127.0.0.1', port=server._serverPort)
        clients.append(cl)
        if (i + 1) % 20 == 0:
            alive = sum(1 for c in clients if c.isClient)
            print("  [mp]     %3d/%d connected (%d live)" % (i + 1, N_CLIENTS, alive))
            time.sleep(0.05)

    connected = sum(1 for c in clients if c.isClient)
    assert connected == N_CLIENTS, \
        "Only %d/%d msgpack clients connected" % (connected, N_CLIENTS)
    print("  [mp]     %d/%d msgpack wrapper clients ready\n" % (connected, N_CLIENTS))
    return server, clients

# ---------------------------------------------------------------------------
# Acquire / release shims
# ---------------------------------------------------------------------------
def _new_acquire(cl, path, timeout):
    """Call acquire_lock on a new Locker.py wrapper client."""
    return cl.acquire_lock(path, timeout=timeout)


def _new_release(cl, lock_id):
    """Call release_lock on a new Locker.py wrapper client."""
    cl.release_lock(lock_id)


def _old_acquire(cl, path, timeout):
    """Call acquire_lock on a legacy ServerLocker.py client."""
    return cl.acquire_lock(path, timeout=timeout)


def _old_release(cl, lock_id):
    """Call release_lock on a legacy ServerLocker.py client."""
    cl.release_lock(lock_id)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    """Run the NxN single-lock contention test across all seven variants."""
    print()
    print(_SEP)
    print("  %d clients x %d rounds -- single shared lock -- seven variants"
          % (N_CLIENTS, N_ROUNDS))
    print("  Lock path : '%s'" % LOCK_PATH)
    print("  fd limit  : %d -> 4096" % _ORIGINAL_SOFT)
    print()
    print("  NEW  SYNC     : Locker.py JSON wrapper, OS thread, run_coroutine_threadsafe bridge")
    print("  NEW  ASYNC    : Locker.py JSON wrapper, asyncio.gather, wrap_future bridge")
    print("  NEW  RAW-ASYNC: raw JSON coroutines, ONE event loop, zero bridging")
    print("  MP   SYNC     : Locker.py msgpack wrapper, OS thread, same bridge as NEW-SYNC")
    print("  MP   ASYNC    : Locker.py msgpack wrapper, asyncio.gather, same bridge as NEW-ASYNC")
    print("  MP   RAW-ASYNC: raw msgpack coroutines, ONE event loop, zero bridging")
    print("  OLD  SYNC     : legacy ServerLocker.py, OS thread, direct pickle socket")
    print(_SEP)

    collected_rows   = []
    all_errors       = []
    total_violations = 0

    # ── NEW LOCKER.PY — SYNC + ASYNC(WRAPPED) + RAW-ASYNC  ───────────────
    print("\nSetting up new Locker.py server + %d wrapper clients ..." % N_CLIENTS)
    tmp_new = tempfile.mktemp(suffix='.new_100x100')
    new_server, new_clients = setup_new(tmp_new)
    new_monitor = CorrectnessMonitor(new_server)
    new_port    = new_server._serverPort

    try:
        # Synchronous (bridged)
        print(_THIN)
        print("  [new]    NEW-SYNC  -- %d clients x %d rounds ..." % (N_CLIENTS, N_ROUNDS))
        ops, elapsed, lats, errs, lut_ok = run_sync(
            new_server, new_clients, _new_acquire, _new_release, 'new'
        )
        collected_rows.append(('NEW', 'SYNC', ops, elapsed, lats, lut_ok))
        if errs:
            all_errors.append("NEW SYNC: %d errors" % errs)
        print("  [new]    NEW-SYNC  done: %d ops in %.2fs\n" % (ops, elapsed))

        # Async wrapped (per-client background loop, self-pipe still active)
        print("  [new]    NEW-ASYNC -- %d clients x %d rounds ..." % (N_CLIENTS, N_ROUNDS))
        ops, elapsed, lats, errs, lut_ok = run_async_wrapped(new_server, new_clients, 'new')
        collected_rows.append(('NEW', 'ASYNC', ops, elapsed, lats, lut_ok))
        if errs:
            all_errors.append("NEW ASYNC: %d errors" % errs)
        print("  [new]    NEW-ASYNC done: %d ops in %.2fs\n" % (ops, elapsed))

        # Raw async (native coroutines, zero bridging — the true async baseline)
        print("  [new]    RAW-ASYNC -- %d coroutines x %d rounds (NO wrapper, NO bridge) ..." % (
            N_CLIENTS, N_ROUNDS
        ))
        ops, elapsed, lats, errs, lut_ok = run_async_raw(new_port, new_server, 'new')
        collected_rows.append(('NEW', 'RAW-ASYNC', ops, elapsed, lats, lut_ok))
        if errs:
            all_errors.append("RAW-ASYNC: %d errors" % errs)
            for msg in errs if isinstance(errs, list) else []:
                print("    ERROR: %s" % msg)
        print("  [new]    RAW-ASYNC done: %d ops in %.2fs\n" % (ops, elapsed))

    finally:
        total_violations += new_monitor.stop()
        print("Stopping new clients and server ...")
        teardown_new(new_server, new_clients, tmp_new)

    # ── MSGPACK LOCKER.PY — SYNC + ASYNC(WRAPPED) + RAW-ASYNC ────────
    print("\nSetting up msgpack Locker.py server + %d msgpack wrapper clients ..." % N_CLIENTS)
    tmp_mp = tempfile.mktemp(suffix='.mp_100x100')
    mp_server, mp_clients = setup_new_mp(tmp_mp)
    mp_monitor = CorrectnessMonitor(mp_server)
    mp_port    = mp_server._serverPort

    try:
        # Msgpack synchronous (bridged) — mirrors NEW SYNC but with binary wire
        print(_THIN)
        print("  [mp]     MP-SYNC   -- %d clients x %d rounds ..." % (N_CLIENTS, N_ROUNDS))
        ops, elapsed, lats, errs, lut_ok = run_sync(
            mp_server, mp_clients, _new_acquire, _new_release, 'mp'
        )
        collected_rows.append(('MP', 'SYNC', ops, elapsed, lats, lut_ok))
        if errs:
            all_errors.append("MP SYNC: %d errors" % errs)
        print("  [mp]     MP-SYNC   done: %d ops in %.2fs\n" % (ops, elapsed))

        # Msgpack async wrapped — mirrors NEW ASYNC but with binary wire
        print("  [mp]     MP-ASYNC  -- %d clients x %d rounds ..." % (N_CLIENTS, N_ROUNDS))
        ops, elapsed, lats, errs, lut_ok = run_async_wrapped(mp_server, mp_clients, 'mp')
        collected_rows.append(('MP', 'ASYNC', ops, elapsed, lats, lut_ok))
        if errs:
            all_errors.append("MP ASYNC: %d errors" % errs)
        print("  [mp]     MP-ASYNC  done: %d ops in %.2fs\n" % (ops, elapsed))

        # Msgpack raw async — mirrors RAW-ASYNC but with binary framing
        print("  [mp]     MP-RAW    -- %d coroutines x %d rounds (NO wrapper, NO bridge) ..."
              % (N_CLIENTS, N_ROUNDS))
        ops, elapsed, lats, errs, lut_ok = run_async_raw_msgpack(mp_port, mp_server, 'mp')
        collected_rows.append(('MP', 'RAW-ASYNC', ops, elapsed, lats, lut_ok))
        if errs:
            all_errors.append("MP RAW-ASYNC: %d errors" % errs)
        print("  [mp]     MP-RAW    done: %d ops in %.2fs\n" % (ops, elapsed))

    finally:
        total_violations += mp_monitor.stop()
        print("Stopping msgpack clients and server ...")
        teardown_new(mp_server, mp_clients, tmp_mp)

    # ── LEGACY SERVERLOCKER.PY — SYNC  ───────────────────────────────────
    print("\nSetting up legacy ServerLocker.py server + %d clients ..." % N_CLIENTS)
    tmp_old = tempfile.mktemp(suffix='.old_100x100')
    old_server, old_clients = setup_old(tmp_old)
    old_monitor = CorrectnessMonitor(old_server)

    try:
        print(_THIN)
        print("  [legacy] OLD-SYNC  -- %d clients x %d rounds ..." % (N_CLIENTS, N_ROUNDS))
        ops, elapsed, lats, errs, lut_ok = run_sync(
            old_server, old_clients, _old_acquire, _old_release, 'legacy'
        )
        collected_rows.append(('OLD', 'SYNC', ops, elapsed, lats, lut_ok))
        if errs:
            all_errors.append("OLD SYNC: %d errors" % errs)
        print("  [legacy] OLD-SYNC  done: %d ops in %.2fs\n" % (ops, elapsed))

    finally:
        total_violations += old_monitor.stop()
        print("Stopping legacy clients and server ...")
        teardown_old(old_server, old_clients, tmp_old)

    # ── Combined results table ────────────────────────────────────────────
    print()
    print(_SEP)
    print("  COMPARISON -- %d clients x %d rounds -- single shared lock"
          % (N_CLIENTS, N_ROUNDS))
    print(_SEP)
    print_table_header()
    for impl_label, method_label, ops, elapsed, lats, lut_ok in collected_rows:
        print_table_row(impl_label, method_label, ops, elapsed, lats, lut_ok)
    print(_SEP)

    # ── Hypothesis verdict ────────────────────────────────────────────────
    print()
    # Look up rows by (IMPL, METHOD) so adding more rows never breaks indexing.
    rows_by_label = {(r[0], r[1]): r for r in collected_rows}

    def tput(impl, method):
        """Return ops/s for the given (impl, method) pair, or 0 if missing."""
        row = rows_by_label.get((impl, method))
        return row[2] / row[3] if row and row[3] > 0 else 0

    def verdict(label, a_name, a_tput, b_name, b_tput):
        """Print a hypothesis comparison line."""
        print("  Hypothesis: %s" % label)
        if a_tput > b_tput:
            pct = (a_tput - b_tput) / b_tput * 100
            print("  Result : CONFIRMED   -- %s is %.0f%% faster (%.0f vs %.0f ops/s)"
                  % (a_name, pct, a_tput, b_tput))
        else:
            pct = (b_tput - a_tput) / a_tput * 100 if a_tput > 0 else 0
            print("  Result : NOT CONFIRMED -- %s is %.0f%% faster (%.0f vs %.0f ops/s)"
                  % (b_name, pct, b_tput, a_tput))

    verdict(
        "RAW-ASYNC (zero bridge, JSON) beats OLD-SYNC (legacy threads)",
        "RAW-ASYNC",  tput("NEW", "RAW-ASYNC"),
        "OLD-SYNC",   tput("OLD", "SYNC"),
    )
    verdict(
        "MP-RAW (zero bridge, msgpack) beats RAW-ASYNC (zero bridge, JSON)",
        "MP-RAW",     tput("MP",  "RAW-ASYNC"),
        "RAW-ASYNC",  tput("NEW", "RAW-ASYNC"),
    )
    verdict(
        "MP-SYNC (msgpack wire) beats NEW-SYNC (JSON wire) at same bridge cost",
        "MP-SYNC",    tput("MP",  "SYNC"),
        "NEW-SYNC",   tput("NEW", "SYNC"),
    )
    verdict(
        "MP-ASYNC (msgpack wire) beats NEW-ASYNC (JSON wire) at same bridge cost",
        "MP-ASYNC",   tput("MP",  "ASYNC"),
        "NEW-ASYNC",  tput("NEW", "ASYNC"),
    )
    print()

    # ── Correctness + overall ─────────────────────────────────────────────
    if total_violations == 0:
        print("  Correctness (no duplicate path holders): PASS")
    else:
        print("  Correctness: FAIL -- %d violation(s) detected" % total_violations)

    if all_errors:
        print("  Acquire errors:")
        for msg in all_errors:
            print("    %s" % msg)

    overall_ok = total_violations == 0 and not all_errors
    print("  Overall result: %s" % ("ALL TESTS PASSED" if overall_ok else "FAILURES DETECTED"))
    print()

    if not overall_ok:
        sys.exit(1)


if __name__ == '__main__':
    main()
