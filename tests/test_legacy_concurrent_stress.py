"""
Concurrent stress-test for the legacy ServerLocker.py implementation.

Runs the same four contention phases as test_concurrent_stress.py so results
can be compared directly against the new Locker.py numbers.

Key architectural differences between old and new that affect performance:
  - Transport    : multiprocessing.connection (pickle + HMAC digest) vs JSON/TCP
  - Server model : one OS thread per connected client vs single asyncio event loop
  - Client model : one blocking background receiver thread per client vs async loop
  - Bind address : server.address (machine IP) vs 0.0.0.0; clients must use
                   server.address / server.port explicitly
  - start() typo : start(address=, port=) has a typo ('adress') in the original
                   source; clients call connect() directly instead

Phases:
  PHASE 1 -- 100 clients x  20 rounds, 1 shared path  (maximum contention).
  PHASE 2 -- 100 clients x  20 rounds, 10 shared paths (medium contention).
  PHASE 3 -- 100 clients x  20 rounds, private paths   (zero contention).
  PHASE 4 -- 100 clients x 100 rounds, 1 shared path   (sustained contention).

Only synchronous acquire_lock / release_lock is tested; the old implementation
has no async API.

Usage:
  python tests/test_legacy_concurrent_stress.py
"""

import collections
import os
import resource
import statistics
import sys
import tempfile
import threading
import time

# ---------------------------------------------------------------------------
# Raise the file-descriptor soft limit before any imports create sockets.
# ---------------------------------------------------------------------------
_ORIGINAL_SOFT, _HARD_LIMIT = resource.getrlimit(resource.RLIMIT_NOFILE)
resource.setrlimit(resource.RLIMIT_NOFILE, (4096, _HARD_LIMIT))

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from ServerLocker import ServerLocker                                  # noqa: E402

# ---------------------------------------------------------------------------
# Test parameters
# ---------------------------------------------------------------------------
PASSWORD        = 'stress-test-secret'
N_CLIENTS       = 100
ACQUIRE_TIMEOUT = 60
MAX_LOCK_TIME   = 300
PROGRESS_EVERY  = 5.0

# Each tuple: (label, path_fn, n_rounds)
PHASES = [
    (
        'PHASE 1: 1 path    x20  (max contention)',
        lambda i: 'competition/single-file',
        20,
    ),
    (
        'PHASE 2: 10 paths  x20  (medium contention)',
        lambda i: 'competition/path-%d' % (i % 10),
        20,
    ),
    (
        'PHASE 3: private   x20  (zero contention)',
        lambda i: 'competition/private/client-%d' % i,
        20,
    ),
    (
        'PHASE 4: 1 path    x100 (sustained contention)',
        lambda i: 'competition/single-file',
        100,
    ),
]

# ---------------------------------------------------------------------------
# Table formatting
# ---------------------------------------------------------------------------
_COL_W = (14, 6, 7, 7, 7, 7, 7, 7, 8)
_HDRS  = ('PHASE', 'METHOD', 'OPS', 'OPS/S', 'p50ms', 'p95ms', 'p99ms', 'MAXms', 'LUT')
_SEP   = '=' * 76
_THIN  = '-' * 76


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


def print_table_row(phase_label, method_label, total_ops,
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

    lut_status  = 'clean' if lut_clean else 'LEAKED'
    short_phase = phase_label.split(':')[0]
    print(_row_fmt(
        short_phase,
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
# Correctness monitor
# ---------------------------------------------------------------------------
class CorrectnessMonitor:
    """Background thread that polls lockedPaths every 20 ms for violations.

    The old ServerLocker stores paths as bytes keys, so both bytes and str
    keys are normalised before comparison.

    .. code-block:: python

        monitor = CorrectnessMonitor(server)
        # ... run workload ...
        violation_count = monitor.stop()
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
        """Poll the server lookup table and flag concurrent path holders."""
        while not self._stop_event.is_set():
            locked = self._server.lockedPaths or {}
            buckets = collections.defaultdict(list)
            for path, rec in locked.items():
                path_key = path.decode('utf-8', 'replace') if isinstance(path, bytes) else path
                lock_id  = rec.get('lock_unique_id') or rec.get('request_unique_id', '?')
                buckets[path_key].append(lock_id)
            for path, ids in buckets.items():
                if len(ids) > 1:
                    key = (path, tuple(sorted(ids)))
                    if key not in self._seen:
                        self._seen.add(key)
                        with self._mutex:
                            self._violations += 1
                        print(
                            "\n  !! VIOLATION: '%s' held by %d IDs at once: %s"
                            % (path, len(ids), ids)
                        )
            time.sleep(0.02)


# ---------------------------------------------------------------------------
# Server + client lifecycle
# ---------------------------------------------------------------------------
def setup(tmp_path):
    """Start one legacy server and connect N_CLIENTS clients.

    The old server binds to the machine's primary IP address (not 0.0.0.0),
    so clients use server.address and server.port.  connect() is called
    directly to avoid the 'adress' typo in start().

    :Parameters:
        #. tmp_path (str): File-system path for the server fingerprint file.

    :Returns:
        #. server (ServerLocker): The running legacy server instance.
        #. clients (list): List of N_CLIENTS connected ServerLocker clients.
    """
    server = ServerLocker(
        password=PASSWORD,
        serverFile=tmp_path,
        autoconnect=True,
        defaultTimeout=ACQUIRE_TIMEOUT,
        maxLockTime=MAX_LOCK_TIME,
        logger=False,
    )
    time.sleep(0.8)
    assert server.isServer, "Legacy server failed to start"
    print("  Server up on %s:%d" % (server.address, server.port))

    srv_address = server.address
    srv_port    = server.port

    clients = []
    for i in range(N_CLIENTS):
        cl = ServerLocker(
            password=PASSWORD,
            serverFile=False,
            autoconnect=False,
            defaultTimeout=ACQUIRE_TIMEOUT,
            logger=False,
        )
        cl.connect(address=srv_address, port=srv_port)
        clients.append(cl)
        if (i + 1) % 10 == 0:
            alive = sum(1 for c in clients if c.isClient)
            print("  %3d/%d connected (%d live)" % (i + 1, N_CLIENTS, alive))
            time.sleep(0.1)

    connected = sum(1 for c in clients if c.isClient)
    print("  %d/%d clients connected\n" % (connected, N_CLIENTS))
    assert connected == N_CLIENTS, (
        "Only %d/%d legacy clients connected" % (connected, N_CLIENTS)
    )
    return server, clients


def teardown(server, clients, tmp_path):
    """Stop all clients and the server, then remove the fingerprint file.

    :Parameters:
        #. server (ServerLocker): The legacy server instance to stop.
        #. clients (list): The list of client instances to stop.
        #. tmp_path (str): Fingerprint file path to remove.
    """
    for i, cl in enumerate(clients):
        try:
            cl.stop()
        except Exception:
            pass
        if (i + 1) % 25 == 0:
            print("  Stopped %d/%d clients" % (i + 1, N_CLIENTS))
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
# Synchronous phase runner
# ---------------------------------------------------------------------------
def run_sync_phase(server, clients, path_fn, n_rounds):
    """Run one contention phase with one OS thread per client.

    Each thread calls acquire_lock / release_lock n_rounds times.
    All 100 threads start simultaneously and are joined with a 240-second
    deadline.  Progress is printed every PROGRESS_EVERY seconds.

    :Parameters:
        #. server (ServerLocker): The running server (used for LUT health check).
        #. clients (list): Connected legacy ServerLocker client instances.
        #. path_fn (callable): Accepts the client index and returns the lock
           path string that client should acquire.
        #. n_rounds (int): Number of acquire-release cycles each client runs.

    :Returns:
        #. totalOps (int): Total successful acquire-release cycles completed.
        #. elapsed (float): Wall-clock seconds from first thread start to last join.
        #. latencies (list): Per-operation round-trip times in seconds.
        #. errorCount (int): Number of failed acquire calls.
        #. lutClean (bool): True if server.lockedPaths is empty after the phase.
    """
    per_client_lats = {}
    errors          = []
    total_expected  = N_CLIENTS * n_rounds

    def worker(idx, cl):
        lats = []
        path = path_fn(idx)
        for _ in range(n_rounds):
            t0 = time.perf_counter()
            ok, lock_id = cl.acquire_lock(path, timeout=ACQUIRE_TIMEOUT)
            if not ok:
                errors.append(
                    "legacy sync client-%d: acquire failed on '%s' (code=%s)"
                    % (idx, path, lock_id)
                )
                continue
            cl.release_lock(lock_id)
            lats.append(time.perf_counter() - t0)
        per_client_lats[idx] = lats

    threads = [
        threading.Thread(target=worker, args=(i, cl), daemon=True,
                         name='legacy-client-%d' % i)
        for i, cl in enumerate(clients)
    ]

    t_start  = time.perf_counter()
    for t in threads:
        t.start()

    remaining = list(threads)
    deadline  = t_start + 240.0
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
            print("  SYNC  %.1fs  %d/%d ops  %d threads active" % (
                time.perf_counter() - t_start, done,
                total_expected, len(remaining),
            ))
            next_tick = time.perf_counter() + PROGRESS_EVERY

    elapsed  = time.perf_counter() - t_start
    all_lats = [lat for lats in per_client_lats.values() for lat in lats]

    time.sleep(0.5)
    lut_clean = not bool(server.lockedPaths)
    return len(all_lats), elapsed, all_lats, len(errors), lut_clean


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    """Orchestrate the full legacy stress-test suite and print the results table."""
    print()
    print(_SEP)
    print("  Legacy ServerLocker.py -- concurrent stress test")
    print("  Transport: multiprocessing.connection (pickle + HMAC digest)")
    print("  Server model: one OS thread per connected client")
    print("  fd limit raised: %d -> 4096" % _ORIGINAL_SOFT)
    print("  %d clients  |  phases: %s"
          % (N_CLIENTS, ', '.join('%d rds' % p[2] for p in PHASES)))
    print(_SEP)

    tmp_path = tempfile.mktemp(suffix='.legacy_stress')
    print("\nStarting legacy server and connecting clients ...")
    server, clients = setup(tmp_path)

    monitor        = CorrectnessMonitor(server)
    collected_rows = []
    all_errors     = []

    try:
        for phase_label, path_fn, n_rounds in PHASES:
            print(_SEP)
            print("  %s" % phase_label)
            print(_THIN)
            print("  [SYNC]  starting ...")

            ops, elapsed, lats, errs, lut_ok = run_sync_phase(
                server, clients, path_fn, n_rounds
            )
            collected_rows.append(
                (phase_label, 'SYNC', ops, elapsed, lats, lut_ok)
            )
            if errs:
                all_errors.append("SYNC / %s: %d errors" % (phase_label, errs))
            print("  [SYNC]  done: %d ops in %.2fs" % (ops, elapsed))
            print()

    finally:
        violations = monitor.stop()
        print("\nStopping clients and server ...")
        teardown(server, clients, tmp_path)

    # ── Results table ─────────────────────────────────────────────────────
    print()
    print(_SEP)
    print("  LEGACY ServerLocker.py -- RESULTS  --  %d clients" % N_CLIENTS)
    print(_SEP)
    print_table_header()
    for phase_label, method_label, ops, elapsed, lats, lut_ok in collected_rows:
        print_table_row(phase_label, method_label, ops, elapsed, lats, lut_ok)
    print(_SEP)

    # ── Verdict ───────────────────────────────────────────────────────────
    print()
    if violations == 0:
        print("  Correctness (no duplicate path holders): PASS")
    else:
        print("  Correctness: FAIL -- %d mutual-exclusion violations detected" % violations)

    if all_errors:
        print("  Acquire errors:")
        for msg in all_errors:
            print("    %s" % msg)

    overall_ok = violations == 0 and not all_errors
    print("  Overall result: %s" % ("ALL TESTS PASSED" if overall_ok else "FAILURES DETECTED"))
    print()

    if not overall_ok:
        sys.exit(1)


if __name__ == '__main__':
    main()
