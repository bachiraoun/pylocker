"""
Long-running soak test: mixed lock load with random client crashes.

The server runs in this process. Client processes lock random paths in a loop
and are killed without warning at random times, then replaced. Server
tables, open file descriptors (FDs), threads and memory are sampled at a fixed
interval; the run fails when any of them keeps growing or does not return to
its start value once every client is gone.

Usage:
  python benchmarks/test_soak.py --minutes 120
"""

import argparse
import os
import random
import resource
import signal
import subprocess
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from Locker import ServerLocker                                        # noqa: E402

PASSWORD          = 'soak-test-secret'
N_WORKERS         = 20
N_PATHS           = 8
SAMPLE_SECONDS    = 10.0
KILL_EVERY_SECONDS = 3.0
SETTLE_SECONDS    = 15.0
MAX_FD_GROWTH     = 5
MAX_THREAD_GROWTH = 5
MAX_RSS_GROWTH_MB = 20.0
SERVER_TABLES     = ('_pathsLUT', '_clientsLUT', '_pending', '_pendingCounts',
                     '_heldCounts', '_loginWriters', '_helloTasks', '_clientTasks')


def run_worker(serverFile, workerSeed):
    """
    Locks random paths in a loop until the process is killed.

    :Parameters:
        #. serverFile (str): Path of the server file to connect through.
        #. workerSeed (int): Seed of this worker's random generator.
    """
    randomGen = random.Random(workerSeed)
    locker = ServerLocker(password=PASSWORD, serverFile=serverFile, allowServing=False,
                          logger=False, defaultTimeout=30)
    while True:
        paths = [f'soak/path-{randomGen.randrange(N_PATHS)}' for _ in range(randomGen.randint(1, 3))]
        lockId = locker.acquire_lock(sorted(set(paths)))
        time.sleep(randomGen.uniform(0.0, 0.05))
        locker.release_lock(lockId)


def count_fds():
    """
    Counts the open file descriptors of this process.

    :Returns:
        #. fdCount (int): Number of entries in /dev/fd.
    """
    return len(os.listdir('/dev/fd'))


def current_rss_mb():
    """
    Reads the peak memory of this process.

    :Returns:
        #. rssMb (float): Peak resident memory in megabytes (macOS reports bytes, Linux kilobytes).
    """
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    divisor = 1024 * 1024 if sys.platform == 'darwin' else 1024
    return peak / divisor


def take_sample(server):
    """
    Records the sizes that must stay bounded.

    :Parameters:
        #. server (ServerLocker): The running server.

    :Returns:
        #. sample (dict): Server table sizes plus fds, threads and rssMb.
    """
    sample = {name: len(getattr(server, name)) for name in SERVER_TABLES}
    sample['fds'] = count_fds()
    sample['threads'] = threading.active_count()
    sample['rssMb'] = current_rss_mb()
    return sample


def start_worker(serverFile, workerSeed):
    """
    Starts one client process.

    :Parameters:
        #. serverFile (str): Path of the server file.
        #. workerSeed (int): Seed passed to the worker.

    :Returns:
        #. process (subprocess.Popen): The started client process.
    """
    return subprocess.Popen([sys.executable, os.path.abspath(__file__), '--worker',
                             serverFile, str(workerSeed)])


def check_results(baseline, samples, final):
    """
    Compares the samples with the allowed growth.

    :Parameters:
        #. baseline (dict): Sample taken before any client started.
        #. samples (list): Samples taken during the run.
        #. final (dict): Sample taken after every client is gone.

    :Returns:
        #. problems (list): One message for each failed check; empty when clean.
    """
    problems = []
    for name in SERVER_TABLES:
        if final[name] != 0:
            problems.append(f'{name} holds {final[name]} entries after all clients left')
    if final['fds'] - baseline['fds'] > MAX_FD_GROWTH:
        problems.append(f"open FDs grew from {baseline['fds']} to {final['fds']}")
    if final['threads'] - baseline['threads'] > MAX_THREAD_GROWTH:
        problems.append(f"threads grew from {baseline['threads']} to {final['threads']}")
    quarter = samples[len(samples) // 4]['rssMb']
    if final['rssMb'] - quarter > MAX_RSS_GROWTH_MB:
        problems.append(f"memory grew from {quarter:.1f} to {final['rssMb']:.1f} MB after warm-up")
    return problems


def main():
    """Runs the soak test and exits with 1 when a check fails."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--minutes', type=float, default=2.0)
    parser.add_argument('--worker', nargs=2, metavar=('SERVER_FILE', 'SEED'))
    args = parser.parse_args()
    if args.worker is not None:
        run_worker(args.worker[0], int(args.worker[1]))
        return
    serverFile = os.path.join(tempfile.mkdtemp(), 'soak_server')
    server = ServerLocker(password=PASSWORD, serverFile=serverFile, logger=False,
                          defaultTimeout=30)
    time.sleep(0.5)
    assert server.isServer, 'server did not start'
    baseline = take_sample(server)
    workers = [start_worker(serverFile, seed) for seed in range(N_WORKERS)]
    nextSeed = N_WORKERS
    samples = []
    kills = 0
    endTime = time.time() + args.minutes * 60
    nextSample = time.time() + SAMPLE_SECONDS
    while time.time() < endTime:
        time.sleep(KILL_EVERY_SECONDS)
        victim = random.randrange(len(workers))
        workers[victim].send_signal(signal.SIGKILL)
        workers[victim].wait()
        workers[victim] = start_worker(serverFile, nextSeed)
        nextSeed += 1
        kills += 1
        if time.time() >= nextSample:
            samples.append(take_sample(server))
            print(f'  t={int(args.minutes * 60 - (endTime - time.time())):6d}s kills={kills:5d} {samples[-1]}')
            nextSample += SAMPLE_SECONDS
    for worker in workers:
        worker.send_signal(signal.SIGKILL)
        worker.wait()
    time.sleep(SETTLE_SECONDS)
    final = take_sample(server)
    server.stop()
    problems = check_results(baseline, samples or [final], final)
    print(f'  baseline {baseline}\n  final    {final}')
    if len(problems) > 0:
        print('SOAK FAILED:\n  ' + '\n  '.join(problems))
        sys.exit(1)
    print('SOAK PASSED')


if __name__ == '__main__':
    main()
