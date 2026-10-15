"""
Runs several processes that compete for one lock and prints how the competition went.

Every process acquires and releases the same path several times and prints a line each time it
gets or is denied the lock. A total summary is printed when all processes are done.

Usage:
  python benchmarks/serverlock_lock_competition.py [--nproc 6] [--rounds 10] [--hold 0.5] [--timeout 5]
"""
import argparse
import datetime
import multiprocessing
import statistics
import threading
import time

from pylocker import ServerLocker

LOCK_PATH = 'competition/single-file'
PASSWORD = 'no-password'
EXTRA_WAIT = 30


def now_text():
    """Returns the current time as text for the output lines.

    :Returns:
        #. text (str): Time formatted as ``YYYY-MM-DD|HH:MM:SS``.
    """
    return datetime.datetime.now().strftime('%Y-%m-%d|%H:%M:%S')


def compete(index, rounds, holdTime, timeout, barrier, resultQueue):
    """Acquires and releases the shared lock repeatedly in one process and reports the measurements.

    :Parameters:
        #. index (int): Number of this process.
        #. rounds (int): Number of acquire attempts.
        #. holdTime (float): Seconds the lock is held once acquired.
        #. timeout (float): Seconds to wait for the lock before giving up.
        #. barrier (multiprocessing.Barrier): Makes every process wait for the others before stopping,
           because the process that became the server must outlive its clients.
        #. resultQueue (multiprocessing.Queue): Receives the measurements as one dictionary.
    """
    locker = ServerLocker(name=f'worker_{index}', password=PASSWORD)
    acquiredTimes = []
    deniedTimes = []
    freeAcquireTimes = []
    releaseFailureCount = 0
    for _ in range(rounds):
        # A private path nobody else uses measures the acquire cost without any queueing
        freeStart = time.time()
        freeAcquired, freeLockId = locker.acquire_lock(path=f'{LOCK_PATH}-private-{index}', timeout=timeout)
        freeAcquireTimes.append(time.time() - freeStart)
        if freeAcquired:
            locker.release_lock(lockId=freeLockId)
        startTime = time.time()
        acquired, lockId = locker.acquire_lock(path=LOCK_PATH, timeout=timeout)
        waitTime = time.time() - startTime
        if not acquired:
            deniedTimes.append(waitTime)
            print(f'{now_text()} - {locker.name}: DENIED after {waitTime:.2f} s (code {lockId})', flush=True)
            continue
        acquiredTimes.append(waitTime)
        print(f'{now_text()} - {locker.name}: acquired in {waitTime:.2f} s', flush=True)
        time.sleep(holdTime)
        released, message = locker.release_lock(lockId=lockId)
        if not released:
            releaseFailureCount += 1
            print(f'{now_text()} - {locker.name}: RELEASE FAILED ({message})', flush=True)
    resultQueue.put({'index': index, 'acquiredTimes': acquiredTimes, 'deniedTimes': deniedTimes,
                     'freeAcquireTimes': freeAcquireTimes, 'releaseFailureCount': releaseFailureCount})
    try:
        barrier.wait()
    except threading.BrokenBarrierError:
        pass
    locker.stop()


def describe(label, seconds):
    """Prints the mean, median and maximum of a list of durations.

    :Parameters:
        #. label (str): Name of the measured time.
        #. seconds (list): Durations in seconds.
    """
    if len(seconds) == 0:
        print(f'  {label:<24}: none')
        return
    print(f'  {label:<24}: mean {statistics.mean(seconds) * 1000:.1f} ms | '
          f'median {statistics.median(seconds) * 1000:.1f} ms | max {max(seconds) * 1000:.1f} ms')


def print_total(results, elapsed, arguments):
    """Prints the totals of all processes.

    :Parameters:
        #. results (list): One measurement dictionary per process.
        #. elapsed (float): Wall-clock seconds of the whole run.
        #. arguments (argparse.Namespace): The run settings.
    """
    acquiredTimes = [value for result in results for value in result['acquiredTimes']]
    deniedTimes = [value for result in results for value in result['deniedTimes']]
    releaseFailureCount = sum(result['releaseFailureCount'] for result in results)
    perProcess = [len(result['acquiredTimes']) for result in results]
    print('=' * 76)
    print(f'  TOTAL SUMMARY -- {arguments.nproc} processes x {arguments.rounds} rounds, '
          f'hold {arguments.hold} s, timeout {arguments.timeout} s')
    print('=' * 76)
    print(f'  attempts                : {len(acquiredTimes) + len(deniedTimes)}')
    print(f'  acquired                : {len(acquiredTimes)}')
    print(f'  denied                  : {len(deniedTimes)}')
    print(f'  release failures        : {releaseFailureCount}')
    print(f'  wall time               : {elapsed:.1f} s ({len(acquiredTimes) / elapsed:.2f} acquired per second)')
    print(f'  acquired per process    : min {min(perProcess)} | max {max(perProcess)}')
    freeAcquireTimes = [value for result in results for value in result['freeAcquireTimes']]
    describe('acquire cost (no queue)', freeAcquireTimes)
    describe('wait for contended lock', acquiredTimes)
    describe('time to be denied', deniedTimes)
    print('=' * 76)


def parse_arguments():
    """Reads and checks the command line.

    :Returns:
        #. arguments (argparse.Namespace): The run settings.
    """
    parser = argparse.ArgumentParser(description='Several processes compete for one lock.')
    parser.add_argument('--nproc', type=int, default=6, help='Number of competing processes.')
    parser.add_argument('--rounds', type=int, default=10, help='Acquire attempts per process.')
    parser.add_argument('--hold', type=float, default=0.5, help='Seconds a lock is held.')
    parser.add_argument('--timeout', type=float, default=5, help='Seconds to wait for a lock.')
    arguments = parser.parse_args()
    if arguments.nproc < 1 or arguments.rounds < 1 or arguments.hold < 0 or arguments.timeout <= 0:
        parser.error('--nproc and --rounds must be at least 1, --hold not negative, --timeout positive')
    return arguments


def main():
    """Starts the competing processes, collects their measurements and prints the totals."""
    arguments = parse_arguments()
    budget = arguments.nproc * arguments.rounds * (arguments.hold + arguments.timeout) + EXTRA_WAIT
    barrier = multiprocessing.Barrier(arguments.nproc, timeout=budget)
    resultQueue = multiprocessing.Queue()
    processes = [multiprocessing.Process(target=compete, args=(index, arguments.rounds, arguments.hold,
                                                               arguments.timeout, barrier, resultQueue))
                 for index in range(arguments.nproc)]
    startOfRun = time.time()
    for process in processes:
        process.start()
    # Results are read before joining, because a process cannot exit while its queue data is unread
    results = [resultQueue.get(timeout=budget) for _ in processes]
    elapsed = time.time() - startOfRun
    for process in processes:
        process.join()
    print_total(results, elapsed, arguments)


if __name__ == '__main__':
    main()
