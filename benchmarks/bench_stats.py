"""Summary statistics shared by the benchmark scripts."""
import statistics


def percentile(sortedValues, fraction):
    """Returns the value that the given fraction of the sorted values does not exceed.

    :Parameters:
        #. sortedValues (list): Values in ascending order.
        #. fraction (float): Fraction between 0 and 1, e.g. 0.95.

    :Returns:
        #. value (float): The selected value.
    """
    index = min(int(len(sortedValues) * fraction), len(sortedValues) - 1)
    return sortedValues[index]


def print_summary(title, latencies, elapsed):
    """Prints the statistics of the times that were needed to get a lock.

    :Parameters:
        #. title (str): Name of the measured run.
        #. latencies (list): Durations in seconds, one per acquisition.
        #. elapsed (float): Wall-clock seconds of the whole run.
    """
    print(f'  Statistical summary -- {title}')
    if len(latencies) == 0:
        print('    no acquisition completed')
        return
    millis = sorted(value * 1000 for value in latencies)
    count = len(millis)
    stdev = statistics.stdev(millis) if count > 1 else 0.0
    rate = count / elapsed if elapsed > 0 else 0.0
    print(f'    acquisitions : {count} in {elapsed:.1f} s ({rate:.2f} per second)')
    print(f'    time (ms)    : min {millis[0]:.1f} | mean {statistics.mean(millis):.1f} | '
          f'median {statistics.median(millis):.1f} | stdev {stdev:.1f}')
    print(f'    tail (ms)    : p90 {percentile(millis, 0.90):.1f} | p95 {percentile(millis, 0.95):.1f} | '
          f'p99 {percentile(millis, 0.99):.1f} | max {millis[-1]:.1f}')
