import concurrent.futures

from utils import duration

POW_LIST = [i for i in range(1000000, 1000008)]


@duration
def call_sequential():
    # Takes ~42 seconds
    for number in POW_LIST:
        pow(number, number)


@duration
def call_with_threads():
    # Takes ~40 seconds
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(pow, number, number) for number in POW_LIST]

    for future in concurrent.futures.as_completed(futures):
        # Print or use the result, if desired
        continue


@duration
def call_with_processes():
    # Takes ~20 seconds
    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = [executor.submit(pow, number, number) for number in POW_LIST]

    for future in concurrent.futures.as_completed(futures):
        # Print or use the result, if desired
        continue


if __name__ == "__main__":
    call_sequential()
    call_with_threads()
    call_with_processes()
