'''
q1 solved with map-reduce

prototype a map-reduce algorithm
exercise map-reduce thinking
'''


import csv
from collections import Counter
from timeit import timeit
from functools import reduce


def create_reader(csv_file):
    '''
    generator of rows
    row is a list
    '''
    with open(csv_file) as f:
        reader = csv.reader(f)
        header = reader.__next__()
        for row in reader:
            yield row


def create_get_leaders(first_n):

    def get_leaders(top_n, contender):
        '''
        first time reduce passes s[0], s[1] as arguments
        the sequence passed to reduce is a list of 2-tuple (int, str)

        reduce emits a single value
        here this single value is a 2-tuple ([], int)
        '''
        if isinstance(top_n, tuple) and isinstance(top_n[0], int):
            top_contender = top_n
            leaders = []
            leaders.append(top_contender)
            n_more = first_n - 1
            top_n = (leaders, n_more)

        leaders, n_more = top_n
        max_counter, _ = leaders[-1]
        counter, _ = contender
        if counter == max_counter:
            leaders.append(contender)
        elif n_more > 0:
            leaders.append(contender)
            n_more -= 1

        return (leaders, n_more)
    
    return get_leaders


def is_top10(row):
    _, _, _, _, _, rank, _ = row
    return (int(rank) <= 10)


def count_urls(url_counter, row):
    '''
    first time reduce passes s[0], s[1] as arguments
    the sequence passed to reduce is a list of rows
    and each row is a list too

    reduce emits a single value
    here this single value is a collection.Counter
    '''
    if isinstance(url_counter, list):
        _, _, _, _, _, rank, url = url_counter
        url_counter = Counter()
        url_counter.update({url: 1})

    _, _, _, _, _, rank, url = row
    url_counter.update({url: 1})

    return url_counter


def swap_kv(counter):
    (k,v) = counter
    return (v,k)


def run_solution_with_sort(reader):
    get_leaders = create_get_leaders(2)

    top10_rank = filter(is_top10, reader)
    url_counter = reduce(count_urls, top10_rank)
    counters_as_key = map(swap_kv, url_counter.items())
    leaderboard = sorted(counters_as_key, reverse=True)
    top_n = reduce(get_leaders, leaderboard)
    return top_n[0]



def count_and_track_leaders(leaderboard, row):
    if isinstance(leaderboard, list):
        _, _, _, _, _, rank, url = leaderboard
        url_counter = Counter()
        url_counter.update({url: 1})
        leaders = [(1, url)]
        leaderboard = (leaders, url_counter)

    _, _, _, _, _, rank, url = row
    leaders, url_counter = leaderboard
    lowest_score, _ = leaders[-1]

    url_counter.update({url: 1})
    if url_counter[url] == lowest_score:
        leaders.append((url_counter[url], url))
    elif url_counter[url] > lowest_score:
        leaders = [(url_counter[url], url)]

    return (leaders, url_counter)


def run_solution_no_sort(reader):
    top10_rank = filter(is_top10, reader)
    top_n = reduce(count_and_track_leaders, top10_rank)
    return top_n[0]



SCENARIO = "NO_SORT"
FILE_NAME = './getstat_com_serp_report_201707.csv'
#FILE_NAME = './test.csv'

def create_func(reader):
    def runner():
        if SCENARIO == "NO_SORT":
            return run_solution_no_sort(reader)
        elif SCENARIO == "WITH_SORT":
            return run_solution_with_sort(reader)
    return runner


class CaptureResult(object):
    def __init__(self, runner):
        self.runner = runner

    def __call__(self):
        self.result = self.runner()
        return self.result


reader = create_reader(FILE_NAME)
runner = CaptureResult(create_func(reader))

print(timeit(runner, number=1))
print(runner.result)
