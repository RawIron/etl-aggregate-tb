import csv
from collections import Counter
from timeit import timeit


def get_leaders(leaderboard, first_n=1):
    top_n = []
    max_counter, _ = leaderboard[0]
    first_n -= 1
    for counter, name in leaderboard:
        if counter == max_counter:
            top_n.append((counter, name))
        elif first_n > 0:
            top_n.append((counter, name))
            max_counter = counter
            first_n -= 1
        else:
            break
    return top_n


def create_reader(csv_file):
    with open(csv_file) as f:
        reader = csv.reader(f)
        header = reader.__next__()
        for row in reader:
            yield row


def count_urls(reader):
    url_counter = Counter()
    for _, _, _, _, _, rank, url in reader:
        if int(rank) <= 10:
            url_counter.update({url: 1})
    return url_counter


def create_leaderboard(url_counter):
    counters_as_key = ((v,k) for (k,v) in url_counter.items())
    leaderboard = sorted(counters_as_key, reverse=True)
    return leaderboard


def run_loops_with_sort(reader):
    url_counter = count_urls(reader)
    leaderboard = create_leaderboard(url_counter)
    return get_leaders(leaderboard, 2)


def count_and_track_leaders(reader):
    url_counter = Counter()
    leaders = [(0, '')]
    lowest_score = 0

    for _, _, _, _, _, rank, url in reader:
        if int(rank) <= 10:
            url_counter.update({url: 1})
            if url_counter[url] == lowest_score:
                leaders.append((lowest_score, url))
            elif url_counter[url] > lowest_score:
                lowest_score = url_counter[url]
                leaders = [(lowest_score, url)]

    return leaders


def run_loops_no_sort(reader):
    return count_and_track_leaders(reader)


#FILE_NAME = './test.csv'
FILE_NAME = './getstat_com_serp_report_201707.csv'
SCENARIO = "NO_SORT"

def create_func(reader):
    def runner():
        if SCENARIO == "NO_SORT":
            return run_loops_no_sort(reader)
        elif SCENARIO == "WITH_SORT":
            return run_loops_with_sort(reader)
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
