# an etl data pipeline to aggregate a large dataset

## Step 1

In any kind of data-processing the underlying data is the key factor.

The phrase garbage-in-garbage-out summarizes what happens when the quality of the data is mediocre.
Invalid data is probably the most common cause for garbage-in.
Poor performance can be caused by a large skew in the data.
A bug in the software which generates the data can create inconsistent data.
Inconsistent data cannot be transformed back to the real world, because the semantics are not meaningful.
At any given point in time data contains only a subset of the states in the real world.
As a consequence a required information can happen to be not in the data or can only be estimated from the available data.

The data encode a model of the real world.
A statement about the real world should be true in the data which model this part of the real world.
To understand and read the data means to be able to make meaningful statements about the state of the real world from the data.
And equal important to understand which questions cannot be answered, which questions can only be answered partially or the answer is of poor quality.

Start with looking at the data and be able to read the data.

* describe the input data
	* understand the data
	* validate the data


## Step 2

Only if the data meets the quality requirements for the task it makes sense to get started on expensive and time-consuming data-processing.
Quality especially means here that all the information needed for the task is in the data.

At this point the most important part is already done.
High quality and complete data (for the given task) is available.

And still it is too early to get started.

Answering a one-time question on a few GB can be done with some crude hack or a simple SQL statement.
Aggregating Terabytes of data once every month within 3 business days leaves room for failures and retries.
Predicting or classifying with cycle-intensive algorithms in milli-second real-time on GB/s data streams will require a sophisticated infrastructure.

Three major factors must be defined:
* time to complete the task,
* frequency of the task and 
* the data volume to be processed by the task.

Important not to investigate the golden-path only.
Things will break and for all possible cases the recovery time should be known.
There was a major bug in the code and all the data-processing has to be re-run.

Finally all the things are in place to take a closer look at the data-processing task.

Usually there are different possible algorithms and they differ in complexity.
For very large data the space complexity is as important as the time complexity.
An estimate of both complexities is sufficient.

For a simple check of the quality of the estimates often a prototype with some sample data will do.

It is about finding information which is useful to eliminate inadequate solutions.

* describe the data-processing
	* requirements, service-level-agreement
	* calculate the data volume
	* complexity of the algorithm
	* prototype and test algorithm


## Step 3

The cornerstones needed to decide on a good design have been provided by the previous step.

A friendly reminder before proceeding that now is the last chance to make sure the right problem is going to be solved.

Not always is the exact number the answer.
A sample of the data might be sufficient to give a useful answer.
And sometimes a little cheating which cannot be observed from the outside helps and gives a satisfactory answer to a user.
Multi-player games do it (Dead Reckoning).

A list of all the tools, platforms, frameworks, services is a good start.
Use the results from the previous steps to filter that list.

For complex problems design is all about tradeoffs and balance.
And a large grain of judgement.
For now a hand-crafted solution would do the trick.
It will in the long-run do more harm than good is a judgement.
It will be better to invest more resource now and this will save us 10x in the near future is a judgement probably leading to heated discussions.

Relax.
No matter how good a decision it was.
The solution decided on will fail in some areas and win in others.
Most critical is that it was a step in the right direction.
What is left is adjusting and fine-tuning.
Week after week.

* design a solution
	* list and discuss different solutions
	* decide on solution for production


# an example

## input data

Does the data look as expected?
Do simple validations succeed?

### sample data

Get a first basic understanding of the data.
Use an IPython Notebook to analyze the sample data.

Comment on the data in the notebook.


#### findings on column data

some keywords are in quotes or brackets.
like “not provided”, [secure search]

location has no data

ranks > 100 are incomplete

market and device are good

urls are long strings.
use a hash of an url during data processing to reduce data volume


## complexity of algorithm: aggregates

### calculate data volume

uncompressed data

```
10**2 bytes per row

1.8 billion rows per day
60 billion rows per month

2 * 10**9 * 10**2 = 200 * 10**9 = 200 GB / day
6 TB / month
```

#### a single file

parser writes a single csv file
csv file
* data is encoded as utf-8 strings
* columns are separated by comma
row-oriented

probably bottlenecked by the sequential file read
with a single file any parallel processing will decrease processing time only if IO throughput is larger than memory bandwidth to a single core

Example:

memory bus throughput is 1Gb/s ~= 128MB/s

sequential read from
* S3 bucket over 1Gb network ~= 128MB/s
* rotating disk 10K ~= 160MB/s
* SSD in PCIe slot ~= 2GB/s


### Q1

which url(s) has the most ranks below 10?
url is a strong fighter in many keyword battles

leader-board url.in_top10

ignore segments, ignore time dimension
count all segments of all days


```
read_filter():
	full scan
	project url, rank
	filter rank <= 10

read_filter()

hash map on url, counter++ 	// max_counter
// filter counter < max_counter
transform swap keys, values

ranking by counter
filter ranking == 1


optimized for top1

read_filter()

hash map on url, counter++
max_counter, list url

(max_counter, list url)
```

#### memory usage

rows	unique url
10M	250K
2B	50M
60B	1.5B

average size of url is 140Byte, make it 200Byte

memory usage for hash map
1GB	250K	50MB
200GB	50M	10GB
6TB	1.5B	300GB

should generate a hash for the url (fnv 128-bit, ..)
decreases the size to ~20Bytes

memory usage for hash map
1GB	250K	5MB
200GB	50M	1GB
6TB	1.5B	30GB


### Q2

which rank 1 keyword(s) change URLs the most?
keyword with an intense battle over rank 1

leader-board keyword.rank1_battle

ignore segments, ignore time dimension
count all segments of all days

“change” can be interpreted differently
* (url1, url2, url1, url2) -> 3
   (url1, url1, url1, url2) -> 1
* (url1, url2, url1, url2) -> 2
   (url1, url1, url1, url2) -> 2


```
full scan
project crawl_date, keyword, url, rank
sort by crawl_date
filter rank == 1

hash map on keyword, counter++ if previous_url != current_url	// max_counter
// filter counter < max_counter
transform swap keys, values
ranking by counter
filter ranking == 1


optimized for top1

hash map on keyword, counter++ if previous_url != current_url
max_counter, list keyword


optimized for topN
heap top_counter, list keyword
	counter > top_counter[smallest]
	if counter in top_counter: list.append
	else: insert counter; remove top_counter[smallest]
```

#### memory usage

rows	unique keyword
10M	1.5K
2B	300K
60B	9M

average size of keyword is 25Byte, make it 50Byte

memory usage for hash map
1GB	1.5K	<1MB
200GB	300K	15MB
6TB	9M	450MB


### Q3

are the top 10 rankings the same for a dimension, for example “device”

rankings are only valid within one permutation of the dimensions
it is not possible to aggregate the ranks

Example:
the keyword “fire mount doom” has the ranks
GB-en, smartphone	7
US-en, smartphone	9
aggregate those 2 rows into
smartphone			??


```
```


## discuss solutions

Spark job, python script, lambda functions on AWS, Flink, Scala/Akka, SQL on column-store

single core, multi core or cluster computing

partitioning
crash of node: restart, clean files, redundant nodes


## Appendix: Parser Output

assuming
* all values are not null
* strings are not empty
* valid dates
* valid devices
* valid markets
* valid url, shorter than 250 bytes
* keyword shorter than 60 bytes
* sanitized strings (no binary, ..)

consistent data
* each keyword in each segment has 100 ranks with an url
* all segments have data (markets, devices, ..)
* all keywords in all segments are complete

questions to identify possible failures
* could the parsing drop one or more markets?
* could the parsing drop keywords?
* could the ‘crawl date’ be wrong?
* could the filename be wrong (crawl_date and filename_suffix do not match)?
* no file or an empty file
* appending to an existing file, write the same data twice but with different crawl_date?
* incomplete write to the file (parser crashed during write)?
