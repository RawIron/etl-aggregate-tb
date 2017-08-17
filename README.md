# build an etl data pipeline to aggregate a large dataset

## Step 1

In data-processing the underlying data is the key factor.
The phrase garbage-in-garbage-out summarizes what happens when the quality of the data is mediocre.

Invalid data is probably the most common cause for garbage-in.

Poor performance can be caused by a large skew in the data.

A bug in the software which generates the data can create inconsistent data.
Inconsistent data cannot be transformed back to the real world, because the semantics are not meaningful.

At any given point in time data contains only a subset of the states in the real world.
As a consequence a required information might be missing in the data or can only be estimated from the available data.

The data encode a model of the real world.
A statement about the real world should be true in the data which model this part of the real world.
To understand and read the data means to be able to make meaningful statements about the state of the real world from the data.
And equal important to understand which questions cannot be answered, which questions can only be answered partially or which answer is of poor quality.

Start with looking at the data and be able to read the data.

* describe the input data
	* understand the data
	* validate the data


## Step 2

Only if the data meets the quality requirements for the task it makes sense to get started on expensive and time-consuming data-processing.
Quality especially means that all the information needed for the task is in the data.

At this point an important part is already done.
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
For example there was a major bug in the code and all the data-processing has to be re-run.

Now all the things are in place to take a closer look at the data-processing task.

Usually there are different possible algorithms and they differ in complexity.
For very large data the space complexity is as important as the time complexity.
An estimate of both complexities is sufficient.

For a simple check of the quality of the estimates often a prototype with some sample data will do.

It is about finding information which is useful to eliminate inadequate solutions later on.

* describe the data-processing
	* requirements, service-level-agreement
	* calculate the data volume
	* time and space complexity of the algorithm
	* prototype and test the algorithm


## Step 3

The necessary facts to decide on a good design have been provided by the previous step.

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
It will be better to invest more resource now and this will save us 10x in the near future is a judgement.

Relax.
No matter how good a decision it was.
The solution decided on will fail in some areas and win in others.
Most critical is that it was a step in the right direction.
What is left is adjusting and fine-tuning, and adjusting and fine-tuning, ..

* design a solution
	* list and discuss different solutions
	* decide on solution for production


# an example

## describe the input data

An IPython Notebook is a great tool to analyze sample data.
The data can be commented in the notebook.

The data used in this example are described in the *data-distributions.ipynb* notebook.
A few examples from the findings:

* some keywords are in quotes or brackets.
like “not provided”, [secure search]
* location has no data
* ranks > 100 are incomplete
* market and device are good
* urls are long strings.
use a hash of url strings during data processing to reduce data volume should be considered.

One thing stands out about this data.
The data is valid, clean and has no noise.
It was generated by algorithms.
It can precisely be described by formulas.

Compare this to data from a billing system.
Billing data from a web-service have a lot of noise.
Fraud Detection will be only one of many filters applied to reduce the noise and improve data quality.


## describe the data-processing

### estimate the data volume

The algorithms work on uncompressed data.
Algorithms on compressed data are ignored for this example.

```
10**2 bytes per row

2 billion rows per day
60 billion rows per month

2 * 10**9 * 10**2 = 200 * 10**9 = 200 GB / day
6 TB / month
```

One classification of data-processing algorithms asks whether an algorithm can construct the overall result from partial results.
The processing is done by a (complicated) function called **F**.

For one group of algorithms an operator **+** exists such that:

```
F(day1) + .. + F(dayN) = F(month)
```
This group is called distributive aggregate functions.

And for a second group such an operator does not exist.
In this group the partial results are of no use when constructing the overall result, for example the *median()*.
This is the group of holistic aggregate functions.

There is a third group.
For example a partial sort can be used in constructing the complete sort.
Though it is not possible to simply take the partial sorts and *concat* them to create a complete sort.
The operator used to calculate the partial results cannot be used to calculate the overall result from the partial results.
These are the algebraic aggregate functions.


### data partitioning

The previous *day-month* example is a special case of the general concept of partitioning.
The data is partitioned by *day* and a query aggregates over a month.

The only way to scale data-processing for very large data volumes is to partition the data.

To read a single very large file and write the partitions to a storage system is time-costly.
The producer of large data should partition the data on write.
And append new data to an already existing partitioned file.


#### non-partitioned data: a single file

A producer writes a single large csv file.
The data
* is encoded as utf-8 strings
* has columns separated by comma
* is row-oriented

Data-processing is probably bottlenecked by the sequential read of the data in the file.
With a single file any parallel processing will decrease processing time only if IO throughput is larger than memory bandwidth to a single core.
Assuming the processing is not CPU-bound.

Example:

```
memory bus throughput is 1Gb/s ~= 128MB/s
file cache is cold

sequential read from
* S3 bucket over 1Gb network ~= 128MB/s
* rotating disk 10K ~= 160MB/s
* SSD in PCIe slot ~= 2GB/s
```


### complexity of algorithm: aggregate Q1

which url(s) has the most ranks below 10?

url is a strong fighter in many keyword battles

leader-board url.in_top10

ignore segments, ignore time dimension = count over all segments of all days


#### execution plan

```
full scan
project url, serp_rank
filter serp_rank <= 10

hash map on url
count url
transform the hash map to a list of tuples, swapped keys with values

rank by counter
filter rank == 1
```


#### size and memory usage of hash map

```
rows	unique url
10M	250K
2B	50M
60B	1.5B
```

average size of url is 140Byte, make it 200Byte

```
memory usage for hash map
1GB	250K	50MB
200GB	50M	10GB
6TB	1.5B	300GB
```

should generate a hash for the url (fnv 128-bit, ..) and decrease the size to ~20Bytes

```
memory usage for hash map
1GB	250K	5MB
200GB	50M	1GB
6TB	1.5B	30GB
```


#### partition and distributed processing

partition function can be any.
the aggregate function is distributive.

in distributed processing the large list of tuples is performance critical.
it has to be sorted and then merged on a master node.


#### note: pre-aggregated data

pre-aggregating the data should be considered if
* the aggregate function is distributive
* cardinality of the dimensions is small

given two dimensions (time, market) the data could be pre-aggregated over those dimensions

```
card(time) = 30
card(market) = 3

given a distributive aggregate function

rows to be read to calculate the aggregate:
one day is reduced from 2B rows to only 3 rows
one month is reduced from 60B rows to only 90 rows
```

In the SQL world the data is pre-aggregated with a SQL-SELECT statement.
The result set of the SQL statement is stored in a materialized view.


in the example with an estimated unique url count

```
the aggregate function is distributive

pre-aggregate the count per url for every segment permutation
170701	CA 	desktop	/the/url1	5
170701	CA 	desktop	/the/url2	9

partition by day and this creates around 125,000 rows per day for the sample data of 10 * 10**6 rows
on production the data for one month is 60B rows = 60 * 10**3 * 10**6

125,000 * 6 * 10**3 = 750 * 10**6 rows per day

30 partitions
each partition has 750M rows


aggregate over a single day:
scan 1 partition and aggregate on the fly
750M rows

over 30 days:
scan 30 partitions and aggregate on the fly
750M * 30 = 22.5B rows
```

The above example shows how the large cardinality of the URL dimension creates very large materialized views.

Though this is not the end.
In this example only the top performers are of interest.
Is there a safe rule to cut the list after a calculated rank?

sprinter vs. endurance

```
sprinter_url1	3, 15, 2
sprinter_url2	15, 1, 1
sprinter_url3	3,  1, 15
endurance_url	7,  7, 7
```

High volatility of the sprinters is in the way to make a safe cut.

Often in data sets about contenders there is a small group of top performers with high scores.
This group is separated by the remaining contenders by a large gap in the scores.
The volatility in the scores is low.

It would be interesting to look deeper into the data and check if a safe cut can be done.


### complexity of algorithm: aggregate Q2

which rank 1 keyword(s) change URLs the most?

keyword with an intense battle over rank 1

leader-board keyword.rank1_battle

ignore segments, ignore time dimension = count all segments of all days

“change” can be interpreted differently
```
(url1, url2, url1, url2) -> 3
(url1, url1, url1, url2) -> 1

(url1, url2, url1, url2) -> 2
(url1, url1, url1, url2) -> 2
```

#### execution plan

```
full scan
filter serp_rank == 1
sort by crawl_date, market, device
project keyword, url

hash map on keyword
count keyword if previous_url != current_url
transform hash map to list of tuples, swapped keys with values

rank by counter
filter rank == 1
```


#### size and memory usage of hash map

```
rows	unique keyword
10M	1.5K
2B	300K
60B	9M
```

average size of keyword is 25Byte, make it 50Byte

```
memory usage for hash map
1GB	1.5K	<1MB
200GB	300K	15MB
6TB	9M	450MB
```

#### partition and distributed processing

the partition function is a hash(keyword).
then all rows for one keyword will be processed on one node.

distributed processing should work fine due to the small list of tuples.


### complexity of algorithm: aggregate Q3

are the top 10 rankings the same for a dimension, for example “device”?

rankings are only valid within one permutation of the dimensions.
it is not possible to aggregate the ranks.

Example:

```
the keyword “fire mount doom” has the ranks

GB-en, smartphone	7
US-en, smartphone	9

aggregate those 2 rows into
smartphone			??
```


## design a solution


### a list of tools, frameworks, infrastructure

* Spark job
* python script
* lambda functions on AWS
* Flink
* Scala/Akka
* SQL on column-store (redshift, bigquery, hbase, ..)

in case the data is already in a clustered column-store some SQL-magic might work.
otherwise Spark is a good choice.

Spark should behave nicley when a node crashes: restart, clean files, redundant nodes.
definitely needs some more investigation here.


### single core, multi core or cluster computing

the daily data of 200GB could be done on a large machine.
with partitions processing will be fast.

for the monthly data a cluster is needed.


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
