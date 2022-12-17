# Multiquery Execution and Slicing

## Multiquery Execution
The source code for this part of the project resides in the `src/main/scala/multiquery/` folder. The given code implements the YSB use case and performs 3 queries together. Each query starts with a join operation, where the campaign_id corresponding to each ad_id is added. This is then followed by filter operation, which differs from query to query. Each query filters based on distinct `event_type`. Then an aggregation (count) is performed.
While the results can be either saved to csv files or printed to the console, the benchmarks and statistics of the processing of each window is printed to the console at the interval specified as "trigger". The benchmark is also saved to a .txt file.
The experiment was to increase the throughput of the Kafka producer, and compare the performance of multiquery execution under heavy load. 

### Result
Due to resource constraints on the execution device, the limitations of multiquery execution could not be assessed. However, the source code would hold up if executed on a capable device.

## Improving processing on overlapping windows using slicing
In case of overlapping windows, it is likely that many results would be recomputed unnecessarily. This computation is costly in a usecase like that of streaming. Therefore, we make use of slicing, where each window is sliced (based on the window length and sliding length, by taking the `gcd` of sliding_length and (window_length - sliding_length)).
Once slices are created, the next step involves knowing all windows a particular slice belongs to. A simple index based approach is trivial, however, the Spark Structured Streaming API doesn't allow indexing on streaming Dataframes.
Therefore, the next logical approach would be to perform 2 aggregations on top of each other: first by grouping into slices; second by grouping slices into windows. However, it is not possible to perform multiple streaming aggregations together in spark.

Therefore, the current implementation dumps the partial aggregates of the slices into a Kafka topic (source code in `src/main/scala/slicing/`) and an aggregator program (source code in `src/main/scala/aggregator) performs a final aggregation using the partial aggregates which are read from the aforementioned Kafka topic. Benchmarks for each of the program are assessed separately.

### Result
It can be seen that the overhead of dumping the results into kafka and reading them again is far greater than merely performing the re-computations. Therefore, a better construct needs to be looked into to improve the performance when stream processing is done on overlapping windows.
