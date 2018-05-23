# orientdb-pokec-benchmark

OrientDB benchmarks are running on Pokec database provided by SNAP https://snap.stanford.edu/data/soc-pokec.html .
Following workloads are implemented.

1. `pokecLoad` - Loading of initial data into database.
2. `pokecRead` - Reading of N profiles from database using Zipfian distribution.
3. `pokecUpdate` - Updating of N profiles from database using Zipfian distribution.

At the end of each workload CSV file with statistics is created.
CSV file consist of following columns:
1. Number of operations performed.
2. Avg. time in microseconds of execution of operation, it is calculated for period of time between last report time and current report time, it is not
avg. time for all duration of workload.
3. Throughput of operations, which is calculated again for interval between current and previous reports, not throughput for
all duration of workload.

Last line of CSV file contains information about avg. operation execution time in microseconds and throughput for all duration of benchmark,
 also it contains total amount of operations performed during workload.
Name of CSV file is created using following format: `<name of workload> <data of workload><csv suffix if any>.csv`
All workloads generate single report except of initial load of data. It generates two reports. One for loading of profiles and one
for loading of relations between them.

Database schema consist of two indexes one for id of the persons profile, and one for artificial
 string key which is generated during the load. This artificial key is used then across all workloads as primary key.
Type of index can be chosen during initial load of data.

Following parameters are supported:
1. `embedded` - indicates whether embedded or remote storage is used for benchmark(true of false, true by default).
2. `engineDirectory` - Path to the directory where all embedded databases will be stored (./build/databases by default).
3. `dbName` - Name of the database which will be used for benchmarks (pokec by default).
4. `csvSuffix` - Suffix which is added to any CSV report. Very handy if you want to compare performance of different versions
of product (empty by default).
5. `remoteURL` - URL to the remote server.
6. `numThreads` - Amount of threads to use for workload (8 by default).
7. `indexType` - Type of index is used in pokec benchmark, possible values are: 'tree', 'hash', 'autosharded'.
By default autosharded index is used.
8. `warmUpOperations` - Amount of operations executed during database warmup (`2 * amount of profiles` by default).
9. `operations` - Amount of operations executed during database workload (`4 * amount of profiles` by default).

To pass those parameters following syntax is used `-P<param name>=<param value>`
To run a workload use following syntax `gradle <workload name> <parameters>`.
For example:
1. To initially load data you can use following command `gradle pokecLoad -PcsvSuffix=\(phlogging,tree\) -PindexType=tree`. It will
load data into pokec database and will use hash index for all database indexes. All CSV reports will have suffix `(phloggin, tree)`
at the end.
2. To run update workload you can use following command `gradle pokecUpdate -PcsvSuffix=\(phlogging,tree\)`. It will run workload
which updates content of user profiles and CSV report will have suffix `(phloggin, tree)` at the end.