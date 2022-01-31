[![Codacy Badge](https://api.codacy.com/project/badge/Grade/f0def607de884e2098570eae7b3f3e5e)](https://app.codacy.com/gh/Seagate/s3bench?utm_source=github.com&utm_medium=referral&utm_content=Seagate/s3bench&utm_campaign=Badge_Grade_Settings) [![License](https://img.shields.io/badge/License-MIT-blue.svg)](https://github.com/Seagate/s3bench/blob/main/LICENSE) [![Slack](https://img.shields.io/badge/chat-on%20Slack-blue")](https://join.slack.com/t/cortxcommunity/shared_invite/zt-femhm3zm-yiCs5V9NBxh89a_709FFXQ?) [![YouTube](https://img.shields.io/badge/Video-YouTube-red)](https://cortx.link/videos)


# Initial
Cloned from
```
https://github.com/igneous-systems/s3bench.git
```

# KNOWN LIMITATIONS
* Latest release
  [v2021-06-28](https://github.com/Seagate/s3bench/releases/tag/v2021-06-28)
  is known to have issues with load distribution across multiple endpoints.
  If you need fair load distribution -- use earlier version:
  [v2020-04-09](https://github.com/Seagate/s3bench/releases/tag/v2020-04-09).

# S3 Bench
This tool offers the ability to run very basic throughput benchmarking against
an S3-compatible endpoint. It does a series of put operations followed by a
series of get operations and displays the corresponding statistics. The tool
uses the AWS Go SDK.

## Requirements
* Go

## Installation

Best way is to download one of pre-created builds, see here:
https://github.com/Seagate/s3bench/releases

Second-best way is to build yourself:

* check out source code
* `cd s3bench.git`
* `./build.sh`
  * Build may fail due to missing dependencies.  Follow the on-screen
    instructions to install missing dependencies, then re-run `./build.sh`.

Third way is to build using `go get` approach.

IMPORTANT:

* If you build this way, `--version` will not be able to report actual version
  (will be reporting dash: `-`).
* If you build this way, you don't get `s3bench_hash` binary.

To build this way:

```
go get github.com/Seagate/s3bench
```

The binary will be placed under `$GOPATH/bin/s3bench`.

## Usage
The s3bench command is self-describing. In order to see all the available options
just run s3bench -help.

### Example input
The following will run a benchmark from 2 concurrent clients, which in
aggregate will put a total of 10 unique new objects. Each object will be
exactly 1024 bytes. The objects will be placed in a bucket named loadgen.
The S3 endpoint will be ran against http://endpoint1:80 and
http://endpoint2:80. Object name will be prefixed with loadgen.

```
./s3bench -accessKey=KEY -accessSecret=SECRET -bucket=loadgen \
  -endpoint=http://endpoint1:80,http://endpoint2:80 \
  -numClients=2 -numSamples=10 -objectNamePrefix=loadgen -objectSize=1024
```

#### Note on regions & endpoints
By default, the region used will be `igneous-test` , a fictitious region which
is suitable for using with the Igneous Data Service.  However, you can elect to
use this tool with Amazon S3, in which case you will need to specify the proper region.

It is also important when using Amazon S3 that you specify the proper endpoint, which
will generally be `http://s3-regionName.amazonaws.com:80`. EG: if the bucket which you are
testing is in Oregon, you would specify:

```
-endpoint http://s3-us-west-2.amazonaws.com:80 -region us-west-2
```

For more information on this, please refer to [AmazonS3 documentation.](https://aws.amazon.com/documentation/s3/)



### Example output
The output will consist of details for every request being made as well as the
current average throughput. At the end of the run summaries of the put and get
operations will be displayed.

```
Test parameters
endpoint(s):      [http://endpoint1:80 http://endpoint2:80]
bucket:           loadgen
objectNamePrefix: loadgen
objectSize:       0.0010 MB
numClients:       2
numSamples:       10


Generating in-memory sample data... Done (95.958µs)

Running Write test...
Write operation completed in 0.37s (1/10) - 0.00MB/s
Write operation completed in 0.39s (2/10) - 0.01MB/s
Write operation completed in 0.34s (3/10) - 0.00MB/s
Write operation completed in 0.72s (4/10) - 0.00MB/s
Write operation completed in 0.53s (5/10) - 0.00MB/s
Write operation completed in 0.38s (6/10) - 0.00MB/s
Write operation completed in 0.54s (7/10) - 0.00MB/s
Write operation completed in 0.59s (8/10) - 0.00MB/s
Write operation completed in 0.79s (9/10) - 0.00MB/s
Write operation completed in 0.60s (10/10) - 0.00MB/s

Running Read test...
Read operation completed in 0.00s (1/10) - 0.51MB/s
Read operation completed in 0.00s (2/10) - 1.00MB/s
Read operation completed in 0.00s (3/10) - 0.85MB/s
Read operation completed in 0.00s (4/10) - 1.13MB/s
Read operation completed in 0.00s (5/10) - 1.02MB/s
Read operation completed in 0.00s (6/10) - 1.15MB/s
Read operation completed in 0.00s (7/10) - 1.12MB/s
Read operation completed in 0.00s (8/10) - 1.26MB/s
Read operation completed in 0.00s (9/10) - 1.20MB/s
Read operation completed in 0.00s (10/10) - 1.28MB/s

Test parameters
endpoint(s):      [http://endpoint1:80 http://endpoint2:80]
bucket:           loadgen
objectNamePrefix: loadgen
objectSize:       0.0010 MB
numClients:       2
numSamples:       10

Results Summary for Write Operation(s)
Total Transferred: 0.010 MB
Total Throughput:  0.00 MB/s
Total Duration:    2.684 s
Number of Errors:  0
------------------------------------
Put times Max:       0.791 s
Put times 99th %ile: 0.791 s
Put times 90th %ile: 0.791 s
Put times 75th %ile: 0.601 s
Put times 50th %ile: 0.543 s
Put times 25th %ile: 0.385 s
Put times Min:       0.336 s


Results Summary for Read Operation(s)
Total Transferred: 0.010 MB
Total Throughput:  1.28 MB/s
Total Duration:    0.008 s
Number of Errors:  0
------------------------------------
Put times Max:       0.002 s
Put times 99th %ile: 0.002 s
Put times 90th %ile: 0.002 s
Put times 75th %ile: 0.002 s
Put times 50th %ile: 0.001 s
Put times 25th %ile: 0.001 s
Put times Min:       0.001 s
```

##### Head-object
It is possible to send head-object requests instead of get-object.
For this purpose one sould use *-metaData* flag

```
./s3bench -accessKey=KEY -accessSecret=SECRET -bucket=loadgen \
  -endpoint=http://endpoint1:80 \
  -numClients=2 -numSamples=10 -objectNamePrefix=loadgen -objectSize=1024 \
  -metaData
```
