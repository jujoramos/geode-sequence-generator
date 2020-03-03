[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0) 

# GEODE Sequence Generator
1. [Overview](#overview)
2. [Building From Source](#building)
3. [Usage](#usage)

_This project used as the starting point the tool [`geode-single-32bit-sequence`](https://github.com/charliemblack/geode-single-32bit-sequence),
developed by Charlie Black some years ago._

## <a name="overview"></a>Overview

[Apache Geode](http://geode.apache.org/) is a data management platform that provides real-time, 
consistent access to data-intensive applications throughout widely distributed cloud architectures.

Sometimes our applications need to use sequential IDs, generating these IDs in distributed systems 
is generally cumbersome and error-prone.

This project is a simple [Apache Geode](http://geode.apache.org/) client wrapper that allows 
applications to use distributed counters entirely stored and managed through a running 
[Apache Geode](http://geode.apache.org/) cluster.

In order to achieve uniqueness and make sure no clients receive the same ID, the project uses
[Geode Functions](https://geode.apache.org/docs/guide/111/developing/function_exec/chapter_overview.html)
and the [Distributed Lock Service](https://geode.apache.org/docs/guide/111/developing/distributed_regions/locking_in_global_regions.html).  
Long story short, every time a sequence should be computed, the client automatically executes a 
`Function` on the server hosting the primary data for the required `sequenceId` and acquires the 
respective lock to make sure no other clients can get the same ID.

The above, of course, will generate contention on the server(s) computing the IDs, so it's always
advisable to retrieve the IDs in batches from the clients and work with those until a new ID is 
required.

## <a name="building"></a>Building From Source

All platforms require a Java installation with JDK 1.8 or more recent version. The JAVA\_HOME 
environment variable can be set as below:

| Platform | Command |
| :---: | --- |
|  Unix    | ``export JAVA_HOME=/usr/java/jdk1.8.0_121``            |
|  OSX     | ``export JAVA_HOME=/usr/libexec/java_home -v 1.8``     |
|  Windows | ``set JAVA_HOME="C:\Program Files\Java\jdk1.8.0_121"`` |

Clone the current repository in your local environment and, within the directory containing the 
source code, run gradle build:
```
$ ./gradlew build
```

## <a name="usage"></a>Usage

### Server Side

Build the tool and deploy it to a running [Apache Geode](http://geode.apache.org/) cluster using the
[gfsh deploy](https://geode.apache.org/docs/guide/111/tools_modules/gfsh/command-pages/deploy.html) command.

### Client Application

Initialize the `geode-sequence-generator` by invoking the `DistributedSequenceFactory.initialize()` 
method. By default the tool will use a `PARTITION_PERSISTENT` region to store and compute the sequences, 
but you can further customize the region type and which specific servers will act as the sequence service
layer by using other `initalize` methods from the `DistributedSequenceFactory` class.
```
DistributedSequenceFactory.initialize(clientcache);
```

Acquire your sequence through the `DistributedSequenceFactory.getSequence(sequenceId)` method.
```
DistributedSequence ticketsSequence = DistributedSequenceFactory.getSequence("Tickets");
```

Use the returned `DistributedSequence` instance to retrieve/compute your sequences.
```
// Get Current Value
Long currentValue = ticketsSequence.get();

// Increment And Get Current Value 
Long sequencePlusOne = ticketsSequence.incrementAndGet();

// Get Next Batch of Unique Sequences
List<Long> sequences = ticketsSequence.nextBatch(1000);
```
