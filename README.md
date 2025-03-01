## Installation

#### Mac OS X

**Install scala-2.12.1 and sbt**

- Install with [Homebrew](https://brew.sh/): \
\
`brew install scala`\
`brew install sbt`

**Download and Install hadoop-2.7.3**

Follow instructions in [Hadoop](https://hadoop.apache.org).

**Download and Install spark-2.1.0-bin-hadoop2.7**

Download and install Spark version 2.1.0 pre-built for Hadoop 2.7 and later from [this link](http://spark.apache.org/downloads.html).

**Download and Install kafka-0.10.2.0**

Use [kafka quick start documentation](http://kafka.apache.org/quickstart) to download and start zookeeper and kafka servers

## Quick Start

Import project into IntelliJ IDEA and install sbt packages.

Create new app at [Twitter Apps](https://apps.twitter.com/) and put consumer key, consumer secret, access key, and access secret in the application.conf.

Run the code using either of the following main functions:
- **DistributedLanguageDetection** for running distributed k-means algorithm
- **DistributedLanguageDetection** for getting twitter data from kafka and running Streaming k-means algorithm
- **CommonNgrams** for preprocessing tweets and find common ngrams in each language
