# Setup & Configuration

Installing Flowman is relatively simple once you have a working Apache Spark environment, like Cloudera CDP. But even 
without a Hadoop/Spark environment, you can resort to prebuilt Docker images for running Flowman in a non-distributed 
local mode which is especially useful for development.

Moreover, managed Spark environments like AWS EMR or Azure Synapse are supported, you will find instructions below:

* [Installation Guide](installation.md)
* [Running Flowman in Docker](docker.md)
* [Running Flowman with Windows](windows.md)
* [Running Flowman in AWS EMR](emr.md)
* [Running Flowman in Azure Synapse](synapse.md)
* [Configuration Properties](config.md)
* [Building Flowman from Source](building.md)


## Supported Spark Environments
Flowman is available for a large number of different Spark/Hadoop environments. Flowman provides different packages
for each of these environments to ensure a high degree of compatibility. Each variant is identified by its suffix
appended to the Flowman version, i.e. `<flowman-version>-<flowman-variant>`. So for example the full version tag
of Flowman 0.30.0 for Cloudera CDP 7.1 and Spark 3.3 would be `0.30.0-cdp7-spark3.3-hadoop3.1`.

The following environments are officially supported with corresponding build variants:

| Distribution     | Spark | Hadoop | Java | Scala | Variant                       |
|------------------|-------|--------|------|-------|-------------------------------|
| Open Source      | 2.4.8 | 2.6    | 1.8  | 2.11  | oss-spark2.4-hadoop2.6        |
| Open Source      | 2.4.8 | 2.7    | 1.8  | 2.11  | oss-spark2.4-hadoop2.7        |
| Open Source      | 3.0.3 | 2.7    | 11   | 2.12  | oss-spark3.0-hadoop2.7        |
| Open Source      | 3.0.3 | 3.2    | 11   | 2.12  | oss-spark3.0-hadoop3.2        |
| Open Source      | 3.1.2 | 2.7    | 11   | 2.12  | oss-spark3.1-hadoop2.7        |
| Open Source      | 3.1.2 | 3.2    | 11   | 2.12  | oss-spark3.1-hadoop3.2        |
| Open Source      | 3.2.3 | 2.7    | 11   | 2.12  | oss-spark3.2-hadoop2.7        |
| Open Source      | 3.2.3 | 3.3    | 11   | 2.12  | oss-spark3.2-hadoop3.3        |
| Open Source      | 3.3.2 | 2.7    | 11   | 2.12  | oss-spark3.3-hadoop2.7        |
| Open Source      | 3.3.2 | 3.3    | 11   | 2.12  | oss-spark3.3-hadoop3.3        |
| AWS EMR 6.10     | 3.3.1 | 3.3    | 1.8  | 2.12  | emr6.10-spark3.3-hadoop3.3    |
| Azure Synapse    | 3.3.1 | 3.3    | 1.8  | 2.12  | synapse3.3-spark3.3-hadoop3.3 |
| Cloudera CDH 6.3 | 2.4.0 | 3.0    | 1.8  | 2.11  | cdh6-spark2.4-hadoop3.0       |
| Cloudera CDP 7.1 | 2.4.8 | 3.1    | 1.8  | 2.11  | cdp7-spark2.4-hadoop3.1       |
| Cloudera CDP 7.1 | 3.2.1 | 3.1    | 11   | 2.12  | cdp7-spark3.2-hadoop3.1       |
| Cloudera CDP 7.1 | 3.3.0 | 3.1    | 11   | 2.12  | cdp7-spark3.3-hadoop3.1       |
