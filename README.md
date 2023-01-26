# [![Flowman Logo](docs/images/flowman-favicon.png) Flowman](https://flowman.io)
The declarative data build tool based on Apache Spark.

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Documentation](https://readthedocs.org/projects/flowman/badge/?version=latest)](https://flowman.readthedocs.io/en/latest/)


## 🤔 What is Flowman?

Flowman is a *data build tool* based on [Apache Spark](https://spark.apache.org) that simplifies the act of 
implementing data transformation logic as part of complex data pipelines. Flowman follows a strict "everything-as-code"
approach, where the whole transformation logic is specified in purely declarative YAML files.
These describe all details of the data sources, sinks and data transformations. This is much simpler and efficient
than writing Spark jobs in Scala or Python. Flowman will take care of all the technical details of a correct and robust 
implementation and the developers can concentrate on the data transformations themselves.

In addition to writing and executing data transformations, Flowman can also be used for managing physical data models, 
i.e. Hive or SQL tables. Flowman can create such tables from a specification with the correct schema and also 
automatically perform migrations. This helps to 
keep all aspects (like transformations and schema information) in a single place managed by a single tool.

[![Flowman Diagram](docs/images/flowman-overview.png)](https://flowman.io)


## 💪 Noteable Features

* Semantics of a build tool like Maven — just for data instead for applications
* Declarative syntax in YAML files
* Data model management (Create, Migrate and Destroy Hive tables, JDBC tables or file based storage)
* Generation of meaningful data model documentation 
* Flexible expression language for parametrizing a project for different environments (DEV, TEST, PROD)
* Jobs for managing build targets (like copying files or uploading data via sftp)
* Automatic data dependency management within the execution of individual jobs
* Meaningful logging output & rich set of execution metrics
* Powerful yet simple command line tools
* Extendable via Plugins


## 💾 Supported Data Sources and Sinks
Flowman supports a wide range of data sources, for example
* Various cloud blob storages (S3, ABS, ...)
* Relational databases (Postgres, Azure SQL, MS SQL Server, MariaDB, ...)
* Hadoop (HDFS & Hive)
* Streaming sources (Kafka)

For file-based sources and sinks, Flowman supports commonly used file formats like CSV, JSON, Parquet and much more. 
The official documentation provides an overview of
[supported connectors](https://docs.flowman.io/en/latest/connectors/index.html).


## 📚 Documentation

You can find the official homepage at [Flowman.io](https://flowman.io)
 and a comprehensive documentation at [Read the Docs](https://docs.flowman.io). 


## 🤓 How do I use Flowman?

### 1. Install Flowman

You can set up Flowman by following our step-by-step instructions for [local installations](https://docs.flowman.io/en/latest/setup/installation.html)
or by starting a [Docker container](https://docs.flowman.io/en/latest/setup/docker.html)

### 2. Create a Project

Flowman will provide some example projects in the `examples` subdirectory, which you can use as a starting point.

### 3. Execute the Project

You can execute the project interactively by starting the [Flowman Shell](https://docs.flowman.io/en/latest/cli/flowshell/index.html)


## 🚀 Installation

You simply grab an appropriate pre-build package at [GitHub](https://github.com/dimajix/flowman/releases),
or you can use a Docker image, which is available at [Docker Hub](https://hub.docker.com/repository/docker/dimajix/flowman).
More details are described in the [Quickstart Guide](QUICKSTART.md) or in the [official Flowman documentation](https://docs.flowman.io/en/latest/setup/index.html).


## 🏗 Building

You can build your own Flowman version via Maven with
```shell
mvn clean install
```
Please also read [BUILDING.md](BUILDING.md) for detailed instructions, specifically on build profiles.


## 💙 Community

- [Slack](https://join.slack.com/t/flowman-io/shared_invite/zt-168ltudzp-52cCI1S69OMh7sSajtp~7A): Message us on Slack


## 😍 Contributing

You want to contribute to Flowman? Welcome! Please read [CONTRIBUTING.md](CONTRIBUTING.md) to understand how you can 
contribute to the project.

* [GitHub issue tracker for Flowman](https://github.com/dimajix/flowman/issues/new) to report feature requests or bugs.


## 📄 License

This project is licensed under Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
