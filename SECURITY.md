# Security Policy

Flowman takes security very serious, and tries to address any security issues as soon as possible.

Since Flowman relies on a provided installation of Apache Spark, you should be aware that Flowman implicitly
inherits all security issues of the libraries used from its environment. For example Flowman will use Spark, 
Jackson and several Apache Commons libraries provided by its runtime environment. This also means that most
(if not all) open security issues reported on GitHub are related to libraries provided by your runtime environment
(for example Cloudera, AWS EMR, ...) and need to be addressed independently.


## Supported Versions

Generally, only the latest Flowman version is currently supported.

| Version | Supported          |
| ------- | ------------------ |
| 1.0.x   | :white_check_mark: |
| < 1.0   | :x:                |


## Reporting a Vulnerability

Please feel free to report any security vulnerability to info@flowman.io or create an issue on GitHub at
https://github.com/dimajix/flowman
