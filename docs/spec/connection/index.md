---
layout: page
title: Flowman Connection Specification
permalink: /spec/connection/index.html
---
# Flowman Connections

For some operations it is required to specify *connections* to other systems. Common examples are 
JDBC connections to relation databases or SSH connections to SFTP servers. Flowman provides the
capability to specify generic connection information (like hostname, url, username, password and
so on) inside a `Connection` object which can be reused at multiple places.

This simplifies working with external systems, for example when multiple tables inside a 
relational database are required for reading and/or writing. Using connections, you only need
to specify the generic parameters once. Moreover connections can also be part of profiles,
thereby easily allowing to specify different connection parameters for different environments
(like dev and test).


## Connection Types

* [`jdbc`](jdbc.html): 
Use a [JDBC Connectionb](jdbc.html) to specify database, host and credentials for a JDBC 
connection

* [`ssh` / `sftp`](ssh.html): 
Use a [SSH Connection](ssh.html) to specify connection credentials required for SSH and SFTP
access to remote systems
