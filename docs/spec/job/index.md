---
layout: page
title: Flowman Job & Task Specification
permalink: /spec/job/index.html
---
# Flowman Job & Task Specification

In addition to a completely data centric data flow specification, Flowman also supports so 
called *jobs*, which include specific *tasks* to be executed. A task simply is an action which
is executed, similar to a (simple) batch file.

Common tasks is the execution of one or more data flows via output tasks, data copy or 
upload tasks or simply printing some information onto the console. 

## Defining Jobs

## Available Tasks

Each job consits of one or multiple tasks, which are executed sequentially.

* [`call`](call.html)
* [`compare-files`](compare-files.html)
* [`copy-file`](copy-file.html)
* [`copy-relation`](copy-relation.html)
* [`create-relation`](create-relation.html)
* [`delete-file`](delete-file.html)
* [`describe-mapping`](describe-mapping.html)
* [`describe-relation`](describe-relation.html)
* [`destroy-relation`](destroy-relation.html)
* [`get-file`](get-file.html)
* [`loop`](loop.html)
* [`merge-files`](merge-files.html)
* [`output`](output.html)
* [`print`](print.html)
* [`put-file`](put-file.html)
* [`sftp-upload`](sftp-upload.html)
* [`show-environment`](show-environment.html)
* [`show-mapping`](show-mapping.html)
