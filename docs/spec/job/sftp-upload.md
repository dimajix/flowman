---
layout: page
title: Flowman SFTP Upload Task
permalink: /spec/job/sftp-upload.html
---
# SFTP Upload Task
The SFTP upload task is used for uploading data which resides either on the local machine or
in a Hadoop compatible filesystem (HDFS, S3, ...) to an external SFTP server.

## Example
```
jobs:
  main:
    tasks:
      - kind: sftp-upload
        description: "Upload some data to a SFTP server"
        connection: my-sftp-connection
        source: "$hdfs_export_basedir/export_table/${processing_date}"
        target: "${sftp_target}/inbox/_${processing_date}.txt"
        merge: true
        overwrite: true
```

## Fields
* `kind` **(mandatory)** *(type: string)*: `sftp-upload`
* `description` **(optional)** *(type: string)*: 
A textual description of the task.

* `source` **(mandatory)** *(type: string)*:
Specifies the source location in the Hadoop compatible filesystem. This may be either a single
file or a whole directory.

* `target` **(mandatory)** *(type: string)*:
Specifies the target location on the SFTP server. This may be either a single file or a whole 
directory.

* `connection` **(mandatory)** *(type: string)*:
Specifies the name of the connection to use when connecting to the SFTP server.

* `merge` **(optional)** *(type: boolean)* *(default: false)*:
Flowman can merge multiple files in the `source` directory into a single output file. This is
useful when working with Hadoop or Spark outputs which often consist of multiple files. Per
default this is set to `false`, meaning that multiple files will be transferred individually.

* `delimiter` **(optional)** *(type: string)* *(default: empty)*:
Specifies an optional delimiter, which should be used between files when they are concatenated
when `merge` is set to `true`. Has no effect otherwise.

* `overwrite` **(optional)** *(type: boolean)* *(default: true)*:
Set to `true` in order to overwrite existing files on the SFTP server. Otherwise an existing
file will result in an error.

## Description
