# SFTP Plugin

The SFTP plugin provides a [`sftpUpload` target](../spec/target/sftpUpload.md).


## Activation

The plugin can be easily activated by adding the following section to the [default-namespace.yml](../spec/namespace.md)
```yaml
plugins:
  - flowman-sftp 
```


## Usage
In order to upload some files to SFTP server, you need to define a `ssh` connection and an `sftpUpload` target as in
the following example.
```yaml
connections:
  my-sftp-connection:
    kind: ssh
    host: "sftp.server.dimajix.net"
    port: "22"
    username: "testuser"
    password: "12345678"
    keyFile: "/home/user/private_key"

jobs:
  main:
    upload_data:
      kind: sftUpload
      description: "Upload some data to a SFTP server"
      connection: my-sftp-connection
      source: "$hdfs_export_basedir/export_table/${processing_date}"
      target: "${sftp_target}/inbox/_${processing_date}.txt"
      merge: true
      overwrite: true
```
Please find more information in the documentation of the [`sftpUpload` target](../spec/target/sftpUpload.md).
