---
layout: page
title: Flowman SSH Connection
permalink: /spec/connection/ssh.html
---
# SSH Connections

## Example
```
connections:
  sftp-server:
    kind: sftp
    host: "sftp.server.dimajix.net"
    port: "22"
    username: "testuser"
    password: "12345678"
    keyFile: "/home/user/private_key"
    knownHosts:
```

## Fields

## Description
