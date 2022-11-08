# Impala Metadata

Impala is another "SQL on Hadoop" execution engine mainly developed and backed up by Cloudera. Impala allows you to
access data stored in Hadoop and registered in the Hive metastore, just like Hive itself, but often at a significantly
better performance. Unfortunately Impala requires that all changes to Hive tables are either performed directly via
Impala itself or that the changed tables are synced from Hive to Impala after changes.

In order to better support environments which rely on Impala, Flowman also supports the second approach of automatically
syncing all changes to Hive tables performed in Flowman. This feature is accomplished by the "Impala" plugin for
Flowman. The plugin only needs to be enabled and properly configured, and then all changes to Hive will be 
automatically propagated to Impala


## Configuration of Impala Plugin

First you need to enable the Impala plugin. This needs to be done in the `system.yml` configuration file in the
`conf` directory of Flowman:
```
# The system configuration loads plugin before namespaces are instantiated. The Impala plugin may already be required
# within a namespace to define an external catalog, therefore it needs to be loaded in advance.
plugins:
  - flowman-impala
```

Next you need to configure the Impala plugin as an "external catalog provider" within the namespace configuration file.
The default namespace is configured via `conf/default-namespace.yml`. You need to add the following sections:
```
# Define the connection to Impala
connections:
  impala:
    kind: jdbc
    url: jdbc:impala://IMPALA_HOST:21050
    properties:
      SocketTimeout: 0

# Setup Impala as an additional catalog besides Hive
catalog:
  kind: impala
  connection: impala
```
Please consult the Cloudera Impala documentation for a comprehensive list of JDBC properties, which can be set under
`properties` in the connection above.
 

## Using Impala with Kerberos

When you are using Impala with Kerberos authentication, you also need to specify some additional details (please
consult the Cloudera Impala documentation for a comprehensive list of JDBC properties):
```
# Define the connection to Impala with Kerberos enabled
connections:
  impala:
    kind: jdbc
    url: jdbc:impala://IMPALA_HOST:21050
    properties:
      SocketTimeout: 0
      AuthMech: 1
      AuthType: 1
      KrbRealm: MY.KERBEROS.REALM
      KrbHostFQDN: IMPALA_HOST
      KrbServiceName: impala
      AllowSelfSignedCerts: 1
      CAIssuedCertsMismatch: 1
      SSL: 1
```

In addition to the namespace configuration, you also need to provide valid Kerberos credentials stored in a keytab.
Impala then in turn requires a valid JAAS configuration file, which refers to that keytab. That file may look as
follows (of course you need to replace `KRB_PRINCIPAL` and `MY.KERBEROS.REALM`:
```
Client {
  com.sun.security.auth.module.Krb5LoginModule required
  useKeyTab=true
  keyTab="conf/KRB_PRINCIPAL.keytab"
  useTicketCache=true
  principal="KRB_PRINCIPAL@MY.KERBEROS.REALM"
  doNotPrompt=true
  debug=false;
};
```
Finally you need to tell Flowman to read in this JAAS file. This can be done by specifing a Java command line option
in `conf/flowman-env.sh` as follows:
```
SPARK_DRIVER_JAVA_OPTS="-Djava.security.auth.login.config=$FLOWMAN_CONF_DIR/jaas.conf"
``` 
