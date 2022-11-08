# Kerberos Authentication

Of course, you can also run Flowman in a Kerberos environment, as long as the components you use actually support
Kerberos. This includes Spark, Hadoop and Kafka.

## Configuring Kerberos

The simplest way to use Kerberos is to provide a customized `flowman-env.sh` in the `conf` directory. You simply
need to set the following variables and provide a Kerberos keytab at the correct location.
```bash
# flowman-env.sh

KRB_PRINCIPAL={{KRB_PRINCIPAL}}@MY-REALM.NET
KRB_KEYTAB=$FLOWMAN_CONF_DIR/{{KRB_PRINCIPAL}}.keytab
```

Of course this way, Flowman will always use the same Kerberos principal for all projects. Currently, there is no other
way, since Spark and Hadoop need to have the Kerberos principal set at startup. But you can simply use different
config directories and switch between them by setting the `FLOWMAN_CONF_DIR` environment variable.


## Impala Catalog Plugin
When you want to use the [Impala Plugin](../plugins/impala.md) with Kerberos authentication, then things get a little
bit more complicated, since you also need to specify a `JAAS` file.
```yaml
# system.yml

# We need to specify the impala plugin as a system plugin, since it is required to instantiate a namespace
plugins:
  - flowman-impala
```

```yaml
# default-namespace.yml

# Define the connection to Impala
connections:
  impala:
    kind: jdbc
    url: jdbc:impala://$System.getenv('IMPALA_HOST'):21050
    properties:
      SocketTimeout: 0
      AuthMech: 1
      AuthType: 1
      KrbRealm: MY-KERBEROS-REALM.NET
      KrbHostFQDN: $System.getenv('IMPALA_HOST')
      KrbServiceName: impala
      AllowSelfSignedCerts: 1
      CAIssuedCertsMismatch: 1
      SSL: 1
      
# Setup Impala as an additional catalog besides Hive
catalog:
  kind: impala
  connection: impala
```

```bash
# flowman-env.sh

SPARK_DRIVER_JAVA_OPTS="-Djava.security.auth.login.config=jaas.conf"
```

```text
Client {
  com.sun.security.auth.module.Krb5LoginModule required
  useKeyTab=true
  keyTab=/path/to/{{KRB_PRINCIPAL}}.keytab
  useTicketCache=true
  principal={{KRB_PRINCIPAL}}@MY-REALM.NET
  doNotPrompt=true
  debug=false;
};
```
