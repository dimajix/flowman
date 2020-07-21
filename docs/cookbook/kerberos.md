# Kerberos

Of course you can also run Flowman in a Kerberos environment, as long as the components you use actually support
Kerberos. This includes Spark, Hadoop and Kafka.

## Configuring Kerberos

The simplest way to use Kerberos is to provide a customized `flowman-env.sh` in the `conf` directory. You simply
need to set the following variables and provide a Kerberos keytab at the correct location.
```bash
KRB_PRINCIPAL={{KRB_PRINCIPAL}}@MY-REALM.NET
KRB_KEYTAB=$FLOWMAN_CONF_DIR/{{KRB_PRINCIPAL}}.keytab
```

Of course this way, Flowman will always use the same Kerberos principal for all projects. Currently there is no other
way, since Spark and Hadoop need to have the Kerberos principal set at startup. But you can simply use different
config directories and switch between them by setting the `FLOWMAN_CONF_DIR` environment variable.
