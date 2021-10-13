# Impala Plugin

The `impala` plugin is responsible for executing `REFRESH` and `COMPUTE STATS` commands on Hive table that are created
or written to by Flowman.

## Provided Entities
* `impala` catalog

## Example
In order to be able to use the Impala catalog plugin, you have to add it to the `system.yml` definition as follows:
```yaml
# system.yml

# We need to specify the impala plugin as a system plugin, since it is required to instantiate a namespace
plugins:
  - flowman-impala
```

Then you have to configure the catalog in `default-namespace.yml` similar to the following code snippet, which also uses
Kerberos for authentication. Note that for using Kerberos with Impala, you actually also need a `jass.conf` file. Other
authentication mechanisms will require different properties - please consult the Impala documentation for more details.
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

config:
  # Enable COMPUTE STATS (already enabled by default)
  - flowman.impala.computeStats=true
```

You can disable the statistics computation (`COMPUTE STATS`) which is normally also performed by the plugin by
setting the following configuration variable:

- `flowman.impala.computeStats` *(type: boolean)* *(default:true)*
  If enabled (i.e. set to `true`), then Flowman will perform a `COMPUTE STATS` within the
  [Impala Catalog plugin](impala.md) whenever a Hive table is updated. The `REFRESH` statements will always
  be executed by the plugin.
