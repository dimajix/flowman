# Securely providing Secrets

Most Flowman projects need to provide some sort of secrets (database credentials, S3 secrets, ...) to external
systems for being able to access them. This opens the question about a best practice how to provide those secrets
to a Flowman project securely.

In order to provide secrets to Flowman, there are two ways:
1. Provide secrets via system environment variables, which then can be accessed in Flowman
2. Provide secrets stored in an external file, which then can be read by Flowman

Both approaches will extract the secret and store it in a Flowman environment variable. To prevent Flowman from
printing the secret onto the console, it is enough when the variable name ends with "credential", "secret" or 
"password". In this case, Flowman will redact the value when logging the environment onto the console.


## Using environment variables

By using the `$System.getenv` template function, you can access environment variables within Flowman. For example
for extracting database credentials from system environment variables, you could proceed as follows:

```yaml
environment:
  - frontend_db_driver=$System.getenv('MYSQL_DRIVER', 'com.mysql.cj.jdbc.Driver')
  - frontend_db_url=$System.getenv('MYSQL_URL')
  - frontend_db_username=$System.getenv('MYSQL_USERNAME')
  - frontend_db_password=$System.getenv('MYSQL_PASSWORD')

connections:
  frontend:
    kind: jdbc
    driver: "$frontend_db_driver"
    url: "$frontend_db_url"
    username: "$frontend_db_username"
    password: "$frontend_db_password"

relations:
  advertiser_setting:
    kind: jdbcTable
    connection: frontend
    table: "advertiser_setting"
    schema:
      kind: inline
      fields:
        - name: id
          type: Integer
        - name: business_rule_id
          type: Integer
        - name: rtb_advertiser_id
          type: Integer
```

Then you only need to populate the system environment variables `MYSQL_DRIVER`, `MYSQL_URL`, `MYSQL_USERNAME` and 
`MYSQL_PASSWORD` before starting Flowman. This approach fits well to deployments where Flowman runs inside Kubernetes,
but can also be used in many other scenarios.


## Using external files

Another viable approach is to provide secrets via files. Flowman can read any file via the template function
`$File.read`, which will simply return the files content. Moreover, by using the template function `$JSON.path` you
can parse any JSON content. An example might look as follows:

```yaml
environment:
  - sql_host=jdbc:sqlserver://$JSON.path($File.read($System.getenv('MSSQLSERVER_SECRETS')), '$.secret[?(@.key=="sql-host-name")].value')
  - sql_username=$JSON.path($File.read($System.getenv('MSSQLSERVER_SECRETS')), '$.secret[?(@.key=="sql-db-username")].value')
  - sql_password=$JSON.path($File.read($System.getenv('MSSQLSERVER_SECRETS')), '$.secret[?(@.key=="sql-db-password")].value')
  - sql_database=$JSON.path($File.read($System.getenv('MSSQLSERVER_SECRETS')), '$.secret[?(@.key=="sql-db-name")].value')

connections:
  datahub:
    kind: jdbc
    driver: "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    url: jdbc:sqlserver://$sql_host
    username: $sql_username
    password: $sql_password
    properties:
      databaseName: $sql_database
```
Then you need to store the secrets in a JSON file stored on the local file system where Flowman is executed. Using the
JSON paths as defined above, the file needs to look as follows:
```json
{
  "secret": [
    {
      "key": "sql-db-username",
      "value": "my-sql-username"
    },
    {
      "key": "sql-db-password",
      "value": "my-secret-password"
    },
    {
      "key": "sql-db-name",
      "value": "MY_DATABASE"
    },
    {
      "key": "sql-host-name",
      "value": "sql-database.windows.net:1433"
    }
  ]
}
```
Finally, you need to set the system environment variable `MSSQLSERVER_SECRETS` to point to the corresponding file before
starting Flowman. This additional indirection is not strictly required, but helps to decouple Flowman configuration
from the deployment logic.


## Using Key Vaults

Another alternative for storing and retrieving secrets is to use key vaults. These are often provided by cloud 
environments. Flowman currently supports Azure Key Vault to access secrets. You will need to activate the
[Azure Plugin](../plugins/azure.md) in order to use this feature in Flowman. Once enabled, you can easily access
secrets as follows:
```yaml
environment:
  - sql_host=$AzureKeyVault.getSecret('datahub', 'sql_host')
  - sql_username=$AzureKeyVault.getSecret('datahub', 'sql_username')
  - sql_password=$AzureKeyVault.getSecret('datahub', 'sql_password')
  - sql_database=$AzureKeyVault.getSecret('datahub', 'sql_database')

connections:
  datahub:
    kind: jdbc
    driver: "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    url: jdbc:sqlserver://$sql_host
    username: $sql_username
    password: $sql_password
    properties:
      databaseName: $sql_database
```
