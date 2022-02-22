# MS SQL Server Plugin

The MS SQL Server plugin provides a JDBC driver  to access MS SQL Server and Azure SQL Server databases via 
the [JDBC relation](../spec/relation/jdbc.md). Moreover, it also provides a specialized  
[`sqlserver` relation](../spec/relation/sqlserver.md) which uses bulk copy to speed up writing process and it
also uses temp tables to encapsulate the whole data upload within a transaction.


## Provided Entities
* [`sqlserver` relation](../spec/relation/sqlserver.md)


## Activation

The plugin can be easily activated by adding the following section to the [default-namespace.yml](../spec/namespace.md)
```yaml
plugins:
  - flowman-mssqlserver 
```
