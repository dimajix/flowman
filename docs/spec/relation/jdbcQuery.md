# JDBC Query Relations

The `jdbcQuery` relation allows you to access databases using a JDBC driver. Note that you need to put an appropriate JDBC
driver onto the classpath of Flowman. This can be done by using an appropriate plugin.


## Example

```yaml
# First specify a connection. This can be used by multiple JDBC relations
connections:
  frontend:
    kind: jdbc
    driver: "$frontend_db_driver"
    url: "$frontend_db_url"
    username: "$frontend_db_username"
    password: "$frontend_db_password"

relations:
  lineitem:
    kind: jdbcQuery
    connection: frontend
    query: "
      SELECT
        CONCAT('DIR_', li.id) AS lineitem,
        li.campaign_id AS campaign,
        IF(c.demand_type_system = 1, 'S', IF(li.demand_type_system = 1, 'S', 'D')) AS demand_type
      FROM
        line_item AS li
      INNER JOIN
        campaign c
        ON c.id = li.campaign_id
    "
    schema:
      kind: embedded
      fields:
        - name: lineitem
          type: string
        - name: campaign
          type: long
        - name: demand_type
          type: string
```
It is also possible to directly embed the connection as follows:
```yaml
relations:
  frontend_users:
    kind: jdbcQuery
    # Specify the name of the connection to use
    connection:
      kind: jdbc
      driver: "$frontend_db_driver"
      url: "$frontend_db_url"
      username: "$frontend_db_username"
      password: "$frontend_db_password"
    query: "
      SELECT
        CONCAT('DIR_', li.id) AS lineitem,
        li.campaign_id AS campaign,
        IF(c.demand_type_system = 1, 'S', IF(li.demand_type_system = 1, 'S', 'D')) AS demand_type
      FROM
        line_item AS li
      INNER JOIN
        campaign c
        ON c.id = li.campaign_id
    "
```
For most cases, it is recommended not to embed the connection, since this prevents reusing the same connection in
multiple places.

The schema is still optional in this case, but it will help [mocking](mock.md) the relation for unittests.


## Fields
 * `kind` **(mandatory)** *(type: string)*: `jdbcQuery`
   
 * `schema` **(optional)** *(type: schema)* *(default: empty)*: 
 Explicitly specifies the schema of the JDBC source. Alternatively Flowman will automatically  try to infer the schema.

 * `description` **(optional)** *(type: string)* *(default: empty)*:
 A description of the relation. This is purely for informational purpose.
 
 * `connection` **(mandatory)** *(type: string)*:
 The *connection* field specifies the name of a [Connection](../connection/index.md)  object which has to be defined 
elsewhere.

 * `query` **(optional)** *(type: string)*:
This property specifies the SQL query which will be executed by the
database for retrieving data. Of course, then only read operations are possible.
  
 * `properties` **(optional)** *(type: map:string)* *(default: empty)*:
 Specifies any additional properties passed to the JDBC connection.  Note that both the JDBC
 relation and the JDBC connection can define properties. So it is advisable to define all
 common properties in the connection and more table specific properties in the relation.
 The connection properties are applied first, then the relation properties. This means that
 a relation property can overwrite a connection property if it has the same name.


## Schema Conversion
The JDBC relation fully supports automatic schema conversion on input and output operations as described in the
corresponding section of [relations](index.md).

## Output Modes
The `jdbcQuery` relation does not support write operations.

## Remarks

### Mocking JDBC relations
Note that Flowman will rely on schema inference in some important situations, like [mocking](mock.md) and generally
for describing the schema of a relation. This might create unwanted connections to the physical data source,
particular in case of self-contained tests. To prevent Flowman from creating a connection to the physical data 
source, you simply need to explicitly specify a schema, which will then be used instead of the physical schema 
in all situations where only schema information is required.
