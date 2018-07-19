---
layout: page
title: Flowman JDBC Relation
permalink: /spec/relation/jdbc.html
---
# JDBC Relations

## Example
```
```

## Fields
 * `kind` **(mandatory)** *(type: string)*: `jdbc`
 * `schema` **(optional)** *(type: schema)* *(default: empty)*: 
 Explicitly specifies the schema of the JDBC source. Alternatively Flowman will automatically
 try to infer the schema.
 
 * `description` **(optional)** *(type: string)* *(default: empty)*:
 A description of the relation. This is purely for informational purpose.
 
 * `options` **(optional)** *(type: map:string)* *(default: empty)*:
 All key-value pairs specified in *options* are directly passed to Apache spark for reading
 and/or writing to this relation.

 * `connection` **(mandatory)** *(type: string)*:
 The *connection* field specifies the name of a [Connection](../connection/index.html)
 object which has to be defined elsewhere.
 
 * `database` **(optional)** *(type: string)* *(default: empty)*: 
 Defines the Hive database where the table is defined. When no database is specified, the
 table is accessed without any specific qualification, meaning that the default database
 will be used or the one specified in the connection.

 * `table` **(mandatory)** *(type: string)*:
 Specifies the name of the table in the relational database.
  
 * `properties` **(optional)** *(type: map:string)* *(default: empty)*:
 Specifies any additional properties passed to the JDBC connection.  Note that both the JDBC
 relation and the JDBC connection can define properties. So it is advisable to define all
 common properties in the connection and more table specific properties in the relation.
 The connection properties are applied first, then the relation properties. This means that
 a relation property can overwrite a connection property if it has the same name.


## Description
