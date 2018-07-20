---
layout: page
title: Flowman Hive View Relation
permalink: /spec/relation/view.html
---
# Hive View Relations

## Example
```
```

## Fields
 * `kind` **(mandatory)** *(string)*: `view` or `hive-view`
 * `schema` **(optional)** *(schema)* *(default: empty)*: 
 Explicitly specifies the schema of the JDBC source. Alternatively Flowman will automatically
 try to infer the schema.

 * `description` **(optional)** *(string)* *(default: empty)*:
 A description of the relation. This is purely for informational purpose.
 
  * `options` **(optional)** *(map:string)* *(default: empty)*:


## Description
