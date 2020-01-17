
# Flowman Schema Specification

Flowman uses schema definitions at various places, most commonly where relations (data sources and sinks)
are defined. Flowman does not only support inline schema definitions, but also supports
various external schema definitions like Avro, Swagger and JSON Schema.


## Schema Types

* [`avro`](avro.md): 
Use an [Avro Schema](avro.md) in an external file.

* [`embedded`](embedded.md): 
Use an [ambedded schema](ambedded.md) directly defined inline a Flowman specification file.

* [`json`](json.md): 
Use an [JSON Schema](json.md) in an external file.

* [`mapping`](mapping.md): 
Infer the schema of a [Flowman mapping](mapping.md).

* [`swagger`](swagger.md): 
Use an [Swagger Schema](swagger.md) in an external file.
