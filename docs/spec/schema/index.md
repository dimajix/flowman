---
layout: page
title: Flowman Schemas
permalink: /spec/schema/index.html
---
# Flowman Schema Specification

Flowman uses schema definitions at various places, most commonly where relations (data sources and sinks)
are defined. Flowman does not only support inline schema definitions, but also supports
various external schema definitions like Avro, Swagger and JSON Schema.


## Schema Types

* [`avro`](avro.html): 
Use an [Avro Schema](avro.html) in an external file.

* [`embedded`](embedded.html): 
Use an [ambedded schema](ambedded.html) directly defined inline a Flowman specification file.

* [`json`](json.html): 
Use an [JSON Schema](json.html) in an external file.

* [`mapping`](mapping.html): 
Infer the schema of a [Flowman mapping](mapping.html).

* [`swagger`](swagger.html): 
Use an [Swagger Schema](swagger.html) in an external file.
