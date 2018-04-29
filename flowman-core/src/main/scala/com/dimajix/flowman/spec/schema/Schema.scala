package com.dimajix.flowman.spec.schema

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo

import com.dimajix.flowman.execution.Context


/**
  * Interface class for declaring relations (for sources and sinks) as part of a model
  */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = classOf[EmbeddedSchema])
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "inline", value = classOf[EmbeddedSchema]),
    new JsonSubTypes.Type(name = "embedded", value = classOf[EmbeddedSchema])
))
abstract class Schema {
    def description(implicit context: Context) : String
    def fields(implicit context: Context) : Seq[Field]
}
