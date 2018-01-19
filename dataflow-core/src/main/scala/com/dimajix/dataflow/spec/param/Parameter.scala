package com.dimajix.dataflow.spec.param

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo

import com.dimajix.dataflow.execution.Context


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "int", value = classOf[IntegerParameter]),
    new JsonSubTypes.Type(name = "float", value = classOf[FloatParameter]),
    new JsonSubTypes.Type(name = "string", value = classOf[StringParameter]),
    new JsonSubTypes.Type(name = "datetime", value = classOf[DateTimeParameter]),
    new JsonSubTypes.Type(name = "duration", value = classOf[DurationParameter]))
)
abstract class Parameter {
    def default(implicit context: Context) : String
    def values(argument:String)(implicit context:Context) : Iterable[String]
}
