package com.dimajix.dataflow.spec.task

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo

import com.dimajix.dataflow.execution.Executor
import com.dimajix.dataflow.spec.Project


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "shell", value = classOf[ShellTask]),
    new JsonSubTypes.Type(name = "output", value = classOf[OutputTask]),
    new JsonSubTypes.Type(name = "loop", value = classOf[LoopTask])
))
abstract class Task {
    /**
      * Abstract method which will perform the output operation. All required tables need to be
      * registered as temporary tables in the Spark session before calling the execute method.
      *
      * @param executor
      */
    def execute(executor:Executor) : Unit
}
