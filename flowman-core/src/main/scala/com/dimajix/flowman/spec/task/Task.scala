package com.dimajix.flowman.spec.task

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo

import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.Project


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "shell", value = classOf[ShellTask]),
    new JsonSubTypes.Type(name = "output", value = classOf[OutputTask]),
    new JsonSubTypes.Type(name = "loop", value = classOf[LoopTask])
))
abstract class Task {
    /**
      * Abstract method which will perform the given task.
      *
      * @param executor
      */
    def execute(executor:Executor) : Boolean
}
