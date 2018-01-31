package com.dimajix.dataflow.spec.runner

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo

import com.dimajix.dataflow.execution.Executor
import com.dimajix.dataflow.spec.Job


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "simple", value = classOf[SimpleRunner]),
    new JsonSubTypes.Type(name = "logged", value = classOf[JdbcLoggedRunner])
))
abstract class Runner {
    def execute(executor: Executor, job:Job) : Boolean
}
