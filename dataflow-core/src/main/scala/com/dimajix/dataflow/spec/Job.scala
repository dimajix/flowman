package com.dimajix.dataflow.spec

import scala.collection.immutable.ListMap

import com.fasterxml.jackson.annotation.JsonProperty

import com.dimajix.dataflow.execution.Executor
import com.dimajix.dataflow.spec.task.Task


class Job {
    @JsonProperty(value="parameters") private var _parameters:ListMap[String,String] = ListMap()
    @JsonProperty(value="tasks") private var _tasks:Seq[Task] = Seq()

    def execute(executor:Executor) : Unit = ???
}
