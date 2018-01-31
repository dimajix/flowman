package com.dimajix.dataflow.spec

import scala.collection.immutable.ListMap

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty

import com.dimajix.dataflow.execution.Executor
import com.dimajix.dataflow.spec.task.Task


case class JobStatus(name:String)
object JobStatus {
    val SUCCESS = new JobStatus("SUCCESS")
    val FAILURE = new JobStatus("FAILURE")
    val ABORTED = new JobStatus("ABORTED")
    val SKIPPED = new JobStatus("SKIPPED")
}

class Job {
    @JsonProperty(value="parameters") private var _parameters:ListMap[String,String] = ListMap()
    @JsonProperty(value="tasks") private var _tasks:Seq[Task] = Seq()

    def tasks : Seq[Task] = _tasks
    @JsonIgnore
    def tasks_=(tasks:Seq[Task]) : Unit = _tasks = tasks

    def execute(executor:Executor) : JobStatus = {
        val result = _tasks.forall(_.execute(executor))
        if (result)
            JobStatus.SUCCESS
        else
            JobStatus.FAILURE
    }
}
