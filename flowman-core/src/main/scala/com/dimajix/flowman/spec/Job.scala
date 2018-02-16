package com.dimajix.flowman.spec

import scala.collection.immutable.ListMap
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.task.Task


case class JobStatus(name:String)
object JobStatus {
    val SUCCESS = new JobStatus("SUCCESS")
    val FAILURE = new JobStatus("FAILURE")
    val ABORTED = new JobStatus("ABORTED")
    val SKIPPED = new JobStatus("SKIPPED")
}

class Job {
    private val logger = LoggerFactory.getLogger(classOf[Job])

    @JsonProperty(value="parameters") private var _parameters:ListMap[String,String] = ListMap()
    @JsonProperty(value="tasks") private var _tasks:Seq[Task] = Seq()

    def tasks : Seq[Task] = _tasks
    @JsonIgnore
    def tasks_=(tasks:Seq[Task]) : Unit = _tasks = tasks

    def execute(executor:Executor) : JobStatus = {
        Try {
            _tasks.forall(_.execute(executor))
        } match {
            case Success(true) =>
                logger.info("Successfully executed job")
                JobStatus.SUCCESS
            case Success(false) =>
                logger.error("Execution of job failed")
                JobStatus.FAILURE
            case Failure(e) =>
                logger.error("Execution of job failed with exception: {}", e.getMessage)
                logger.error(e.getStackTrace.mkString("\n    at "))
                JobStatus.FAILURE
        }
    }
}
