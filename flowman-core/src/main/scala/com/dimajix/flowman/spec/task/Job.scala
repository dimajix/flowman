/*
 * Copyright 2018 Kaya Kupferschmidt
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dimajix.flowman.spec.task

import scala.collection.immutable.ListMap
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import com.fasterxml.jackson.annotation.JsonProperty
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.execution.ScopedContext


case class JobStatus(name:String)
object JobStatus {
    val SUCCESS = new JobStatus("SUCCESS")
    val FAILURE = new JobStatus("FAILURE")
    val ABORTED = new JobStatus("ABORTED")
    val SKIPPED = new JobStatus("SKIPPED")
}

object Job {
    def apply(tasks:Seq[Task], description:String) : Job = {
        val job = new Job
        job._tasks = tasks
        job._description = description
        job
    }
}

class Job {
    private val logger = LoggerFactory.getLogger(classOf[Job])

    @JsonProperty(value="description") private var _description:String = ""
    @JsonProperty(value="parameters") private var _parameters:ListMap[String,String] = ListMap()
    @JsonProperty(value="tasks") private var _tasks:Seq[Task] = Seq()

    def description(implicit context:Context) : String = context.evaluate(_description)
    def tasks : Seq[Task] = _tasks

    def execute(executor:Executor) : JobStatus = {
        implicit val context = executor.context
        logger.info(s"Running job: '$description'")

        // Create a new execution environment
        val jobContext = new ScopedContext(context)
        val jobExecutor = executor.withContext(jobContext)

        Try {
            _tasks.forall { task =>
                logger.info(s"Executing task ${task.description}")
                task.execute(jobExecutor)
            }
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
