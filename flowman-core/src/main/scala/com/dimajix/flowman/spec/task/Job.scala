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

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import com.fasterxml.jackson.annotation.JsonProperty
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.execution.ScopedContext
import com.dimajix.flowman.spec.schema.FieldType
import com.dimajix.flowman.spec.schema.FieldValue
import com.dimajix.flowman.spec.schema.StringType


case class JobStatus(name:String)
object JobStatus {
    val SUCCESS = new JobStatus("SUCCESS")
    val FAILURE = new JobStatus("FAILURE")
    val ABORTED = new JobStatus("ABORTED")
    val SKIPPED = new JobStatus("SKIPPED")
}

class JobParameter {
    @JsonProperty(value="name") private var _name:String = ""
    @JsonProperty(value="type", required = false) private var _type: FieldType = StringType
    @JsonProperty(value="granularity", required = false) private var _granularity: String = _
    @JsonProperty(value="value", required = false) private var _value: String = ""

    def name : String = _name
    def ftype : FieldType = _type
    def granularity(implicit context: Context) : String = context.evaluate(_granularity)
    def value(implicit context:Context) : String = context.evaluate(_value)

    def interpolate(value:FieldValue)(implicit context:Context) : Iterable[Any] = {
        ftype.interpolate(value, granularity)
    }
    def parse(value:String)(implicit context:Context) : Any = {
        ftype.parse(value)
    }
}


object Job {
    def apply(tasks:Seq[Task], description:String) : Job = {
        val job = new Job
        job._tasks = tasks
        job._description = description
        job
    }
}

/**
  * A Job represents a collection of individual tasks. Jobs can be logged by appropriate runners.
  */
class Job {
    private val logger = LoggerFactory.getLogger(classOf[Job])

    @JsonProperty(value="description") private var _description:String = ""
    @JsonProperty(value="parameters") private var _parameters:Seq[JobParameter] = Seq()
    @JsonProperty(value="tasks") private var _tasks:Seq[Task] = Seq()

    def description(implicit context:Context) : String = context.evaluate(_description)
    def tasks : Seq[Task] = _tasks
    def parameters: Seq[JobParameter] = _parameters

    /**
      * Determine final arguments of this job, by performing granularity adjustments atc
      * @param args
      * @param context
      * @return
      */
    def arguments(args:Map[String,String])(implicit context:Context) : Map[String,String] = {
        val paramsByName = parameters.map(p => (p.name, p)).toMap
        val processedArgs = args.map(kv => (kv._1, paramsByName(kv._1).parse(kv._2).toString))
        parameters.map(p => (p.name, p.value)).toMap ++ processedArgs
    }

    /**
      * Executes this job and adds the arguments as additional environment variables. They will only be
      * available inside the job and are cleared afterwards
      *
      * @param executor
      * @param args
      * @return
      */
    def execute(executor:Executor, args:Map[String,String]) : JobStatus = {
        implicit val context = executor.context
        logger.info(s"Running job: '$description'")

        // Create a new execution environment
        val jobArgs = arguments(args)
        val jobContext = new ScopedContext(context).withConfig(jobArgs)
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
