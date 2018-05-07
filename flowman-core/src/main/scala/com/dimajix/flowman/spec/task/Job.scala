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

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.util.StdConverter
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.execution.ScopedContext
import com.dimajix.flowman.spec.schema.FieldType
import com.dimajix.flowman.spec.schema.FieldValue
import com.dimajix.flowman.spec.schema.StringType


sealed abstract class JobStatus;
object JobStatus {
    case object SUCCESS extends JobStatus
    case object FAILURE extends JobStatus
    case object ABORTED extends JobStatus
    case object SKIPPED extends JobStatus
}

class JobParameter {
    @JsonProperty(value="name") private var _name:String = ""
    @JsonProperty(value="description") private var _description:String = ""
    @JsonProperty(value="type", required = false) private var _type: FieldType = StringType
    @JsonProperty(value="granularity", required = false) private var _granularity: String = _
    @JsonProperty(value="default", required = false) private var _default: String = _

    def this(name:String, ftype:FieldType, granularity:String = null, value:String = null, description:String = "") = {
        this()
        _name = name
        _description = description
        _type = ftype
        _granularity = granularity
        _default = value
    }

    def name : String = _name
    def description : String = _description
    def ftype : FieldType = _type
    def granularity(implicit context: Context) : String = context.evaluate(_granularity)
    def default(implicit context:Context) : String = context.evaluate(_default)

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

    class Builder {
        private val job = new Job

        def build() : Job = job

        def setName(name:String) : Builder = {
            job._name = name
            this
        }
        def setDescription(desc:String) : Builder = {
            job._description = desc
            this
        }
        def setParameters(params:Seq[JobParameter]) : Builder = {
            job._parameters = params
            this
        }
        def addParameter(param:JobParameter) : Builder = {
            job._parameters = job._parameters :+ param
            this
        }
        def addParameter(name:String, ftype:FieldType, granularity:String = null, value:String = null) : Builder = {
            job._parameters = job._parameters :+ new JobParameter(name, ftype, granularity, value)
            this
        }
        def setTasks(tasks:Seq[Task]) : Builder = {
            job._tasks = tasks
            this
        }
        def addTask(task:Task) : Builder = {
            job._tasks = job._tasks :+ task
            this
        }
    }

    def builder() : Builder = new Builder

    class NameResolver extends StdConverter[Map[String,Job],Map[String,Job]] {
        override def convert(value: Map[String,Job]): Map[String,Job] = {
            value.foreach(kv => kv._2._name = kv._1)
            value
        }
    }
}

/**
  * A Job represents a collection of individual tasks. Jobs can be logged by appropriate runners.
  */
class Job {
    private val logger = LoggerFactory.getLogger(classOf[Job])

    @JsonIgnore private var _name:String = ""
    @JsonProperty(value="description") private var _description:String = ""
    @JsonProperty(value="parameters") private var _parameters:Seq[JobParameter] = Seq()
    @JsonProperty(value="tasks") private var _tasks:Seq[Task] = Seq()

    def name : String = _name
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
        val processedArgs = args.map(kv =>
            (kv._1, paramsByName.getOrElse(kv._1, throw new IllegalArgumentException(s"Parameter ${kv._1} not defined for job")).parse(kv._2).toString))
        parameters.map(p => (p.name, p.default)).toMap ++ processedArgs
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
        logger.info(s"Running job: '$name' ($description)")

        // Create a new execution environment
        val jobArgs = arguments(args)
        jobArgs.filter(_._2 == null).foreach(p => throw new IllegalArgumentException(s"Parameter ${p._1} not defined"))
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
