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
import com.dimajix.flowman.execution.RootContext
import com.dimajix.flowman.execution.RootExecutor
import com.dimajix.flowman.execution.SettingLevel
import com.dimajix.flowman.spec.schema.FieldType
import com.dimajix.flowman.spec.schema.FieldValue
import com.dimajix.flowman.spec.schema.StringType
import com.dimajix.flowman.util.splitSettings


sealed abstract class JobStatus
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

    /**
      * Returns the name of the paramter
      * @return
      */

    def name : String = _name
    /**
      * Returns the optional description of the paramter
      * @return
      */
    def description : String = _description

    /**
      * Returns the data type of the parameter
      * @return
      */
    def ftype : FieldType = _type

    /**
      * Returns the string representation of the granularity of the parameter.
      * @param context
      * @return
      */
    def granularity(implicit context: Context) : String = context.evaluate(_granularity)

    /**
      * Returns an optional default value of the parameter
      * @param context
      * @return
      */
    def default(implicit context:Context) : Any = {
        val v = context.evaluate(_default)
        if (v != null)
            ftype.parse(v)
        else
            null
    }

    /**
      * Interpolates a given FieldValue returning all values as an Iterable
      * @param value
      * @param context
      * @return
      */
    def interpolate(value:FieldValue)(implicit context:Context) : Iterable[Any] = {
        ftype.interpolate(value, granularity)
    }

    /**
      * Pasres a string representing a single value for the parameter
      * @param value
      * @param context
      * @return
      */
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
    @JsonProperty(value="logged") private var _logged:String = "true"
    @JsonProperty(value="parameters") private var _parameters:Seq[JobParameter] = Seq()
    @JsonProperty(value="environment") private var _environment: Seq[String] = Seq()
    @JsonProperty(value="tasks") private var _tasks:Seq[Task] = Seq()
    @JsonProperty(value="failure") private var _failure:Seq[Task] = Seq()
    @JsonProperty(value="cleanup") private var _cleanup:Seq[Task] = Seq()

    def name : String = _name
    def description(implicit context:Context) : String = context.evaluate(_description)
    def logged(implicit context:Context) : Boolean = context.evaluate(_logged).toBoolean
    def tasks : Seq[Task] = _tasks
    def failure : Seq[Task] = _failure
    def cleanup : Seq[Task] = _cleanup
    def environment : Seq[(String,String)] = splitSettings(_environment)
    def parameters: Seq[JobParameter] = _parameters

    /**
      * Determine final arguments of this job, by performing granularity adjustments atc
      * @param args
      * @param context
      * @return
      */
    def arguments(args:Map[String,String])(implicit context:Context) : Map[String,Any] = {
        val paramsByName = parameters.map(p => (p.name, p)).toMap
        val processedArgs = args.map(kv =>
            (kv._1, paramsByName.getOrElse(kv._1, throw new IllegalArgumentException(s"Parameter '${kv._1}' not defined for job '$name'")).parse(kv._2)))
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

        // Create a new execution environment.
        val jobArgs = arguments(args)
        jobArgs.filter(_._2 == null).foreach(p => throw new IllegalArgumentException(s"Parameter '${p._1}' not defined for job '$name'"))

        // Check if the job should run isolated. This is required if arguments are specified, which could
        // result in different DataFrames with different arguments
        val isolated = args != null && args.nonEmpty

        // Create a new execution environment.
        val jobContext = RootContext.builder(context)
            .withEnvironment(jobArgs.toSeq, SettingLevel.SCOPE_OVERRIDE)
            .withEnvironment(environment, SettingLevel.SCOPE_OVERRIDE)
            .build()
        val jobExecutor = new RootExecutor(executor, jobContext, isolated)
        val projectExecutor = if (context.project != null) jobExecutor.getProjectExecutor(context.project) else jobExecutor

        val result = runJob(projectExecutor)

        // Release any resources
        if (isolated) {
            jobExecutor.cleanup()
        }

        result
    }

    private def runJob(executor:Executor) : JobStatus = {
        implicit val context = executor.context
        val result = runTasks(executor, _tasks)

        // Execute failure action
        result match {
            case Success(false) | Failure(_) =>
                logger.info(s"Running failure tasks for job '$name'")
                runTasks(executor, _failure)
            case Success(_) =>
        }

        // Execute cleanup actions
        logger.info(s"Running cleanup tasks for job '$name'")
        runTasks(executor, _cleanup) match {
            case Success(true) =>
                logger.info(s"Successfully executed all cleanup tasks of job '$name'")
            case Success(false) | Failure(_) =>
                logger.error(s"Execution of cleanup tasks failed for job '$name'")
        }

        result match {
            case Success(true) =>
                logger.info("Successfully executed job")
                JobStatus.SUCCESS
            case Success(false) | Failure(_) =>
                logger.error(s"Execution of job '$name' failed")
                JobStatus.FAILURE
        }
    }

    private def runTasks(executor:Executor, tasks:Seq[Task]) : Try[Boolean] = {
        implicit val context = executor.context
        val result = Try {
            tasks.forall { task =>
                logger.info(s"Executing task '${task.description}'")
                task.execute(executor)
            }
        }
        result match {
            case Failure(e) =>
                logger.error("Execution of task failed with exception: ", e)
            case _ =>
                logger.info(s"Successfully executed all tasks of jon '$name'")
        }
        result
    }
}
