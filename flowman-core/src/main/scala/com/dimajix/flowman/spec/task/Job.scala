/*
 * Copyright 2018-2019 Kaya Kupferschmidt
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
import com.fasterxml.jackson.databind.util.StdConverter
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.execution.RootContext
import com.dimajix.flowman.execution.RootExecutor
import com.dimajix.flowman.execution.SettingLevel
import com.dimajix.flowman.spec.AbstractInstance
import com.dimajix.flowman.spec.Instance
import com.dimajix.flowman.spec.JobIdentifier
import com.dimajix.flowman.spec.NamedSpec
import com.dimajix.flowman.spec.Namespace
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.spec.Spec
import com.dimajix.flowman.spec.splitSettings
import com.dimajix.flowman.spi.TypeRegistry
import com.dimajix.flowman.state.JobInstance
import com.dimajix.flowman.state.Status
import com.dimajix.flowman.types.FieldType
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.StringType


case class JobParameter(
    name:String,
    ftype : FieldType,
    granularity: Option[String]=None,
    default: Option[Any] = None,
    description: Option[String]=None
) {
    /**
      * Interpolates a given FieldValue returning all values as an Iterable
      * @param value
      * @return
      */
    def interpolate(value:FieldValue) : Iterable[Any] = {
        ftype.interpolate(value, granularity)
    }

    /**
      * Pasres a string representing a single value for the parameter
      * @param value
      * @return
      */
    def parse(value:String) : Any = {
        ftype.parse(value)
    }
}


object Job {
    object Properties {
        def apply(context:Context, name:String="") : Properties = {
            require(context != null)
            Properties(
                context,
                context.namespace,
                context.project,
                name,
                Map()
            )
        }
    }
    case class Properties(
        context: Context,
        namespace:Namespace,
        project:Project,
        name: String,
        labels: Map[String, String]
    ) extends Instance.Properties {
        override val kind : String = "job"
    }

    class Builder(context:Context) {
        require(context != null)
        private var name:String = ""
        private var description:Option[String] = None
        private var logged:Boolean = true
        private var parameters:Seq[JobParameter] = Seq()
        private var tasks:Seq[TaskSpec] = Seq()
        private var failure:Seq[TaskSpec] = Seq()
        private var cleanup:Seq[TaskSpec] = Seq()

        def build() : Job = Job(
            Job.Properties(context, name),
            description,
            parameters,
            Map(),
            tasks,
            failure,
            cleanup,
            logged
        )

        def setName(name:String) : Builder = {
            require(name != null)
            this.name = name
            this
        }
        def setDescription(desc:String) : Builder = {
            require(desc != null)
            this.description = Some(desc)
            this
        }
        def setLogged(boolean: Boolean) : Builder = {
            this.logged = boolean
            this
        }
        def setParameters(params:Seq[JobParameter]) : Builder = {
            require(params != null)
            this.parameters = params
            this
        }
        def addParameter(param:JobParameter) : Builder = {
            require(param != null)
            this.parameters = this.parameters :+ param
            this
        }
        def addParameter(name:String, ftype:FieldType, granularity:Option[String] = None, value:Option[Any] = None) : Builder = {
            require(name != null)
            require(ftype != null)
            this.parameters = this.parameters :+ JobParameter(name, ftype, granularity, value)
            this
        }
        def setTasks(tasks:Seq[TaskSpec]) : Builder = {
            require(tasks != null)
            this.tasks = tasks
            this
        }
        def addTask(task:TaskSpec) : Builder = {
            require(task != null)
            this.tasks = this.tasks :+ task
            this
        }
        def addTask(task:Task) : Builder = {
            require(task != null)
            require(task.context eq context)
            val wrapped = new TaskSpec {
                override def instantiate(context: Context): Task = task
            }
            this.tasks = this.tasks :+ wrapped
            this
        }
    }

    def builder(context: Context) : Builder = new Builder(context)
}

/**
  * A Job represents a collection of individual tasks. Jobs can be logged by appropriate runners.
  */
case class Job (
    instanceProperties:Job.Properties,
    description:Option[String],
    parameters:Seq[JobParameter],
    environment:Map[String,String],
    tasks:Seq[TaskSpec],
    failure:Seq[TaskSpec],
    cleanup:Seq[TaskSpec],
    logged:Boolean
) extends AbstractInstance {
    private val logger = LoggerFactory.getLogger(classOf[Job])

    override def category: String = "job"
    override def kind : String = "job"

    /**
      * Returns an identifier for this job
      * @return
      */
    def identifier : JobIdentifier = JobIdentifier(name, Option(project).map(_.name))

    /**
      * Returns a JobInstance used for state management
      * @return
      */
    def instance(args:Map[String,String]) : JobInstance = {
        JobInstance(
            Option(context.namespace).map(_.name).getOrElse(""),
            Option(context.project).map(_.name).getOrElse(""),
            name,
            args
        )
    }

    /**
      * Determine final arguments of this job, by performing granularity adjustments etc
      * @param args
      * @return
      */
    def arguments(args:Map[String,String]) : Map[String,Any] = {
        val paramsByName = parameters.map(p => (p.name, p)).toMap
        val processedArgs = args.map(kv =>
            (kv._1, paramsByName.getOrElse(kv._1, throw new IllegalArgumentException(s"Parameter '${kv._1}' not defined for job '$name'")).parse(kv._2)))
        parameters.map(p => (p.name, p.default.orNull)).toMap ++ processedArgs
    }

    /**
      * Executes this job and adds the arguments as additional environment variables. They will only be
      * available inside the job and are cleared afterwards
      *
      * @param executor
      * @param args
      * @return
      */
    def execute(executor:Executor, args:Map[String,String]) : Status = {
        require(args != null)

        val description = this.description.map("(" + _ + ")").getOrElse("")
        logger.info(s"Running job: '$name' $description")

        // Create a new execution environment.
        val jobArgs = arguments(args)
        jobArgs.filter(_._2 == null).foreach(p => throw new IllegalArgumentException(s"Parameter '${p._1}' not defined for job '$name'"))

        // Check if the job should run isolated. This is required if arguments are specified, which could
        // result in different DataFrames with different arguments
        val isolated = args.nonEmpty

        // Create a new execution environment.
        val rootContext = RootContext.builder(context)
            .withEnvironment(jobArgs, SettingLevel.SCOPE_OVERRIDE)
            .withEnvironment(environment, SettingLevel.SCOPE_OVERRIDE)
            .build()
        val projectExecutor = new RootExecutor(executor, isolated)
        val projectContext = if (context.project != null) rootContext.getProjectContext(context.project) else rootContext

        val result = runJob(projectContext, projectExecutor)

        // Release any resources
        if (isolated) {
            projectExecutor.cleanup()
        }

        result
    }

    private def runJob(context:Context, executor:Executor) : Status = {
        val result = runTasks(context, executor, tasks)

        // Execute failure action
        if (failure.nonEmpty) {
            result match {
                case Success(false) | Failure(_) =>
                    logger.info(s"Running failure tasks for job '$name'")
                    runTasks(context, executor, failure)
                case Success(_) =>
            }
        }

        // Execute cleanup actions
        if (cleanup.nonEmpty) {
            logger.info(s"Running cleanup tasks for job '$name'")
            runTasks(context, executor, cleanup) match {
                case Success(true) =>
                    logger.info(s"Successfully executed all cleanup tasks of job '$name'")
                case Success(false) | Failure(_) =>
                    logger.error(s"Execution of cleanup tasks failed for job '$name'")
            }
        }

        result match {
            case Success(true) =>
                logger.info("Successfully executed job")
                Status.SUCCESS
            case Success(false) | Failure(_) =>
                logger.error(s"Execution of job '$name' failed")
                Status.FAILED
        }
    }

    private def runTasks(context:Context, executor:Executor, tasks:Seq[TaskSpec]) : Try[Boolean] = {
        val result = Try {
            tasks.forall { spec =>
                val task = spec.instantiate(context)
                logger.info(s"Executing next task ${task.description.map("'" + _ + "'").getOrElse("")}")
                task.execute(executor)
            }
        }
        result match {
            case Failure(e) =>
                logger.error("Execution of task failed with exception: ", e)
            case Success(false) =>
                logger.error("Execution of task failed with an error")
            case Success(true) =>
                logger.info(s"Successfully executed all tasks of job '$name'")
        }
        result
    }
}




object JobSpec extends TypeRegistry[JobSpec] {
    class NameResolver extends StdConverter[Map[String, JobSpec], Map[String, JobSpec]] {
        override def convert(value: Map[String, JobSpec]): Map[String, JobSpec] = {
            value.foreach(kv => kv._2.name = kv._1)
            value
        }
    }
}

class JobSpec extends NamedSpec[Job] {
    @JsonProperty(value="description") private var description:Option[String] = None
    @JsonProperty(value="logged") private var logged:String = "true"
    @JsonProperty(value="parameters") private var parameters:Seq[JobParameterSpec] = Seq()
    @JsonProperty(value="environment") private var environment: Seq[String] = Seq()
    @JsonProperty(value="tasks") private var tasks:Seq[TaskSpec] = Seq()
    @JsonProperty(value="failure") private var failure:Seq[TaskSpec] = Seq()
    @JsonProperty(value="cleanup") private var cleanup:Seq[TaskSpec] = Seq()

    override def instantiate(context: Context): Job = {
        Job(
            instanceProperties(context),
            description.map(context.evaluate),
            parameters.map(_.instantiate(context)),
            splitSettings(environment).toMap,
            tasks,
            failure,
            cleanup,
            context.evaluate(logged).toBoolean
        )
    }

    /**
      * Returns a set of common properties
      * @param context
      * @return
      */
    override protected def instanceProperties(context:Context) : Job.Properties = {
        require(context != null)
        Job.Properties(
            context,
            context.namespace,
            context.project,
            name,
            labels
        )
    }
}


class JobParameterSpec extends Spec[JobParameter] {
    @JsonProperty(value = "name") private var name: String = ""
    @JsonProperty(value = "description") private var description: Option[String] = None
    @JsonProperty(value = "type", required = false) private var ftype: FieldType = StringType
    @JsonProperty(value = "granularity", required = false) private var granularity: Option[String] = None
    @JsonProperty(value = "default", required = false) private var default: Option[String] = None

    override def instantiate(context: Context): JobParameter = {
        require(context != null)

        JobParameter(
            context.evaluate(name),
            ftype,
            granularity.map(context.evaluate),
            default.map(context.evaluate).map(d => ftype.parse(d)),
            description.map(context.evaluate)
        )
    }
}
