/*
 * Copyright 2019 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.job

import scala.collection.mutable

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.util.StdConverter
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.RootContext
import com.dimajix.flowman.execution.RootExecutor
import com.dimajix.flowman.execution.SettingLevel
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.metric.MetricSystem
import com.dimajix.flowman.spec.AbstractInstance
import com.dimajix.flowman.spec.JobIdentifier
import com.dimajix.flowman.spec.Instance
import com.dimajix.flowman.spec.NamedSpec
import com.dimajix.flowman.spec.Namespace
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.spec.Spec
import com.dimajix.flowman.spec.TargetIdentifier
import com.dimajix.flowman.spec.metric.MetricBoardSpec
import com.dimajix.flowman.spec.splitSettings
import com.dimajix.flowman.spec.target.Target
import com.dimajix.flowman.spi.TypeRegistry
import com.dimajix.flowman.types.FieldType
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.StringType


/**
  * A BatchInstance serves as an identifier of a specific batch in the History
  * @param namespace
  * @param project
  * @param job
  * @param args
  */
case class JobInstance(
    namespace:String,
    project:String,
    job:String,
    args:Map[String,String] = Map()
) {
    require(namespace != null)
    require(project != null)
    require(job != null)
    require(args != null)
}

object Job {
    case class Parameter(
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
        override val kind : String = "batch"
   }

    class Builder(context:Context) {
        require(context != null)
        private var name:String = ""
        private var description:Option[String] = None
        private var parameters:Seq[Parameter] = Seq()
        private var targets:Seq[TargetIdentifier] = Seq()

        def build() : Job = Job(
            Job.Properties(context, name),
            description,
            parameters,
            Map(),
            targets
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
        def setParameters(params:Seq[Parameter]) : Builder = {
            require(params != null)
            this.parameters = params
            this
        }
        def addParameter(param:Parameter) : Builder = {
            require(param != null)
            this.parameters = this.parameters :+ param
            this
        }
        def addParameter(name:String, ftype:FieldType, granularity:Option[String] = None, value:Option[Any] = None) : Builder = {
            require(name != null)
            require(ftype != null)
            this.parameters = this.parameters :+ Parameter(name, ftype, granularity, value)
            this
        }
        def setTargets(targets:Seq[TargetIdentifier]) : Builder = {
            require(targets != null)
            this.targets = targets
            this
        }
        def addTarget(target:TargetIdentifier) : Builder = {
            require(target != null)
            this.targets = this.targets :+ target
            this
        }
    }

    def builder(context: Context) : Builder = new Builder(context)
}


case class Job(
    instanceProperties:Job.Properties,
    description:Option[String],
    parameters:Seq[Job.Parameter],
    environment:Map[String,String],
    targets:Seq[TargetIdentifier],
    metrics:Option[MetricBoardSpec] = None
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

    def execute(executor:Executor, phase:Phase, args:Map[String,String], force:Boolean=false) : Status = {
        require(args != null)

        val description = this.description.map("(" + _ + ")").getOrElse("")
        logger.info(s"Running phase '$phase' of execution: '$name' $description")

        // Create a new execution environment.
        val jobArgs = arguments(args)
        jobArgs.filter(_._2 == null).foreach(p => throw new IllegalArgumentException(s"Parameter '${p._1}' not defined for execution '$name'"))

        // Check if the job should run isolated. This is required if arguments are specified, which could
        // result in different DataFrames with different arguments
        val isolated = args.nonEmpty || environment.nonEmpty

        // Create a new execution environment.
        val rootContext = RootContext.builder(context)
            .withEnvironment("force", force)
            .withEnvironment(jobArgs, SettingLevel.SCOPE_OVERRIDE)
            .withEnvironment(environment, SettingLevel.SCOPE_OVERRIDE)
            .build()
        val projectExecutor = new RootExecutor(executor, isolated)
        val projectContext = if (context.project != null) rootContext.getProjectContext(context.project) else rootContext

        withMetrics(projectContext, projectExecutor.metrics) {
            val result = executeTargets(projectContext, projectExecutor, phase, force)

            // Release any resources
            if (isolated) {
                projectExecutor.cleanup()
            }

            result
        }
    }

    /**
      * Execute all targets of this batch in an appropriate order
      * @param context
      * @param executor
      * @param phase
      * @param force
      * @return
      */
    private def executeTargets(context: Context, executor: Executor, phase:Phase, force:Boolean) : Status = {
        val order = phase match {
            case Phase.DESTROY | Phase.TRUNCATE => orderTargets(context, phase).reverse
            case _ => orderTargets(context, phase)
        }

        val runner = executor.runner
        val iter = order.iterator
        var error = false
        while (iter.hasNext) {
            val target = iter.next()
            val status = runner.executeTarget(executor, target, phase, force)
            error |= (status != Status.SUCCESS && status != Status.SKIPPED)
        }

        if (error)
            Status.FAILED
        else
            Status.SUCCESS
    }

    /**
      * Create ordering of specified targets, such that all dependencies are fullfilled
      * @param context
      * @return
      */
    private def orderTargets(context: Context, phase:Phase) : Seq[Target] = {
        def normalize(target:Target, deps:Seq[TargetIdentifier]) : Seq[TargetIdentifier] = {
            deps.map(dep =>
                if (dep.project.nonEmpty)
                    dep
                else
                    TargetIdentifier(dep.name, Option(target.project).map(_.name))
            )
        }

        val targets = this.targets.map(t => context.getTarget(t))
        val targetIds = targets.map(_.identifier).toSet
        val targetsByResources = targets.flatMap(t => t.provides(phase).map(id => (id,t.identifier))).toMap

        val nodes = mutable.Map(targets.map(t => t.identifier -> mutable.Set[TargetIdentifier]()):_*)

        // Process all 'after' dependencies
        targets.foreach(t => {
            val deps =  normalize(t, t.after).filter(targetIds.contains)
            deps.foreach(d => nodes(t.identifier).add(d))
        })

        // Process all 'before' dependencies
        targets.foreach(t => {
            val deps = normalize(t, t.before).filter(targetIds.contains)
            deps.foreach(b => nodes(b).add(t.identifier))
        })

        // Process all 'requires' dependencies
        targets.foreach(t => {
            val deps = t.requires(phase).flatMap(targetsByResources.get)
            deps.foreach(d => nodes(t.identifier).add(d))
        })

        val order = mutable.ListBuffer[TargetIdentifier]()
        while (nodes.nonEmpty) {
            val candidate = nodes.find(_._2.isEmpty).map(_._1)
                .getOrElse(throw new RuntimeException("Cannot create target order"))

            // Remove candidate
            nodes.remove(candidate)
            // Remove this target from all dependencies
            nodes.foreach { case (k,v) => v.remove(candidate) }
            // Append candidate to build sequence
            order.append(candidate)
        }

        order.map(context.getTarget)
    }

    private def withMetrics[T](context:Context, metricSystem:MetricSystem)(fn: => T) : T = {
        val metrics = this.metrics.map(_.instantiate(context, metricSystem))

        // Publish metrics
        metrics.foreach { metrics =>
            metrics.reset()
            metricSystem.addBoard(metrics)
        }

        // Run original function
        val result = fn

        // Unpublish metrics
        metrics.foreach { metrics =>
            metricSystem.commitBoard(metrics)
            metricSystem.removeBoard(metrics)
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
    @JsonProperty(value="extends") private var parents:Seq[String] = Seq()
    @JsonProperty(value="description") private var description:Option[String] = None
    @JsonProperty(value="parameters") private var parameters:Seq[JobParameterSpec] = Seq()
    @JsonProperty(value="environment") private var environment: Seq[String] = Seq()
    @JsonProperty(value="targets") private var targets: Seq[String] = Seq()
    @JsonProperty(value="metrics") private var metrics:Option[MetricBoardSpec] = None

    override def instantiate(context: Context): Job = {
        require(context != null)

        val parents = this.parents.map(job => context.getJob(JobIdentifier(job)))

        val parentParameters = parents
            .map(job => job.parameters.map(p => (p.name, p)).toMap)
            .reduceOption((params, elems) => params ++ elems)
            .getOrElse(Map())
        val parentEnvironment = parents
            .map(job => job.environment)
            .reduceOption((envs, elems) => envs ++ elems)
            .getOrElse(Map())
        val parentTargets = parents
            .map(job => job.targets.toSet)
            .reduceOption((targets, elems) => targets ++ elems)
            .getOrElse(Set())
        val parentMetrics = parents
            .flatMap(job => job.metrics)
            .headOption

        val curEnvironment = splitSettings(environment).toMap
        val allEnvironment = parentEnvironment ++ curEnvironment

        val curParameters = parameters.map(_.instantiate(context)).map(p => (p.name,p)).toMap
        val allParameters = parentParameters -- allEnvironment.keySet ++ curParameters

        val curTargets = targets.map(context.evaluate).map(TargetIdentifier.parse)
        val allTargets = parentTargets ++ curTargets

        val allMetrics = metrics.orElse(parentMetrics)

        Job(
            instanceProperties(context),
            description.map(context.evaluate),
            allParameters.values.toSeq,
            allEnvironment,
            allTargets.toSeq,
            allMetrics
        )
    }

    /**
      * Returns a set of common properties
      *
      * @param context
      * @return
      */
    override protected def instanceProperties(context: Context): Job.Properties = {
        require(context != null)
        Job.Properties(
            context,
            context.namespace,
            context.project,
            name,
            context.evaluate(labels)
        )
    }
}

class JobParameterSpec extends Spec[Job.Parameter] {
    @JsonProperty(value = "name") private var name: String = ""
    @JsonProperty(value = "description") private var description: Option[String] = None
    @JsonProperty(value = "type", required = false) private var ftype: FieldType = StringType
    @JsonProperty(value = "granularity", required = false) private var granularity: Option[String] = None
    @JsonProperty(value = "default", required = false) private var default: Option[String] = None

    override def instantiate(context: Context): Job.Parameter = {
        require(context != null)

        Job.Parameter(
            context.evaluate(name),
            ftype,
            granularity.map(context.evaluate),
            default.map(context.evaluate).map(d => ftype.parse(d)),
            description.map(context.evaluate)
        )
    }
}
