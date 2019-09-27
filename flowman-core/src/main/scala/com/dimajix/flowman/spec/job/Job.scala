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

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.util.StdConverter
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.execution.JobExecutor
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.spec.AbstractInstance
import com.dimajix.flowman.spec.Instance
import com.dimajix.flowman.spec.JobIdentifier
import com.dimajix.flowman.spec.NamedSpec
import com.dimajix.flowman.spec.Namespace
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.spec.Spec
import com.dimajix.flowman.spec.TargetIdentifier
import com.dimajix.flowman.spec.job.Job.Parameter
import com.dimajix.flowman.spec.metric.MetricBoardSpec
import com.dimajix.flowman.spec.splitSettings
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

    def interpolate(args:Map[String,FieldValue])(fn:Map[String,Any] => Boolean) : Boolean = {
        def interpolate(fn:Map[String,Any] => Boolean, param:Parameter, values:FieldValue) : Map[String,Any] => Boolean = {
            val vals = param.ftype.interpolate(values, param.granularity)
            args:Map[String,Any] => vals.forall(v => fn(args + (param.name -> v)))
        }

        // Iterate by all parameters and create argument map
        val paramByName = parameters.map(p => (p.name, p)).toMap
        val invocations = args.toSeq.foldRight(fn)((p,a) => interpolate(a, paramByName(p._1), p._2))

        invocations(Map())
    }

    /**
      * This method will execute all targets secified in this job in the correct order. It is a convenient wrapper
      * around JobExecutor, which actually takes care about all the details
      * @param executor
      * @param phase
      * @param args
      * @param force
      * @return
      */
    def execute(executor:Executor, phase:Phase, args:Map[String,String], force:Boolean=false) : Status = {
        require(args != null)
        require(phase != null)
        require(args != null)

        val jobExecutor = new JobExecutor(executor, this, args, force)
        jobExecutor.execute(phase)
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
