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

package com.dimajix.flowman.model

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.execution.JobExecutor
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.metric.MetricBoard
import com.dimajix.flowman.model.Dataset.Properties
import com.dimajix.flowman.model.Job.Parameter
import com.dimajix.flowman.types.FieldType
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.RangeValue
import com.dimajix.flowman.types.SingleValue


/**
  * A BatchInstance serves as an identifier of a specific batch in the History
  * @param namespace
  * @param project
  * @param job
  * @param args
  */
final case class JobInstance(
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
                Map(),
                None
            )
        }
    }
    final case class Properties(
        context: Context,
        namespace:Option[Namespace],
        project:Option[Project],
        name: String,
        labels: Map[String, String],
        description:Option[String]
   ) extends Instance.Properties[Properties] {
        override val kind : String = "batch"
        override def withName(name: String): Properties = copy(name=name)
   }

    class Builder(context:Context) {
        require(context != null)
        private var name:String = ""
        private var labels:Map[String,String] = Map()
        private var description:Option[String] = None
        private var parameters:Seq[Parameter] = Seq()
        private var targets:Seq[TargetIdentifier] = Seq()
        private var environment:Map[String,String] = Map()

        def build() : Job = Job(
            Job.Properties(context, context.namespace, context.project, name, labels, description),
            parameters,
            environment,
            targets
        )
        def setProperties(props:Job.Properties) : Builder = {
            require(props != null)
            require(props.context eq context)
            name = props.name
            labels = props.labels
            description = props.description
            this
        }
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
        def setEnvironment(env:Map[String,String]) : Builder = {
            require(env != null)
            this.environment = env
            this
        }
        def addEnvironment(key:String, value:String) : Builder = {
            require(key != null)
            require(value != null)
            this.environment = this.environment + (key -> value)
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

    /**
     * Creates a new Job from an existing job and a list of parent jobs
     * @param job
     * @param parents
     * @return
     */
    def merge(job:Job, parents:Seq[Job]) : Job = {
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

        val allEnvironment = parentEnvironment ++ job.environment

        val allParameters = parentParameters -- allEnvironment.keySet ++ job.parameters.map(p => (p.name,p)).toMap

        val allTargets = parentTargets ++ job.targets

        val allMetrics = job.metrics.orElse(parentMetrics)

        Job(
            job.instanceProperties,
            allParameters.values.toSeq,
            allEnvironment,
            allTargets.toSeq,
            allMetrics
        )
    }
}


final case class Job(
    instanceProperties:Job.Properties,
    parameters:Seq[Job.Parameter] = Seq(),
    environment:Map[String,String] = Map(),
    targets:Seq[TargetIdentifier] = Seq(),
    metrics:Option[Template[MetricBoard]] = None
) extends AbstractInstance {
    private val logger = LoggerFactory.getLogger(classOf[Job])

    override def category: String = "job"
    override def kind : String = "job"

    /**
      * Returns an identifier for this job
      * @return
      */
    def identifier : JobIdentifier = JobIdentifier(name, project.map(_.name))

    /**
      * Returns a description of the job
      * @return
      */
    def description : Option[String] = instanceProperties.description

    /**
      * Returns a JobInstance used for state management
      * @return
      */
    def instance(args:Map[String,String]) : JobInstance = {
        JobInstance(
            namespace.map(_.name).getOrElse(""),
            project.map(_.name).getOrElse(""),
            name,
            args
        )
    }

    /**
      * Determine final arguments of this job, by performing granularity adjustments etc. Missing arguments will
      * be replaced by default values if they are defined.
      * @param args
      * @return
      */
    def arguments(args:Map[String,String]) : Map[String,Any] = {
        val paramsByName = parameters.map(p => (p.name, p)).toMap
        val processedArgs = args.map(kv =>
            (kv._1, paramsByName.getOrElse(kv._1, throw new IllegalArgumentException(s"Parameter '${kv._1}' not defined for job '$name'")).parse(kv._2)))
        parameters.flatMap(p => p.default.map(v => p.name -> v)).toMap ++ processedArgs
    }

    /**
      * Performs interpolation of given arguments as FieldValues. This will return an Iterable of argument maps each
      * of them to be used by a job executor.
      * @param args
      * @return
      */
    def interpolate(args:Map[String,FieldValue]) : Iterable[Map[String,Any]] = {
        def interpolate(args:Iterable[Map[String,Any]], param:Parameter, values:FieldValue) : Iterable[Map[String,Any]] = {
            val vals = param.ftype.interpolate(values, param.granularity)
            args.flatMap(map => vals.map(v => map + (param.name -> v)))
        }

        // Iterate by all parameters and create argument map
        val paramByName = parameters.map(p => (p.name, p)).toMap
        args.toSeq.foldRight(Iterable[Map[String,Any]](Map()))((p,i) => interpolate(i, paramByName(p._1), p._2))
    }

    /**
      * Parse command line parameters
      * @param rawArgs
      * @return
      */
    def parseArguments(rawArgs:Map[String,String]) : Map[String,FieldValue] = {
        val paramNames = parameters.map(_.name).toSet
        rawArgs.foldLeft(Map[String,FieldValue]()){(map,arg) =>
            val rawName = arg._1
            val rawValue = arg._2
            val entry =
                if (rawName.endsWith(":start")) {
                    val name = rawName.dropRight(6)
                    map.get(name) match {
                        case Some(RangeValue(_,end,step)) => name -> RangeValue(rawValue, end, step)
                        case _ => name -> RangeValue(rawValue, rawValue)
                    }
                }
                else if (rawName.endsWith(":end")) {
                    val name = rawName.dropRight(4)
                    map.get(name) match {
                        case Some(RangeValue(start,_,step)) => name -> RangeValue(start, rawValue, step)
                        case _ => name -> RangeValue(rawValue, rawValue)
                    }
                }
                else if (rawName.endsWith(":step")) {
                    val name = rawName.dropRight(5)
                    map.get(name) match {
                        case Some(RangeValue(start,end,_)) => name -> RangeValue(start, end, Some(rawValue))
                        case _ => name -> RangeValue("", "", Some(rawValue))
                    }
                }
                else {
                    rawName -> SingleValue(rawValue)
                }

            if (!paramNames.contains(entry._1))
                throw new IllegalArgumentException(s"Parameter '${entry._1}' not defined for job '${identifier}'")

            map + entry
        }
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

        val jobArgs = arguments(args)
        val jobExecutor = new JobExecutor(executor, this, jobArgs, force)
        jobExecutor.execute(phase) { (executor,target,force) =>
            Try {
                target.execute(executor, phase)
            } match {
                case Success(_) =>
                    logger.info(s"Successfully finished phase '$phase' of execution of job '${identifier}'")
                    Status.SUCCESS
                case Failure(_) =>
                    logger.error(s"Execution of phase '$phase' of job '${identifier}' failed")
                    Status.FAILED
            }
        }
    }
}
