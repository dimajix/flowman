/*
 * Copyright (C) 2019 The Flowman Authors
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

import scala.util.control.NonFatal
import scala.util.matching.Regex

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.CyclePolicy
import com.dimajix.flowman.execution.Runner
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.history.NullStateStore
import com.dimajix.flowman.metric.MetricBoard
import com.dimajix.flowman.model.Job.Parameter
import com.dimajix.flowman.types.FieldType
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.RangeValue
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.model


/**
  * A BatchInstance serves as an identifier of a specific batch in the History
  * @param namespace
  * @param project
  * @param job
  * @param args
  */
final case class JobDigest(
    namespace:String,
    project:String,
    job:String,
    phase:Phase,
    args:Map[String,String] = Map.empty
) {
    def asMap: Map[String, String] =
         Map(
            "namespace" -> namespace,
            "project" -> project,
            "name" -> job,
            "job" -> job,
            "phase" -> phase.toString
        ) ++ args
}


final case class JobLifecycle(
    namespace:String,
    project:String,
    job:String,
    phases:Seq[Phase],
    args:Map[String,String] = Map.empty
) {
    def asMap: Map[String, String] =
        Map(
            "namespace" -> namespace,
            "project" -> project,
            "name" -> job,
            "job" -> job
        ) ++ args
}


object Job {
    final case class Parameter(
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

    final case class Execution(
        phase:Phase,
        cycle:CyclePolicy = CyclePolicy.ALWAYS,
        targets:Seq[Regex] = Seq(".*".r)
    )

    object Properties {
        def apply(context:Context, name:String="") : Properties = {
            require(context != null)
            Properties(
                context,
                Metadata(context, name, Category.JOB, "job"),
                None
            )
        }
    }
    final case class Properties(
        context: Context,
        metadata:Metadata,
        description:Option[String]
   ) extends model.Properties[Properties] {
        require(metadata.category == Category.JOB.lower)
        require(metadata.namespace == context.namespace.map(_.name))
        require(metadata.project == context.project.map(_.name))
        require(metadata.version == context.project.flatMap(_.version))

        override val namespace : Option[Namespace] = context.namespace
        override val project : Option[Project] = context.project
        override val kind : String = metadata.kind
        override val name : String = metadata.name

        override def withName(name: String): Properties = copy(metadata=metadata.copy(name = name))

        def merge(other: Properties): Properties = {
            Properties(context, metadata.merge(other.metadata), description.orElse(other.description))
        }
   }

    class Builder(context:Context) {
        require(context != null)
        private var name:String = ""
        private var metadata:Metadata = Metadata(context, "", Category.JOB, "job")
        private var description:Option[String] = None
        private var parameters:Seq[Parameter] = Seq.empty
        private var targets:Seq[TargetIdentifier] = Seq.empty
        private var environment:Map[String,String] = Map.empty
        private var hooks:Seq[Prototype[Hook]] = Seq.empty

        def build() : Job = Job(
            Job.Properties(context, metadata.copy(name=name), description),
            parameters = parameters,
            environment = environment,
            targets = targets,
            hooks = hooks
        )
        def setProperties(props:Job.Properties) : Builder = {
            require(props != null)
            require(props.context eq context)
            name = props.name
            metadata = props.metadata
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
        def addHook(hook:Prototype[Hook]) : Builder = {
            require(hook != null)
            this.hooks = this.hooks :+ hook
            this
        }
        def addHook(hook:Hook) : Builder = {
            require(hook != null)
            val template = Prototype.of(hook)
            this.hooks = this.hooks :+ template
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
            .getOrElse(Map.empty)
        val parentEnvironment = parents
            .map(job => job.environment)
            .reduceOption((envs, elems) => envs ++ elems)
            .getOrElse(Map.empty)
        val parentTargets = parents
            .flatMap(job => job.targets)
        val parentHooks = parents
            .flatMap(job => job.hooks)
        val parentMetrics = parents
            .flatMap(job => job.metrics)
            .headOption
        val parentExecutions = parents
            .flatMap(job => job.executions)

        val allEnvironment = parentEnvironment ++ job.environment

        val allParameters = parentParameters -- allEnvironment.keySet ++ job.parameters.map(p => (p.name,p)).toMap

        val allTargets = (parentTargets ++ job.targets).distinct

        val allHooks = parentHooks ++ job.hooks

        val allMetrics = job.metrics.orElse(parentMetrics)

        val allExecutions = parentExecutions ++ job.executions

        Job(
            job.instanceProperties,
            allParameters.values.toSeq,
            allEnvironment,
            allTargets,
            allMetrics,
            allHooks,
            allExecutions
        )
    }
}


final case class Job(
    instanceProperties:Job.Properties,
    parameters:Seq[Job.Parameter] = Seq.empty,
    environment:Map[String,String] = Map.empty,
    targets:Seq[TargetIdentifier] = Seq.empty,
    metrics:Option[Prototype[MetricBoard]] = None,
    hooks:Seq[Prototype[Hook]] = Seq.empty,
    executions:Seq[Job.Execution] = Seq.empty
) extends AbstractInstance {
    override type PropertiesType = Job.Properties

    override def category: Category = Category.JOB
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
      * Returns a JobDigest used for state management
      * @return
      */
    def digest(phase:Phase, args:Map[String,String]) : JobDigest = {
        val pargs = parameters.map(_.name).toSet
        if (args.keySet != pargs)
            throw new IllegalArgumentException(s"Argument mismatch for job '$identifier', expected: ${pargs.mkString(",")} received: ${args.keySet.mkString(",")}")

        JobDigest(
            namespace.map(_.name).getOrElse(""),
            project.map(_.name).getOrElse(""),
            name,
            phase,
            args
        )
    }

    /**
     * Returns a lifecycle representation of this job used for state management
     * @param phases
     * @param args
     * @return
     */
    def lifecycle(phases:Seq[Phase], args:Map[String,String]) : JobLifecycle = {
        val pargs = parameters.map(_.name).toSet
        if (args.keySet != pargs)
            throw new IllegalArgumentException(s"Argument mismatch for job '$identifier', expected: ${pargs.mkString(",")} received: ${args.keySet.mkString(",")}")

        JobLifecycle(
            namespace.map(_.name).getOrElse(""),
            project.map(_.name).getOrElse(""),
            name,
            phases,
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
        val processedArgs = args.map { case (pname, sval) =>
            val param = paramsByName.getOrElse(pname, throw new IllegalArgumentException(s"Parameter '$pname' not defined for job '$name'"))
            val pval = try {
                param.parse(sval)
            }
            catch {
                case NonFatal(ex) => throw new IllegalArgumentException(s"Cannot parse parameter '$pname' of job '$name' with value '$sval'", ex)
            }
            (pname, pval)
        }
        parameters.map { p =>
            val pname = p.name
            pname -> processedArgs.get(pname)
                .orElse(p.default)
                .getOrElse(throw new IllegalArgumentException(s"Missing parameter '$pname' in job '$name'"))
        }.toMap
    }

    /**
      * Performs interpolation of given arguments as FieldValues. This will return an Iterable of argument maps each
      * of them to be used by a job execution.
      * @param args
      * @return
      */
    def interpolate(args:Map[String,FieldValue]) : Iterable[Map[String,Any]] = {
        def interpolate(args:Iterable[Map[String,Any]], param:Parameter, values:FieldValue) : Iterable[Map[String,Any]] = {
            val vals = try {
                param.interpolate(values)
            }
            catch {
                case NonFatal(ex) => throw new IllegalArgumentException(s"Cannot interpolate parameter '${param.name}' of job '$name' with values '$values'", ex)
            }
            args.flatMap(map => vals.map(v => map + (param.name -> v)))
        }

        // Iterate by all parameters and create argument map
        val paramByName = parameters.map(p => (p.name, p)).toMap
        args.toSeq.foldRight(Iterable[Map[String,Any]](Map.empty))((p,i) => interpolate(i, paramByName(p._1), p._2))
    }

    /**
      * Parse command line parameters into [[FieldValue]] entities, which then can be interpolated via the
      * [[interpolate]] method.
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
      * This method will execute all targets specified in this job in the correct order. It is a convenient wrapper
      * using the [[Runner]] class, which actually takes care about all the details.
      * @param executor
      * @param phase
      * @param args
      * @param force
      * @return
      */
    def execute(executor:Execution, phase:Phase, args:Map[String,String], targets:Seq[Regex]=Seq(".*".r), force:Boolean=false, dryRun:Boolean=false) : Status = {
        require(args != null)
        require(phase != null)
        require(args != null)

        val jobArgs = arguments(args)
        val jobRunner = new Runner(executor, NullStateStore(context))
        jobRunner.executeJob(this, Seq(phase), jobArgs, targets, force=force, dryRun=dryRun, isolated=true)
    }
}
