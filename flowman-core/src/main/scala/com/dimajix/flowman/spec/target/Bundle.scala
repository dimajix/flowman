package com.dimajix.flowman.spec.target

import com.fasterxml.jackson.annotation.JsonProperty
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.RootContext
import com.dimajix.flowman.execution.RootExecutor
import com.dimajix.flowman.execution.SettingLevel
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.history.JobInstance
import com.dimajix.flowman.spec.AbstractInstance
import com.dimajix.flowman.spec.Instance
import com.dimajix.flowman.spec.JobIdentifier
import com.dimajix.flowman.spec.NamedSpec
import com.dimajix.flowman.spec.Namespace
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.spec.TargetIdentifier
import com.dimajix.flowman.spec.task.JobParameter
import com.dimajix.flowman.spec.task.JobParameterSpec
import com.dimajix.flowman.types.FieldType


object Bundle {
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
        override val kind : String = "execution"
   }

    class Builder(context:Context) {
        require(context != null)
        private var name:String = ""
        private var description:Option[String] = None
        private var parameters:Seq[JobParameter] = Seq()
        private var targets:Seq[TargetIdentifier] = Seq()

        def build() : Bundle = Bundle(
            Bundle.Properties(context, name),
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


case class Bundle(
                     instanceProperties:Bundle.Properties,
                     description:Option[String],
                     parameters:Seq[JobParameter],
                     environment:Map[String,String],
                     targets:Seq[TargetIdentifier]
) extends AbstractInstance {
    private val logger = LoggerFactory.getLogger(classOf[Bundle])

    override def category: String = "execution"
    override def kind : String = "execution"

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

    def execute(executor:Executor, args:Map[String,String], phase:Phase, force:Boolean) : Status = {
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
        val projectRunner = projectExecutor.runner

        targets.foreach( id => {
            val target = projectContext.getTarget(id)
            phase.execute(projectRunner, projectExecutor, target)
        })

        // Release any resources
        if (isolated) {
            projectExecutor.cleanup()
        }

        result
    }
}


class BundleSpec extends NamedSpec[Bundle] {
    @JsonProperty(value="description") private var description:Option[String] = None
    @JsonProperty(value="parameters") private var parameters:Seq[JobParameterSpec] = Seq()
    @JsonProperty(value="environment") private var environment: Seq[String] = Seq()
    @JsonProperty(value="targets") private var targets: Seq[String] = Seq()

}
