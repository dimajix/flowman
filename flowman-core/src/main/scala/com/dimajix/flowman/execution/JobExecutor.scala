package com.dimajix.flowman.execution

import scala.collection.mutable

import org.slf4j.LoggerFactory

import com.dimajix.flowman.history.JobToken
import com.dimajix.flowman.spec.TargetIdentifier
import com.dimajix.flowman.spec.job.Job
import com.dimajix.flowman.spec.target.Target


/**
  * This helper class is used for executing jobs. It will set up an appropriate execution environment that can be
  * reused during multiple phases
  * @param parentExecutor
  * @param job
  * @param args
  * @param force
  */
class JobExecutor(parentExecutor:Executor, val job:Job, val args:Map[String,String], force:Boolean=false) {
    require(parentExecutor != null)
    require(job != null)
    require(args != null)

    private val logger = LoggerFactory.getLogger(classOf[JobExecutor])

    // Create a new execution environment.
    private val jobArgs = job.arguments(args)
    jobArgs.filter(_._2 == null).foreach(p => throw new IllegalArgumentException(s"Parameter '${p._1}' not defined for job '${job.name}'"))

    // Create a new execution environment.
    private val rootContext = RootContext.builder(job.context)
        .withEnvironment("force", force)
        .withEnvironment(jobArgs, SettingLevel.SCOPE_OVERRIDE)
        .withEnvironment(job.environment, SettingLevel.SCOPE_OVERRIDE)
        .build()

    /** The executor that should be used for running targets */
    val executor = new RootExecutor(parentExecutor, isolated)
    /** The contetx that shoukd be used for resolving variables and instantiating objects */
    val context = if (job.context.project != null) rootContext.getProjectContext(job.context.project) else rootContext


    // Check if the job should run isolated. This is required if arguments are specified, which could
    // result in different DataFrames with different arguments
    private def isolated = args.nonEmpty || job.environment.nonEmpty

    /**
      * Returns the JobInstance representing the bound job with the given arguments
      * @return
      */
    def instance = job.instance(args)

    /**
      * Executes a single phase of the job
      * @param phase
      * @param jobToken
      * @return
      */
    def execute(phase:Phase, jobToken:Option[JobToken] = None) : Status = {
        require(phase != null)

        val description = job.description.map("(" + _ + ")").getOrElse("")
        logger.info(s"Running phase '$phase' of job '${job.name}' $description with arguments ${args.map(kv => kv._1 + "=" + kv._2).mkString(", ")}")

        val order = phase match {
            case Phase.DESTROY | Phase.TRUNCATE => orderTargets(context, phase).reverse
            case _ => orderTargets(context, phase)
        }

        val runner = executor.runner
        Status.forall(order) { target =>
            runner.executeTarget(executor, target, phase, jobToken, force)
        }
    }

    /**
      * Releases any resources created during execution.
      */
    def cleanup() : Unit = {
        // Release any resources
        if (isolated) {
            executor.cleanup()
        }
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

        val targets = job.targets.map(t => context.getTarget(t))
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
}
