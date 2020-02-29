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

package com.dimajix.flowman.execution

import org.slf4j.LoggerFactory

import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.JobInstance
import com.dimajix.flowman.model.Target


/**
  * This helper class is used for executing jobs. It will set up an appropriate execution environment that can be
  * reused during multiple phases. The specified arguments need to contain all required parameters (i.e. those without
  * any default value) and must not contain any values for non-existing parameters.
  *
  * @param parentExecutor
  * @param job
  * @param args
  * @param force
  */
class JobExecutor(parentExecutor:Executor, val job:Job, args:Map[String,Any], force:Boolean=false) {
    require(parentExecutor != null)
    require(job != null)
    require(args != null)

    private val logger = LoggerFactory.getLogger(classOf[JobExecutor])

    /** Evaluate final arguments including default values */
    val arguments : Map[String,Any] = job.parameters.flatMap(p => p.default.map(d => p.name -> d)).toMap ++ args

    // Create a new execution environment.
    private val rootContext = RootContext.builder(job.context)
        .withEnvironment("force", force)
        .withEnvironment(arguments, SettingLevel.SCOPE_OVERRIDE)
        .withEnvironment(job.environment, SettingLevel.SCOPE_OVERRIDE)
        .build()

    /** The context that should be used for resolving variables and instantiating objects */
    val context : Context = if (job.context.project.nonEmpty) rootContext.getProjectContext(job.context.project.get) else rootContext
    /** The executor that should be used for running targets */
    val executor : Executor = if (isolated) new ScopedExecutor(parentExecutor) else parentExecutor

    // Check if the job should run isolated. This is required if arguments are specified, which could
    // result in different DataFrames with different arguments
    private def isolated = arguments.nonEmpty || job.environment.nonEmpty

    /**
      * Returns the JobInstance representing the bound job with the given arguments
      * @return
      */
    def instance : JobInstance = job.instance(arguments.map{ case(k,v) => k -> v.toString })

    def environment : Map[String,Any] = context.environment

    /**
      * Executes a single phase of the job. This method will also check if the arguments passed to the constructor
      * are correct and sufficient, otherwise an IllegalArgumentException will be thrown.
      *
      * @param phase
      * @return
      */
    def execute(phase:Phase)(fn:(Executor,Target,Boolean) => Status) : Status = {
        require(phase != null)

        val desc = job.description.map("(" + _ + ")").getOrElse("")
        val args = if (arguments.nonEmpty) s"with arguments ${arguments.map(kv => kv._1 + "=" + kv._2).mkString(", ")}" else ""
        logger.info(s"Running phase '$phase' of job '${job.identifier}' $desc $args")

        // Verify job arguments. This is moved from the constructor into this place, such that only this method throws an exception
        val argNames = arguments.keySet
        val paramNames = job.parameters.map(_.name).toSet
        argNames.diff(paramNames).foreach(p => throw new IllegalArgumentException(s"Unexpected argument '$p' not defined in job '${job.identifier}'"))
        paramNames.diff(argNames).foreach(p => throw new IllegalArgumentException(s"Required parameter '$p' not specified for job '${job.identifier}'"))

        val targets = job.targets.map(t => context.getTarget(t)).filter(_.phases.contains(phase))
        val order = phase match {
            case Phase.DESTROY | Phase.TRUNCATE => TargetOrdering.sort(targets, phase).reverse
            case _ => TargetOrdering.sort(targets, phase)
        }

        logger.info(s"Executing phase '$phase' with sequence: ${order.map(_.identifier).mkString(", ")}")

        Status.ofAll(order) { target => fn(executor,target,force) }
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
}
