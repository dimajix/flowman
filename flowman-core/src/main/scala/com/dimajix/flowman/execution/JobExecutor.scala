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

import com.dimajix.flowman.spec.job.Job
import com.dimajix.flowman.spec.job.JobInstance
import com.dimajix.flowman.spec.target.Target
import com.dimajix.flowman.spec.target.orderTargets


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

    // Resolve all arguments
    private val jobArgs = job.arguments(args)

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
    def instance : JobInstance = job.instance(args)

    def environment : Map[String,Any] = context.environment
    def arguments : Map[String,Any] = jobArgs

    /**
      * Executes a single phase of the job
      * @param phase
      * @return
      */
    def execute(phase:Phase)(fn:(Executor,Target,Boolean) => Status) : Status = {
        require(phase != null)

        val description = job.description.map("(" + _ + ")").getOrElse("")
        val arguments = if (args.nonEmpty) s"with arguments ${args.map(kv => kv._1 + "=" + kv._2).mkString(", ")}" else ""
        logger.info(s"Running phase '$phase' of job '${job.identifier}' $description $arguments")

        // Verify job arguments. This is moved from the constructor into this place, such that only this method throws an exception
        jobArgs.filter(_._2 == null).foreach(p => throw new IllegalArgumentException(s"Parameter '${p._1}' not defined for job '${job.identifier}'"))

        val targets = job.targets.map(t => context.getTarget(t)).filter(_.phases.contains(phase))
        val order = phase match {
            case Phase.DESTROY | Phase.TRUNCATE => orderTargets(targets, phase).reverse
            case _ => orderTargets(targets, phase)
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
