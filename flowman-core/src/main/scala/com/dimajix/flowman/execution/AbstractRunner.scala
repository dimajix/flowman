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

package com.dimajix.flowman.execution

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import org.slf4j.Logger

import com.dimajix.flowman.history.JobToken
import com.dimajix.flowman.history.TargetToken
import com.dimajix.flowman.metric.MetricSystem
import com.dimajix.flowman.metric.withWallTime
import com.dimajix.flowman.spec.job.Job
import com.dimajix.flowman.spec.job.JobInstance
import com.dimajix.flowman.spec.target.Target
import com.dimajix.flowman.spec.target.TargetInstance


abstract class AbstractRunner extends Runner {
    protected val logger:Logger

    /**
      * Executes a single job using the given executor and a map of parameters. The Runner may decide not to
      * execute a specific job, because some information may indicate that the job has already been successfully
      * run in the past. This behaviour can be overriden with the force flag
      * @param executor
      * @param job
      * @param phases
      * @param args
      * @param force
      * @return
      */
    override def executeJob(executor:Executor, job:Job, phases:Seq[Phase], args:Map[String,String], force:Boolean=false) : Status = {
        require(args != null)
        require(phases != null)
        require(args != null)

        val jobExecutor = new JobExecutor(executor, job, args, force)

        logger.info(s"Executing phases ${phases.map(p => "'" + p + "'").mkString(",")} for job '${job.identifier}'")
        jobExecutor.arguments.toSeq.sortBy(_._1).foreach { case (k,v) => logger.info(s"Job argument $k=$v")}
        jobExecutor.environment.toSeq.sortBy(_._1).foreach { case (k,v) => logger.info(s"Job environment $k=$v")}

        val result = Status.ofAll(phases){ phase =>
            withMetrics(jobExecutor.context, job, phase, executor.metrics) {
                executeJobPhase(jobExecutor, phase)
            }
        }

        jobExecutor.cleanup()

        result
    }

    private def executeJobPhase(executor:JobExecutor, phase:Phase) : Status = {
        require(executor != null)
        require(phase != null)

        val result = withWallTime(executor.executor.metrics, executor.job.metadata, phase) {
            // Create job instance for state server
            val instance = executor.instance
            val job = executor.job

            // Get Token
            val token = startJob(instance, phase)

            val shutdownHook = new Thread() { override def run() : Unit = finishJob(token, Status.FAILED) }
            withShutdownHook(shutdownHook) {
                Try {
                    executor.execute(phase) { (executor,target,force) =>
                        executeTarget(executor, target, phase, Some(token), force)
                    }
                }
                match {
                    case Success(status @ Status.SUCCESS) =>
                        logger.info(s"Successfully finished phase '$phase' of job '${job.identifier}'")
                        finishJob(token, Status.SUCCESS)
                        status
                    case Success(status @ Status.FAILED) =>
                        logger.error(s"Execution of phase '$phase' of job '${job.identifier}' failed")
                        finishJob(token, Status.FAILED)
                        status
                    case Success(status @ Status.ABORTED) =>
                        logger.error(s"Execution of phase '$phase' of job '${job.identifier}' aborted")
                        finishJob(token, Status.ABORTED)
                        status
                    case Success(status @ Status.SKIPPED) =>
                        logger.error(s"Execution of phase '$phase' of job '${job.identifier}' skipped")
                        finishJob(token, Status.SKIPPED)
                        status
                    case Success(status @ Status.RUNNING) =>
                        logger.error(s"Execution of phase '$phase' of job '${job.identifier}' already running")
                        finishJob(token, Status.SKIPPED)
                        status
                    case Success(status) =>
                        logger.error(s"Execution of phase '$phase' of job '${job.identifier}' in unknown state. Assuming failure")
                        finishJob(token, Status.FAILED)
                        status
                    case Failure(e) =>
                        logger.error(s"Caught exception while executing phase '$phase' of job '${job.identifier}'", e)
                        finishJob(token, Status.FAILED)
                        Status.FAILED
                }
            }
        }

        result
    }

    /**
      * Executes a single job using the given executor and a map of parameters. The Runner may decide not to
      * execute a specific job, because some information may indicate that the job has already been successfully
      * run in the past. This behaviour can be overriden with the force flag
      * @param executor
      * @param target
      * @param phase
      * @param force
      * @return
      */
    override def executeTarget(executor: Executor, target:Target, phase:Phase, job:Option[JobToken]=None, force:Boolean) : Status = {
        // Create job instance for state server
        val instance = target.instance

        // Get Token
        val present = checkTarget(instance, phase)
        val token = startTarget(instance, phase, job)

        val shutdownHook = new Thread() { override def run() : Unit = finishTarget(token, Status.FAILED) }
        withShutdownHook(shutdownHook) {
            // First checkJob if execution is really required
            if (present && !force) {
                logger.info("Everything up to date, skipping execution")
                finishTarget(token, Status.SKIPPED)
                Status.SKIPPED
            }
            else {
                Try {
                    logger.info(s"Running phase '$phase' of target '${target.identifier}'")
                    withWallTime(executor.metrics, target.metadata, phase) {
                        target.execute(executor, phase)
                    }
                }
                match {
                    case Success(_) =>
                        logger.info(s"Successfully finished phase '$phase' for target '${target.identifier}'")
                        finishTarget(token, Status.SUCCESS)
                        Status.SUCCESS
                    case Failure(e) =>
                        logger.error(s"Caught exception while executing phase '$phase' for target '${target.identifier}'", e)
                        finishTarget(token, Status.FAILED)
                        Status.FAILED
                }
            }
        }
    }

    /**
      * Starts the run and returns a token, which can be anything
      *
      * @param batch
      * @return
      */
    protected def startJob(batch:JobInstance, phase:Phase) : JobToken

    /**
      * Marks a run as a success
      *
      * @param token
      */
    protected def finishJob(token:JobToken, status:Status) : Unit

    /**
      * Performs some checks, if the target is already up to date
      * @param target
      * @return
      */
    protected def checkTarget(target:TargetInstance, phase:Phase) : Boolean

    /**
      * Starts the run and returns a token, which can be anything
      *
      * @param target
      * @return
      */
    protected def startTarget(target:TargetInstance, phase:Phase, parent:Option[JobToken]) : TargetToken

    /**
      * Marks a run as a success
      *
      * @param token
      */
    protected def finishTarget(token:TargetToken, status:Status) : Unit

    private def withShutdownHook[T](shutdownHook:Thread)(block: => T) : T = {
        Runtime.getRuntime.addShutdownHook(shutdownHook)
        val result = block
        Runtime.getRuntime.removeShutdownHook(shutdownHook)
        result
    }

    private def withMetrics(context:Context, job:Job, phase:Phase, metricSystem:MetricSystem)(fn: => Status) : Status = {
        // Create new local context which only provides the current phase as an additional environment variable
        val metricContext = ScopeContext.builder(context)
            .withEnvironment("phase", phase.toString)
            .build()

        val metrics = job.metrics.map(_.instantiate(metricContext, metricSystem))

        // Publish metrics
        metrics.foreach { metrics =>
            metrics.reset()
            metricSystem.addBoard(metrics)
        }

        // Run original function
        var result:Status = Status.UNKNOWN
        try {
            result = fn
        }
        finally {
            // Unpublish metrics
            metrics.foreach { metrics =>
                // Do not publish metrics for skipped jobs
                if (result != Status.SKIPPED) {
                    metricSystem.commitBoard(metrics)
                }
                metricSystem.removeBoard(metrics)
            }
        }

        result
    }
}
