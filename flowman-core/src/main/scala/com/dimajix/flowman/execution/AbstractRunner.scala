/*
 * Copyright 2018 Kaya Kupferschmidt
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

import com.dimajix.flowman.state.JobInstance
import com.dimajix.flowman.spec.task.Job
import com.dimajix.flowman.state.Status


abstract class AbstractRunner extends Runner {
    protected val logger:Logger

    /**
      * Executes a given job with the given executor. The runner will take care of
      * logging and monitoring
      *
      * @param executor
      * @param job
      * @return
      */
    def execute(executor: Executor, job:Job, args:Map[String,String] = Map(), force:Boolean=false) : Status = {
        require(executor != null)
        require(args != null)

        implicit val context = executor.context

        if (job.logged)
            runLogged(executor, job, args, force)
        else
            runUnlogged(executor, job, args)
    }

    /**
      * Runs the given job in a logged way. This means that appropriate methods will be called on job startJob, finish
      * and failures
      * @param executor
      * @param job
      * @param args
      * @param force
      * @return
      */
    private def runLogged(executor: Executor, job:Job, args:Map[String,String], force:Boolean) : Status = {
        implicit val context = executor.context

        // Create job instance for state server
        val instance = job.instance(args)

        // Get Token
        val present = check(context, instance)
        val token = start(context, instance)

        val shutdownHook = new Thread() { override def run() : Unit = failure(context, token) }
        withShutdownHook(shutdownHook) {
            // First checkJob if execution is really required
            if (present && !force) {
                logger.info("Everything up to date, skipping execution")
                skipped(context, token)
                Status.SKIPPED
            }
            else {
                runJob(executor, job, args, token)
            }
        }
    }

    /**
      * Runs a job without logging.
      * @param executor
      * @param job
      * @param args
      * @return
      */
    private def runUnlogged(executor: Executor, job:Job, args:Map[String,String]) : Status = {
        implicit val context = executor.context

        Try {
            job.execute(executor, args)
        }
        match {
            case Success(status @ Status.SUCCESS) =>
                logger.info("Successfully finished execution of Job")
                status
            case Success(status @ Status.FAILED) =>
                logger.error("Execution of Job failed")
                status
            case Success(status @ Status.UNKNOWN) =>
                logger.error("Execution of Job in unknown state. Assuming failure")
                status
            case Success(status @ Status.ABORTED) =>
                logger.error("Execution of Job aborted")
                status
            case Success(status @ Status.SKIPPED) =>
                logger.error("Execution of Job skipped")
                status
            case Success(status @ Status.RUNNING) =>
                logger.error("Execution of Job already running")
                status
            case Failure(e) =>
                logger.error("Caught exception while executing job.", e)
                Status.FAILED
        }
    }

    private def runJob(executor: Executor, job:Job, args:Map[String,String], token:Object) : Status = {
        implicit val context = executor.context

        // Check if the job should run isolated. This is required if arguments are specified, which could
        // result in different DataFrames with different arguments
        val isolated = false //args != null && args.nonEmpty

        // Create a new execution environment. This ensures that all DataFrames are only reused within a single job
        // since other runs may have different parameters
        val jobExecutor = if (isolated) {
            val rootContext = RootContext.builder(context).build()
            val rootExecutor = new RootExecutor(executor.session, rootContext)
            if (context.project != null) rootExecutor.getProjectExecutor(context.project) else rootExecutor
        }
        else {
            executor
        }

        val result = Try {
            job.execute(jobExecutor, args)
        }
        match {
            case Success(status @ Status.SUCCESS) =>
                logger.info("Successfully finished execution of Job")
                success(context, token)
                status
            case Success(status @ Status.FAILED) =>
                logger.error("Execution of Job failed")
                failure(context, token)
                status
            case Success(status @ Status.UNKNOWN) =>
                logger.error("Execution of Job in unknown state. Assuming failure")
                failure(context, token)
                status
            case Success(status @ Status.ABORTED) =>
                logger.error("Execution of Job aborted")
                aborted(context, token)
                status
            case Success(status @ Status.SKIPPED) =>
                logger.error("Execution of Job skipped")
                skipped(context, token)
                status
            case Success(status @ Status.RUNNING) =>
                logger.error("Execution of Job already running")
                skipped(context, token)
                status
            case Failure(e) =>
                logger.error("Caught exception while executing job.", e)
                failure(context, token)
                Status.FAILED
        }

        // Release any resources
        if (isolated) {
            jobExecutor.root.cleanup()
        }

        result
    }

    /**
      * Performs some checkJob, if the run is required
      * @param job
      * @return
      */
    protected def check(context: Context, job:JobInstance) : Boolean

    /**
      * Starts the run and returns a token, which can be anything
      *
      * @param job
      * @return
      */
    protected def start(context: Context, job:JobInstance) : Object

    /**
      * Marks a run as a success
      *
      * @param token
      */
    protected def success(context: Context, token:Object) : Unit

    /**
      * Marks a run as a failure
      *
      * @param token
      */
    protected def failure(context: Context, token:Object) : Unit

    /**
      * Marks a run as a failure
      *
      * @param token
      */
    protected def aborted(context: Context, token:Object) : Unit

    /**
      * Marks a run as being skipped
      *
      * @param token
      */
    protected def skipped(context: Context, token:Object) : Unit

    private def withShutdownHook[T](shutdownHook:Thread)(block: => T) : T = {
        Runtime.getRuntime.addShutdownHook(shutdownHook)
        val result = block
        Runtime.getRuntime.removeShutdownHook(shutdownHook)
        result
    }

}
