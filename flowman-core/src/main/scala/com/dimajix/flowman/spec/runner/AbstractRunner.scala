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

package com.dimajix.flowman.spec.runner

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.task.Job
import com.dimajix.flowman.spec.task.JobStatus


abstract class AbstractRunner extends Runner {
    private val logger = LoggerFactory.getLogger(classOf[JdbcLoggedRunner])

    /**
      * Executes a given job with the given executor. The runner will take care of
      * logging and monitoring
      *
      * @param executor
      * @param job
      * @return
      */
    def execute(executor: Executor, job:Job, args:Map[String,String] = Map()) : JobStatus = {
        implicit val context = executor.context

        // Get Monitor
        val present = check(context, job, args)
        val token = start(context, job, args)

        val shutdownHook = new Thread() { override def run() : Unit = failure(context, token) }
        withShutdownHook(shutdownHook) {
            // First check if execution is really required
            if (present) {
                logger.info("Everything up to date, skipping execution")
                skipped(context, token)
                JobStatus.SKIPPED
            }
            else {
                runJob(executor, job, args, token)
            }
        }
    }

    private def runJob(executor: Executor, job:Job, args:Map[String,String], token:Object) : JobStatus = {
        implicit val context = executor.context
        Try {
            job.execute(executor, args)
        }
        match {
            case Success(status @ JobStatus.SUCCESS) =>
                logger.info("Successfully finished execution of Job")
                success(context, token)
                status
            case Success(status @ JobStatus.FAILURE) =>
                logger.error("Execution of Job failed")
                failure(context, token)
                status
            case Success(status @ JobStatus.ABORTED) =>
                logger.error("Execution of Job aborted")
                aborted(context, token)
                status
            case Success(status @ JobStatus.SKIPPED) =>
                logger.error("Execution of Job skipped")
                skipped(context, token)
                status
            case Failure(e) =>
                logger.error("Caught exception while executing job.", e)
                failure(context, token)
                JobStatus.FAILURE
        }
    }

    /**
      * Performs some check, if the run is required
      * @param context
      * @return
      */
    protected def check(context:Context, job:Job, args:Map[String,String]) : Boolean

    /**
      * Starts the run and returns a token, which can be anything
      *
      * @param context
      * @return
      */
    protected def start(context:Context, job:Job, args:Map[String,String]) : Object

    /**
      * Marks a run as a success
      *
      * @param context
      * @param token
      */
    protected def success(context: Context, token:Object) : Unit

    /**
      * Marks a run as a failure
      *
      * @param context
      * @param token
      */
    protected def failure(context: Context, token:Object) : Unit

    /**
      * Marks a run as a failure
      *
      * @param context
      * @param token
      */
    protected def aborted(context: Context, token:Object) : Unit

    /**
      * Marks a run as being skipped
      *
      * @param context
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
