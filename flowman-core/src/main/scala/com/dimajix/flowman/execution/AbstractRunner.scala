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

import scala.collection.mutable
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.slf4j.Logger

import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spec.Namespace
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.spec.target.Target
import com.dimajix.flowman.state.JobInstance
import com.dimajix.flowman.spec.task.Job
import com.dimajix.flowman.state.JobToken
import com.dimajix.flowman.state.Status
import com.dimajix.flowman.state.TargetInstance
import com.dimajix.flowman.state.TargetToken


object AbstractRunner {
    /**
      * This class is a very thin wrapper around another executor, just to return a different runner
      * @param _parent
      * @param _runner
      */
    class JobExecutor(_parent:Executor, _runner:Runner) extends Executor {
        override def session: Session = _parent.session
        override def project : Project = _parent.project
        override def namespace : Namespace = _parent.namespace
        override def root: Executor = _parent.root
        override def context: Context = _parent.context
        override def runner: Runner = _runner
        override def spark: SparkSession = _parent.spark
        override def sparkRunning: Boolean = _parent.sparkRunning
        override def instantiate(identifier: MappingIdentifier) : DataFrame = _parent.instantiate(identifier)
        override def cleanup() : Unit = _parent.cleanup()
        override protected[execution] def cache : mutable.Map[(String,String),DataFrame] = _parent.cache
    }

}


abstract class AbstractRunner(parentJob:Option[JobToken] = None) extends Runner {
    import com.dimajix.flowman.execution.AbstractRunner.JobExecutor

    protected val logger:Logger

    /**
      * Executes a given job with the given executor. The runner will take care of
      * logging and monitoring
      *
      * @param executor
      * @param job
      * @return
      */
    override def execute(executor: Executor, job:Job, args:Map[String,String] = Map(), force:Boolean=false) : Status = {
        require(executor != null)
        require(args != null)

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

        // Now run the job
        val result = if (job.logged) {
            runLogged(jobExecutor, job, args, force)
        }
        else {
            runUnlogged(jobExecutor, job, args)
        }

        // Release any resources
        if (isolated) {
            jobExecutor.root.cleanup()
        }

        result
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
        val present = checkJob(context, instance)
        val token = startJob(context, instance, parentJob)

        val shutdownHook = new Thread() { override def run() : Unit = finishJob(context, token, Status.FAILED) }
        withShutdownHook(shutdownHook) {
            // First checkJob if execution is really required
            if (present && !force) {
                logger.info(s"Job '${job.name}' with arguments ${args.map(kv => kv._1 + "=" + kv._2).mkString(", ")} is up to date - will be skipped")
                finishJob(context, token, Status.SKIPPED)
                Status.SKIPPED
            }
            else {
                Try {
                    logger.info(s"Running job '${job.name}' with arguments ${args.map(kv => kv._1 + "=" + kv._2).mkString(", ")}")
                    val jobExecutor = new JobExecutor(executor, jobRunner(token))
                    job.execute(jobExecutor, args)
                }
                match {
                    case Success(status @ Status.SUCCESS) =>
                        logger.info(s"Successfully finished execution of job '${job.name}'")
                        finishJob(context, token, Status.SUCCESS)
                        status
                    case Success(status @ Status.FAILED) =>
                        logger.error(s"Execution of job '${job.name}' failed")
                        finishJob(context, token, Status.FAILED)
                        status
                    case Success(status @ Status.ABORTED) =>
                        logger.error(s"Execution of job '${job.name}' aborted")
                        finishJob(context, token, Status.ABORTED)
                        status
                    case Success(status @ Status.SKIPPED) =>
                        logger.error(s"Execution of job '${job.name}' skipped")
                        finishJob(context, token, Status.SKIPPED)
                        status
                    case Success(status @ Status.RUNNING) =>
                        logger.error(s"Execution of job '${job.name}' already running")
                        finishJob(context, token, Status.SKIPPED)
                        status
                    case Success(status) =>
                        logger.error(s"Execution of job '${job.name}' in unknown state. Assuming failure")
                        finishJob(context, token, Status.FAILED)
                        status
                    case Failure(e) =>
                        logger.error(s"Caught exception while executing job '${job.name}'", e)
                        finishJob(context, token, Status.FAILED)
                        Status.FAILED
                }
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
            logger.info(s"Running job '${job.name}' with arguments ${args.map(kv => kv._1 + "=" + kv._2).mkString(", ")}")
            job.execute(executor, args)
        }
        match {
            case Success(status @ Status.SUCCESS) =>
                logger.info(s"Successfully finished execution of job '${job.name}'")
                status
            case Success(status @ Status.FAILED) =>
                logger.error(s"Execution of job '${job.name}' failed")
                status
            case Success(status @ Status.ABORTED) =>
                logger.error(s"Execution of job '${job.name}' aborted")
                status
            case Success(status @ Status.SKIPPED) =>
                logger.error(s"Execution of job '${job.name}' skipped")
                status
            case Success(status @ Status.RUNNING) =>
                logger.error(s"Execution of job '${job.name}'already running")
                status
            case Success(status) =>
                logger.error(s"Execution of job '${job.name}' in unknown state. Assuming failure")
                status
            case Failure(e) =>
                logger.error(s"Caught exception while executing job '${job.name}'", e)
                Status.FAILED
        }
    }

    /**
      * Builds a single target
      */
    override def build(executor: Executor, target: Target, logged:Boolean=true): Status = {
        implicit val context = executor.context

        // Now run the job
        val force = true
        if (logged) {
            buildLogged(executor, target, force)
        }
        else {
            buildUnlogged(executor, target)
        }
    }

    /**
      * Runs the given job in a logged way. This means that appropriate methods will be called on job startJob, finish
      * and failures
      * @param executor
      * @param target
      * @param force
      * @return
      */
    private def buildLogged(executor: Executor, target:Target, force:Boolean) : Status = {
        implicit val context = executor.context

        // Create job instance for state server
        val instance = target.instance(context)

        // Get Token
        val present = checkTarget(context, instance)
        val token = startTarget(context, instance, parentJob)

        val shutdownHook = new Thread() { override def run() : Unit = finishTarget(context, token, Status.FAILED) }
        withShutdownHook(shutdownHook) {
            // First checkJob if execution is really required
            if (present && !force) {
                logger.info("Everything up to date, skipping execution")
                finishTarget(context, token, Status.SKIPPED)
                Status.SKIPPED
            }
            else {
                Try {
                    buildTarget(executor, target)
                }
                match {
                    case Success(_) =>
                        logger.info(s"Successfully finished building target '${target.name}'")
                        finishTarget(context, token, Status.SUCCESS)
                        Status.SUCCESS
                    case Failure(e) =>
                        logger.error(s"Caught exception while building target '${target.name}'", e)
                        finishTarget(context, token, Status.FAILED)
                        Status.FAILED
                }
            }
        }
    }

    /**
      * Runs a job without logging.
      * @param executor
      * @param target
      * @return
      */
    private def buildUnlogged(executor: Executor, target:Target) : Status = {
        implicit val context = executor.context

        Try {
            buildTarget(executor, target)
        }
        match {
            case Success(_) =>
                logger.info(s"Successfully built target '${target.name}'")
                Status.SUCCESS
            case Failure(e) =>
                logger.error(s"Caught exception while building target '${target.name}'", e)
                Status.FAILED
        }
    }

    private def buildTarget(executor: Executor, target:Target) : Unit = {
        implicit val context = executor.context

        logger.info("Resolving dependencies for target '{}'", target.name)
        val dependencies = target.dependencies
            .map(d => (d, executor.instantiate(d)))
            .toMap

        logger.info("Building target '{}'", target.name)
        target.build(executor, dependencies)
    }

    /**
      * Builds a single target
      */
    override def clean(executor: Executor, target: Target, logged:Boolean=true): Status = {
        implicit val context = executor.context

        // Now run the job
        if (logged) {
            cleanLogged(executor, target)
        }
        else {
            cleanUnlogged(executor, target)
        }
    }

    /**
      * Runs the given job in a logged way. This means that appropriate methods will be called on job startJob, finish
      * and failures
      * @param executor
      * @param target
      * @return
      */
    private def cleanLogged(executor: Executor, target:Target) : Status = {
        implicit val context = executor.context

        // Create job instance for state server
        val instance = target.instance(context)

        // Get Token
        val token = startTarget(context, instance, parentJob)

        val shutdownHook = new Thread() { override def run() : Unit = finishTarget(context, token, Status.FAILED) }
        withShutdownHook(shutdownHook) {
            Try {
                target.clean(executor)
            }
            match {
                case Success(_) =>
                    logger.info(s"Successfully finished cleaning target '${target.name}'")
                    finishTarget(context, token, Status.CLEANED)
                    Status.CLEANED
                case Failure(e) =>
                    logger.error(s"Caught exception while cleaning target '${target.name}'", e)
                    finishTarget(context, token, Status.FAILED)
                    Status.FAILED
            }
        }
    }

    /**
      * Runs a job without logging.
      * @param executor
      * @param target
      * @return
      */
    private def cleanUnlogged(executor: Executor, target:Target) : Status = {
        implicit val context = executor.context

        Try {
            target.clean(executor)
        }
        match {
            case Success(_) =>
                logger.info(s"Successfully finished cleaning target '${target.name}'")
                Status.SUCCESS
            case Failure(e) =>
                logger.error("Caught exception while cleaning target target '${target.name}'.", e)
                Status.FAILED
        }
    }

    protected def jobRunner(job:JobToken) : Runner

        /**
      * Performs some checks, if the run is required. If the method returns true, the Job should be run
      * @param job
      * @return
      */
    protected def checkJob(context: Context, job:JobInstance) : Boolean

    /**
      * Starts the run and returns a token, which can be anything
      *
      * @param job
      * @return
      */
    protected def startJob(context: Context, job:JobInstance, parent:Option[JobToken]) : JobToken

    /**
      * Marks a run as a success
      *
      * @param token
      */
    protected def finishJob(context: Context, token:JobToken, status:Status) : Unit

    /**
      * Performs some checks, if the target is already up to date
      * @param target
      * @return
      */
    protected def checkTarget(context: Context, target:TargetInstance) : Boolean

    /**
      * Starts the run and returns a token, which can be anything
      *
      * @param target
      * @return
      */
    protected def startTarget(context: Context, target:TargetInstance, parent:Option[JobToken]) : TargetToken

    /**
      * Marks a run as a success
      *
      * @param token
      */
    protected def finishTarget(context: Context, token:TargetToken, status:Status) : Unit

    private def withShutdownHook[T](shutdownHook:Thread)(block: => T) : T = {
        Runtime.getRuntime.addShutdownHook(shutdownHook)
        val result = block
        Runtime.getRuntime.removeShutdownHook(shutdownHook)
        result
    }

}
