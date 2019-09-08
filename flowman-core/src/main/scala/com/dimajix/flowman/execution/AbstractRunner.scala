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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.slf4j.Logger

import com.dimajix.common.IdentityHashMap
import com.dimajix.flowman.hadoop.FileSystem
import com.dimajix.flowman.history.BatchToken
import com.dimajix.flowman.history.TargetToken
import com.dimajix.flowman.metric.MetricSystem
import com.dimajix.flowman.metric.withWallTime
import com.dimajix.flowman.spec.Namespace
import com.dimajix.flowman.spec.flow.Mapping
import com.dimajix.flowman.spec.target.Batch
import com.dimajix.flowman.spec.target.BatchInstance
import com.dimajix.flowman.spec.target.Target
import com.dimajix.flowman.spec.target.TargetInstance


object AbstractRunner {
    /**
      * This class is a very thin wrapper around another executor, just to return a different runner
      * @param _parent
      * @param _runner
      */
    class JobExecutor(_parent:Executor, _runner:Runner) extends Executor {
        override def session: Session = _parent.session
        override def namespace : Namespace = _parent.namespace
        override def root: Executor = _parent.root
        override def runner: Runner = _runner
        override def metrics: MetricSystem = _parent.metrics
        override def fs: FileSystem = _parent.fs
        override def spark: SparkSession = _parent.spark
        override def sparkRunning: Boolean = _parent.sparkRunning
        override def instantiate(mapping: Mapping) : Map[String,DataFrame] = _parent.instantiate(mapping)
        override def instantiate(mapping: Mapping, output:String) : DataFrame = _parent.instantiate(mapping, output)
        override def cleanup() : Unit = _parent.cleanup()
        override protected[execution] def cache : IdentityHashMap[Mapping,Map[String,DataFrame]] = _parent.cache
    }
}


abstract class AbstractRunner(parentJob:Option[BatchToken] = None) extends Runner {
    import com.dimajix.flowman.execution.AbstractRunner.JobExecutor

    protected val logger:Logger

    /**
      * Executes a given job with the given executor. The runner will take care of
      * logging and monitoring
      *
      * @param executor
      * @param batch
      * @return
      */
    override def execute(executor: Executor, batch:Batch, phase:Phase, args:Map[String,String], force:Boolean) : Status = {
        require(executor != null)
        require(args != null)

        val result = withWallTime(executor.metrics, batch.metadata) {
            // Create job instance for state server
            val instance = batch.instance(args)

            // Get Token
            val token = startBatch(instance, phase)

            val shutdownHook = new Thread() { override def run() : Unit = finishBatch(token, Status.FAILED) }
            withShutdownHook(shutdownHook) {
                Try {
                    logger.info(s"Running phase '$phase' of execution '${batch.identifier}' with arguments ${args.map(kv => kv._1 + "=" + kv._2).mkString(", ")}")
                    val jobExecutor = new JobExecutor(executor, jobRunner(token))
                    batch.execute(jobExecutor, args, phase, force)
                }
                match {
                    case Success(status @ Status.SUCCESS) =>
                        logger.info(s"Successfully finished phase '$phase' of execution of bundle '${batch.identifier}'")
                        finishBatch(token, Status.SUCCESS)
                        status
                    case Success(status @ Status.FAILED) =>
                        logger.error(s"Execution of phase '$phase' of bundle '${batch.identifier}' failed")
                        finishBatch(token, Status.FAILED)
                        status
                    case Success(status @ Status.ABORTED) =>
                        logger.error(s"Execution of phase '$phase' of bundle '${batch.identifier}' aborted")
                        finishBatch(token, Status.ABORTED)
                        status
                    case Success(status @ Status.SKIPPED) =>
                        logger.error(s"Execution of phase '$phase' of bundle '${batch.identifier}' skipped")
                        finishBatch(token, Status.SKIPPED)
                        status
                    case Success(status @ Status.RUNNING) =>
                        logger.error(s"Execution of phase '$phase' of bundle '${batch.identifier}' already running")
                        finishBatch(token, Status.SKIPPED)
                        status
                    case Success(status) =>
                        logger.error(s"Execution of phase '$phase' of bundle '${batch.identifier}' in unknown state. Assuming failure")
                        finishBatch(token, Status.FAILED)
                        status
                    case Failure(e) =>
                        logger.error(s"Caught exception while executing phase '$phase' of bundle '${batch.identifier}'", e)
                        finishBatch(token, Status.FAILED)
                        Status.FAILED
                }
            }
        }

        result
    }

    override def execute(executor: Executor, target:Target, phase:Phase, force:Boolean) : Status = {
        // Create job instance for state server
        val instance = target.instance

        // Get Token
        val present = checkTarget(instance, phase)
        val token = startTarget(instance, phase, parentJob)

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
                    withWallTime(executor.metrics, target.metadata) {
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

    protected def jobRunner(job:BatchToken) : Runner

    /**
      * Starts the run and returns a token, which can be anything
      *
      * @param batch
      * @return
      */
    protected def startBatch(batch:BatchInstance, phase:Phase) : BatchToken

    /**
      * Marks a run as a success
      *
      * @param token
      */
    protected def finishBatch(token:BatchToken, status:Status) : Unit

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
    protected def startTarget(target:TargetInstance, phase:Phase, parent:Option[BatchToken]) : TargetToken

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
}
