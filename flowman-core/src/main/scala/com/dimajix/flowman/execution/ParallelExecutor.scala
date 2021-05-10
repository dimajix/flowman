/*
 * Copyright 2018-2021 Kaya Kupferschmidt
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

import java.lang.Thread.UncaughtExceptionHandler
import java.util.concurrent.ForkJoinPool

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.util.Failure
import scala.util.Success
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.ForkJoinWorkerThread

import org.slf4j.LoggerFactory

import com.dimajix.common.IdentityHashMap
import com.dimajix.flowman.config.FlowmanConf
import com.dimajix.flowman.model.Target


class ParallelExecutor extends Executor {
    private val logger = LoggerFactory.getLogger(classOf[SimpleExecutor])

    private class MyForkJoinWorkerThread(pool: ForkJoinPool) extends ForkJoinWorkerThread(pool) { // set the correct classloader here
        setContextClassLoader(Thread.currentThread.getContextClassLoader)
    }
    private object MyForkJoinWorkerThreadFactory extends ForkJoinPool.ForkJoinWorkerThreadFactory {
        override final def newThread(pool: ForkJoinPool) = new MyForkJoinWorkerThread(pool)
    }
    private val exceptionHandler = new UncaughtExceptionHandler {
        override def uncaughtException(thread: Thread, throwable: Throwable): Unit = {
            logger.error("Uncaught exception: ", throwable)
        }
    }


    /**
     * Executes a list of targets in an appropriate order.
     *
     * @param execution
     * @param context
     * @param phase - Phase to execute
     * @param targets - List of all targets, even those which should not be executed
     * @param filter - Filter predicate to find all targets to be execution
     * @param keepGoing - True if errors in one target should not stop other targets from being executed
     * @param fn - Function to call. Note that the function is expected not to throw a non-fatal exception.
     * @return
     */
    def execute(execution: Execution, context:Context, phase: Phase, targets: Seq[Target], filter:Target => Boolean, keepGoing: Boolean)(fn:(Execution,Target,Phase) => Status) : Status = {
        val clazz = execution.flowmanConf.getConf(FlowmanConf.EXECUTION_SCHEDULER_CLASS)
        val ctor = clazz.getDeclaredConstructor()
        val scheduler = ctor.newInstance()

        scheduler.initialize(targets, phase, filter)

        val parallelism = execution.flowmanConf.getConf(FlowmanConf.EXECUTION_EXECUTOR_PARALLELISM)
        val threadPool = new ForkJoinPool(parallelism, MyForkJoinWorkerThreadFactory, exceptionHandler, true)
        implicit val ec:ExecutionContext = ExecutionContext.fromExecutorService(threadPool)

        val tasks = IdentityHashMap[Target, Future[Status]]()
        var error = false
        var skipped = true
        var empty = true

        def executeTarget(target:Target) : Unit = {
            val task = Future {
                    fn(execution, target, phase)
                }.andThen { case status =>
                    // Inform scheduler that Target is built
                    scheduler.synchronized {
                        scheduler.complete(target)
                    }

                    // Remove task
                    tasks.synchronized {
                        tasks.remove(target)

                        // Evaluate status (still in 'tasks' lock)
                        status match {
                            case Success(status) =>
                                empty = false
                                error |= (status != Status.SUCCESS && status != Status.SKIPPED)
                                skipped &= (status == Status.SKIPPED)
                                status
                            case Failure(_) =>
                                empty = false
                                error = true
                                Status.FAILED
                        }
                    }
                }

            // Append future to list of running futures
            tasks.synchronized {
                tasks.put(target, task)
            }
        }

        def scheduleTargets(): Unit = {
            var noMoreWork = false
            while (!noMoreWork) {
                scheduler.synchronized(scheduler.next()) match {
                    case Some(target) => executeTarget(target)
                    case None => noMoreWork = true
                }
            }
        }

        // Run all targets and schedule new targets as they become runnable
        while(scheduler.synchronized(scheduler.hasNext()) && (!error || keepGoing)) {
            scheduleTargets()

            val next = tasks.synchronized(Future.firstCompletedOf(tasks.values))
            Await.ready(next, Duration.Inf)
        }

        threadPool.shutdown()

        // Evaluate overall status
        if (empty)
            Status.SUCCESS
        else if (error)
            Status.FAILED
        else if (skipped)
            Status.SKIPPED
        else
            Status.SUCCESS
    }
}
