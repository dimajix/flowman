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

import java.util.concurrent.TimeUnit

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.util.Failure
import scala.util.Success

import com.dimajix.flowman.common.ThreadUtils
import com.dimajix.flowman.config.FlowmanConf
import com.dimajix.flowman.model.Target


class ParallelExecutor extends Executor {
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
        val threadPool = ThreadUtils.newThreadPool("ParallelExecutor", parallelism)
        implicit val ec:ExecutionContext = ExecutionContext.fromExecutorService(threadPool)

        // Allocate state variables for tracking overall Status
        val statusLock = new Object
        var error = false
        var skipped = true
        var empty = true

        def executeTarget(target:Target) : Future[Status] = {
            Future {
                fn(execution, target, phase)
            }.andThen { case status =>
                // Inform scheduler that Target is built
                scheduler.synchronized {
                    scheduler.complete(target)
                }

                // Evaluate status
                statusLock.synchronized {
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
        }

        def scheduleTargets(): Seq[Future[Status]] = {
            val tasks = mutable.ListBuffer[Future[Status]]()
            var noMoreWork = false
            while (!noMoreWork) {
                scheduler.synchronized(scheduler.next()) match {
                    case Some(target) =>
                        val task = executeTarget(target)
                        tasks.append(task)
                    case None =>
                        noMoreWork = true
                }
            }
            tasks
        }


        @tailrec
        def wait(tasks:Seq[Future[Status]]) : Unit = {
            val runningTasks = tasks.filter(!_.isCompleted)
            if (runningTasks.nonEmpty) {
                val next = Future.firstCompletedOf(runningTasks)
                Await.ready(next, Duration.Inf)
                wait(tasks.filter(!_.isCompleted))
            }
        }

        @tailrec
        def run(tasks:Seq[Future[Status]] = Seq()) : Unit = {
            // First wait for tasks
            val (finishedTasks,runningTasks) = tasks.partition(_.isCompleted)
            if (finishedTasks.isEmpty && runningTasks.nonEmpty) {
                val next = Future.firstCompletedOf(runningTasks)
                Await.ready(next, Duration.Inf)
            }

            // Schedule new Tasks
            val newTasks = scheduleTargets()
            val allTasks = runningTasks ++ newTasks
            if(scheduler.synchronized(scheduler.hasNext()) && (!error || keepGoing)) {
                run(allTasks)
            }
            else {
                wait(allTasks)
            }
        }

        // Now schedule and execute all targets
        run()

        // Tidy up!
        threadPool.shutdown()
        threadPool.awaitTermination(3600, TimeUnit.SECONDS)

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
