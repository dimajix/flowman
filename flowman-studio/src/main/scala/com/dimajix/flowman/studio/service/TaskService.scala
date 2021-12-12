/*
 * Copyright 2021 Kaya Kupferschmidt
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

package com.dimajix.flowman.studio.service

import java.util.UUID

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import com.dimajix.flowman.execution.Lifecycle
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.Target



sealed abstract class Task[T] {
    val id:String = UUID.randomUUID().toString

    def result:Future[T]

    final def isComplete:Boolean = result.isCompleted
    final def isRunning:Boolean = !result.isCompleted
    final def value:Option[Try[T]] = result.value
}
case class JobTask(
    job:Job,
    phase:Phase,
    lifecycle: Seq[Phase],
    rawArgs:Map[String,String],
    args:Map[String,Any],
    force:Boolean,
    keepGoing:Boolean,
    dryRun:Boolean,
    override val result:Future[Status]
) extends Task[Status] {
    final def status:Status = {
        if (result.isCompleted) {
            result.value.get match {
                case Success(value) => value
                case Failure(_) => Status.FAILED
            }
        }
        else {
            Status.RUNNING
        }
    }
}


class TaskService(sessionService: SessionService) {
    implicit private val executionContext:ExecutionContext = sessionService.executionContext
    private val tasks = mutable.ListBuffer[Task[_]]()

    private def listType[T : ClassTag]() : Seq[T] = {
        val entities = mutable.ListBuffer[T]()
        tasks.synchronized {
            tasks.foreach {
                case j:T => entities.append(j)
                case _ =>
            }
        }
        entities
    }

    /**
     * Returns a list of all tasks for jobs
     * @return
     */
    def listJobs() : Seq[JobTask] = listType[JobTask]()

    /**
     * Executes a single job
     * @param job
     * @param phase
     * @param args
     * @param force
     * @param keepGoing
     * @param dryRun
     * @return
     */
    def runJob(job:Job, phase:Phase, args:Map[String,String], force:Boolean=false, keepGoing:Boolean=false, dryRun:Boolean=false) : JobTask = {
        val jobArgs = job.arguments(args)
        val lifecycle = Lifecycle.ofPhase(phase)
        val runner = sessionService.runner

        val future = Future {
            runner.executeJob(job, lifecycle, jobArgs, force = force, keepGoing = keepGoing, dryRun = dryRun)
        }

        val task = JobTask(
            job,
            phase,
            lifecycle,
            args,
            jobArgs,
            force = force,
            keepGoing = keepGoing,
            dryRun = dryRun,
            result = future
        )
        val id = task.id

        tasks.synchronized {
            tasks.append(task)
        }
        future.onComplete { status =>
            tasks.synchronized {
                val index = tasks.indexWhere(_.id == id)
                if (index >= 0) {
                    tasks.remove(index)
                }
            }
        }

        task
    }

    def runTarget(target:Target, phase:Phase) : Unit = ???
}
