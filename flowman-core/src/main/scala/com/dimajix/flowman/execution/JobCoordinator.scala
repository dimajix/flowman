/*
 * Copyright 2022 Kaya Kupferschmidt
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

import scala.util.matching.Regex

import com.dimajix.flowman.model.Job
import com.dimajix.flowman.types.FieldValue


class JobCoordinator(session:Session, noLifecycle:Boolean=false, force:Boolean=false, keepGoing:Boolean=false, dryRun:Boolean=false, parallelism:Int= -1) {
    private val defaultPhases = Map(
        Phase.VALIDATE -> PhaseExecutionPolicy.ALWAYS,
        Phase.CREATE -> PhaseExecutionPolicy.ALWAYS,
        Phase.BUILD -> PhaseExecutionPolicy.ALWAYS,
        Phase.VERIFY -> PhaseExecutionPolicy.ALWAYS,
        Phase.TRUNCATE -> PhaseExecutionPolicy.ALWAYS,
        Phase.DESTROY -> PhaseExecutionPolicy.ALWAYS
    )

    def execute(job:Job, phase:Phase, args:Map[String,FieldValue]=Map.empty, targets:Seq[Regex]=Seq(".*".r), dirtyTargets:Seq[Regex]=Seq.empty) : Status = {
        val lifecycle =
            if (noLifecycle)
                Seq(phase)
            else
                Lifecycle.ofPhase(phase)

        execute(job, lifecycle, args, targets, dirtyTargets)
    }

    def execute(job: Job, lifecycle: Seq[Phase], args: Map[String, FieldValue], targets: Seq[Regex], dirtyTargets: Seq[Regex]): Status = {
        if (parallelism > 1)
            executeParallel(job, args, lifecycle, targets, dirtyTargets)
        else
            executeLinear(job, args, lifecycle, targets, dirtyTargets)
    }

    private def executeLinear(job: Job, args: Map[String, FieldValue], lifecycle: Seq[Phase], targets:Seq[Regex], dirtyTargets:Seq[Regex]): Status = {
        val instances = interpolate(job, args)
        Status.ofAll(instances, keepGoing = keepGoing) { case(args,first,last) =>
            executeInstance(job, args, lifecycle, targets, dirtyTargets, first, last)
        }
    }

    private def executeParallel(job: Job, args: Map[String, FieldValue], lifecycle: Seq[Phase], targets:Seq[Regex], dirtyTargets:Seq[Regex]): Status = {
        val instances = interpolate(job, args)
        Status.parallelOfAll(instances.toSeq, parallelism, keepGoing = keepGoing, prefix = "JobExecution") { case(args,first,last) =>
            executeInstance(job, args, lifecycle, targets, dirtyTargets, first, last)
        }
    }

    private def executeInstance(job:Job, args:Map[String,Any], lifecycle: Seq[Phase], targets:Seq[Regex], dirtyTargets:Seq[Regex], first:Boolean, last:Boolean) : Status = {
        val runner = session.runner
        val phases = lifecycle.flatMap { p =>
            val policy = job.phases.get(p)
                .orElse(defaultPhases.get(p))
                .getOrElse(PhaseExecutionPolicy.ALWAYS)
            policy match {
                case PhaseExecutionPolicy.ALWAYS => Some(p)
                case PhaseExecutionPolicy.FIRST if first => Some(p)
                case PhaseExecutionPolicy.LAST if last => Some(p)
                case _ => None
            }
        }
        runner.executeJob(job, phases, args, targets, dirtyTargets = dirtyTargets, force = force, keepGoing = keepGoing, dryRun = dryRun, isolated = true)
    }

    private def interpolate(job:Job, args:Map[String, FieldValue]) = {
        val i = job.interpolate(args)
        new Iterable[(Map[String,Any],Boolean,Boolean)] {
            override def iterator: Iterator[(Map[String, Any], Boolean, Boolean)] = new Iterator[(Map[String, Any], Boolean, Boolean)] {
                val delegate = i.iterator
                var first = true

                override def hasNext: Boolean = delegate.hasNext

                override def next(): (Map[String, Any], Boolean, Boolean) = {
                    val rvalue = delegate.next()
                    val rfirst = first
                    val rlast = !delegate.hasNext
                    first = false
                    (rvalue, rfirst, rlast)
                }
            }
        }
    }
}
