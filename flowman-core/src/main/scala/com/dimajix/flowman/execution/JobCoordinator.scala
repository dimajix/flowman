/*
 * Copyright (C) 2022 The Flowman Authors
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
import com.dimajix.flowman.model.TargetIdentifier
import com.dimajix.flowman.types.FieldValue


class JobCoordinator(session:Session, force:Boolean=false, keepGoing:Boolean=false, dryRun:Boolean=false, parallelism:Int= -1) {
    def execute(job: Job, lifecycle: Seq[Phase], args: Map[String, FieldValue]=Map.empty, targets: Seq[Regex]=Seq(".*".r), dirtyTargets: Seq[Regex]=Seq.empty): Status = {
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
        val phaseTargets = lifecycle.map { p =>
            val executions0 = job.executions
                .filter(_.phase == p)
            // Fall back to default execution if phase not found
            val executions = if (executions0.nonEmpty) executions0 else Seq(Job.Execution(p, CyclePolicy.ALWAYS, Seq(".*".r)))
            // Collect all target regexes within the current Job.Execution
            val activeTargets = executions.flatMap { e =>
                    e.cycle match {
                        case CyclePolicy.ALWAYS => e.targets
                        case CyclePolicy.FIRST if first => e.targets
                        case CyclePolicy.LAST if last => e.targets
                        case _ => Seq.empty
                    }
                }
            p -> activeTargets
        }.filter(_._2.nonEmpty)

        // This will destroy the ordering of the phases!
        val phaseTargetsMap = phaseTargets.toMap

        // Create predicate to select active target according the the executions
        def targetPredicate(phase: Phase, target: TargetIdentifier): Boolean = {
            phaseTargetsMap.get(phase).exists(_.exists(_.unapplySeq(target.name).nonEmpty)) &&
                targets.exists(_.unapplySeq(target.name).nonEmpty)
        }
        def dirtyPredicate(phase: Phase, target: TargetIdentifier): Boolean = {
            dirtyTargets.exists(_.unapplySeq(target.name).nonEmpty)
        }

        val phases = phaseTargets.map(_._1)
        runner.executeJob(job, phases, args, targetPredicate _, dirtyPredicate _, force = force, keepGoing = keepGoing, dryRun = dryRun, ignoreHistory = false, isolated = true)
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
