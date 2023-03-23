/*
 * Copyright (C) 2021 The Flowman Authors
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

import scala.annotation.tailrec
import scala.util.control.NonFatal

import org.slf4j.LoggerFactory

import com.dimajix.common.IdentityHashMap
import com.dimajix.common.IdentityHashSet
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetIdentifier


class DependencyScheduler(execution: Execution, context: Context) extends Scheduler {
    private val logger = execution.loggerFactory.getLogger(classOf[DependencyScheduler].getName)

    // Map containing all dependencies of all targets. Each entry represents the list of dependencies which
    // have to be executed before the target itself
    private var dependencies = IdentityHashMap[Target,IdentityHashSet[Target]]()
    private var running = IdentityHashSet[Target]()
    private var phase:Phase = Phase.BUILD
    private var filter:Target => Boolean = (Target) => true

    /**
     * Initializes the [[Scheduler]] with a new list of targets to be processed. Note that the specified
     * list of targets may be larger than what actually should be processed. The [[Scheduler]] needs to
     * take into account the processing [[Phase]] and the given filter
     *
     * @param targets Contains the list of all targets required for finding a correct sequence
     * @param phase Specified the processing phase
     * @param filter Specifies a filter
     */
    override def initialize(targets: Seq[Target], phase:Phase, filter:Target => Boolean) : Unit = {
        if (logger.isDebugEnabled) {
            targets.foreach { t =>
                logger.debug(s"Analyzing build phase '$phase' of target '${t.identifier}'")
                t.requires(phase).foreach(r => logger.debug(s"  requires $r"))
                t.provides(phase).foreach(r => logger.debug(s"  provides $r"))
            }
        }

        // Initialize dependencies map with targets as keys
        val dependencies = IdentityHashMap[Target,IdentityHashSet[Target]]()
        targets.foreach(t => dependencies.put(t, IdentityHashSet[Target]()))

        // Insert all explicit 'before' and 'after' dependencies
        addExplicitDependencies(targets, dependencies)

        // Insert all implicit 'requires' dependencies
        addResourceDependencies(targets, phase, dependencies)

        // Finally reverse ordering for TRUNCATE and DESTROY
        this.dependencies = phase match {
            case Phase.DESTROY | Phase.TRUNCATE =>
                // Reverse dependencies in case
                val newDeps = IdentityHashMap[Target,IdentityHashSet[Target]]()
                dependencies.keys.foreach(tgt => newDeps.put(tgt, IdentityHashSet[Target]()))
                dependencies.foreach { case (tgt,deps) =>
                    deps.foreach(d => newDeps(d).add(tgt))
                }
                newDeps
            case _ =>
                dependencies
        }
        this.running = IdentityHashSet[Target]()
        this.phase = phase
        this.filter = filter

        logger.debug(s"Dependencies of phase '$phase'")
        this.dependencies.foreach { case(n,deps) =>
            logger.debug(s"  ${n.identifier}  depends on  ${deps.map(_.identifier.toString).mkString(",")}")
        }
    }

    /**
     * Marks a given target as complete after it has been processed. This method will unblock all targets waiting
     * for this target to be finished
     * @param target
     */
    override def complete(target: Target): Unit = {
        // Check that target is not in dependencies
        if (dependencies.contains(target) || !running.contains(target))
            throw new IllegalArgumentException("Removing unprocessed target is not allowed")

        // Remove target from list of running targets
        running.remove(target)

        // Remove the given target from all dependencies
        removeDependency(target)
    }

    /**
     * Returns the next target to be processed. The method may return [[None]] in case that there are still
     * unprocessed targets, but their dependencies are still not processed
     * @return
     */
    override def next(): Option[Target] = {
        @tailrec
        def getCandidates() : Iterable[Target] = {
            val candidates = dependencies.filter(_._2.isEmpty).keys

            // Check for cyclic dependencies: No candidates found, but dependencies non-empty and no running targets
            if (candidates.isEmpty && dependencies.nonEmpty && running.isEmpty) {
                val deps = dependencies.map { case(k,v) =>
                    s"""  ${k.identifier}
                       |     depends on: ${v.map(_.identifier.toString).mkString(", ")}
                       |     provides: ${k.provides(phase).map(_.text).mkString(", ")}
                       |     requires: ${k.requires(phase).map(_.text).mkString(", ")}""".stripMargin
                }.mkString("\n")
                throw new RuntimeException(s"Cannot create target order, probably due to cyclic dependencies.\n$deps")
            }

            val (actives,inactives) = candidates.partition(t => t.phases.contains(phase) && filter(t))

            // Remove all inactive candidates from dependencies
            inactives.foreach { t =>
                removeTarget(t)
                removeDependency(t)
            }

            // If only inactive targets have been found, try again
            if (actives.isEmpty && candidates.nonEmpty)
                getCandidates()
            else
                actives
        }

        val candidates = getCandidates()
        val result = selectNext(candidates)

        result.foreach { t =>
            // Add target to list of running targets
            running.add(t)
            // Remove target from dependencies, so it won't be returned next time
            removeTarget(t)
        }

        result
    }

    /**
     * Returns [[true]] if more work is to be done. But [[true]] does not mandate that [[next]] will return a
     * valid target, instead it may also return [[None]]
     * @return
     */
    override def hasNext(): Boolean = {
        dependencies.nonEmpty
    }

    /**
     * Selects the next target to be emitted from a list of candidate targets. The candidate targets are known to
     * be free of blocking dependencies.
     * @param candidates
     * @return
     */
    protected def selectNext(candidates:Iterable[Target]) : Option[Target] = {
        // Sort candidates, so the selection algorithm is deterministic
        candidates.toSeq.sortBy(_.name).headOption
    }

    /**
     * Adds a new target (without any dependencies). If the target is already present, its dependencies will not change
     * @param target
     */
    protected def addTarget(target:Target) : Unit = {
        if (!dependencies.contains(target)) {
            dependencies.put(target, IdentityHashSet[Target]())
        }
    }

    /**
     * Removes a target from the list of to-be-processed targets. This should be called for every target returned
     * in [[next]]
     * @param target
     */
    protected def removeTarget(target:Target) : Unit = {
        dependencies.remove(target)
    }

    /**
     * Removes a target as a dependency from all remaining targets. This should be called in [[complete]]
     *
     * @param dependency
     */
    protected def removeDependency(dependency:Target) : Unit = {
        dependencies.foreach { case (_,v) => v.remove(dependency) }
    }

    /**
     * Explicitly adds a new dependency to a target. This method may be called in [[next]] when newly created targets
     * are emitted
     * @param target
     * @param dependency
     */
    protected def addDependency(target:Target, dependency:Target) : Unit = {
        dependencies.getOrElseUpdate(target, IdentityHashSet[Target]()).add(dependency)
    }

    /**
     * Adds all [[Target.before]] and [[Target.after]] dependencies
     * @param targets
     * @param dependencies
     */
    private def addExplicitDependencies(targets:Seq[Target], dependencies: IdentityHashMap[Target,IdentityHashSet[Target]]) : Unit = {
        val targetsById = targets.map(t => (t.identifier, t)).toMap

        // Insert all 'after' dependencies
        targets.foreach(t => {
            val deps =  normalizeDependencies(t, t.after).flatMap(targetsById.get)
            val node = dependencies(t)
            deps.foreach(d => node += d)
        })

        // Insert all 'before' dependencies
        targets.foreach(t => {
            val deps = normalizeDependencies(t, t.before).flatMap(targetsById.get)
            deps.foreach(b => dependencies(b) += t)
        })
    }

    /**
     * Adds all implicit resource based dependencies
     * @param targets
     * @param phase
     * @param dependencies
     */
    private def addResourceDependencies(targets: Seq[Target], phase:Phase, dependencies: IdentityHashMap[Target,IdentityHashSet[Target]]) : Unit = {
        val producedResources = targets.flatMap(t =>
            try {
                t.provides(phase).map(id => (id,t))
            }
            catch {
                case NonFatal(ex) => throw new ExecutionException(s"Caught exception while resolving provided resources of target '${t.identifier}'", ex)
            }
        )

        // Insert all 'requires' dependencies
        targets.foreach(t => {
            val node = dependencies(t)
            try {
                // For each target requirement...
                t.requires(phase)
                    .foreach(req =>
                        // ... identify all targets which produce the resource
                        producedResources
                            .filter(res => req.intersects(res._1))
                            .foreach(res => node += res._2)
                    )
            }
            catch {
                case NonFatal(ex) => throw new ExecutionException(s"Caught exception while resolving required resources of target '${t.identifier}'", ex)
            }
        })
    }

    private def normalizeDependencies(target:Target, deps:Seq[TargetIdentifier]) : Seq[TargetIdentifier] = {
        deps.map(dep =>
            if (dep.project.nonEmpty)
                dep
            else
                TargetIdentifier(dep.name, target.project.map(_.name))
        )
    }
}
