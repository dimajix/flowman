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

package com.dimajix.flowman.execution

import scala.util.control.NonFatal
import scala.util.matching.Regex

import org.slf4j.LoggerFactory

import com.dimajix.common.IdentityHashMap
import com.dimajix.common.IdentityHashSet
import com.dimajix.flowman.model.Target


final class DirtyTargets(targets: Seq[Target], phase:Phase) {
    private val logger = LoggerFactory.getLogger(classOf[DirtyTargets])

    private val dependencies = {
        // Initialize dependencies map with targets as keys
        val dependencies = IdentityHashMap[Target,IdentityHashSet[Target]]()
        targets.foreach(t => dependencies.put(t, IdentityHashSet[Target]()))

        // Insert all implicit 'requires' dependencies
        addResourceDependencies(targets, phase, dependencies)

        // Finally reverse ordering for TRUNCATE and DESTROY
        phase match {
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
    }
    private val dirtyTargets = IdentityHashSet[Target]()

    /**
     * Checks if a given target was tainted, i.e. it is dirty
     * @param target
     * @return
     */
    def isDirty(target: Target) : Boolean = {
        dirtyTargets.synchronized {
            dirtyTargets.contains(target)
        }
    }

    /**
     * Taint a given target as being dirty
     * @param target
     */
    def taint(target: Target) : Unit = {
        dirtyTargets.synchronized {
            dirtyTargets += target
            dependencies.get(target).map { deps =>
                val newDirty = deps -- dirtyTargets
                if (newDirty.nonEmpty) {
                    logger.info(s"Cascade target '${target.identifier}' to taint targets ${newDirty.map(_.identifier.toString).mkString(",")}")
                    dirtyTargets ++= newDirty
                }
            }
        }
    }

    /**
     * Taint possibly multiple targets as being dirty
     * @param targets
     */
    def taint(targets: Seq[Regex]) : Unit = {
        val dirtyTargets = dependencies.keys.filter(target => targets.exists(_.unapplySeq(target.name).nonEmpty))
        if (dirtyTargets.nonEmpty)
            logger.info(s"Explicitly taint targets as dirty: ${dirtyTargets.map(_.identifier.toString).mkString(",")}")
        dirtyTargets.foreach(taint)
    }

    /**
     * Adds all implicit resource based dependencies
     * @param targets
     * @param phase
     * @param dependencies
     */
    private def addResourceDependencies(targets: Seq[Target], phase:Phase, dependencies: IdentityHashMap[Target,IdentityHashSet[Target]]) : Unit = {
        val requiredResources = targets.flatMap(t =>
            try {
                t.requires(phase).map(id => (id,t))
            }
            catch {
                case NonFatal(ex) => throw new ExecutionException(s"Caught exception while resolving provided resources of target '${t.identifier}'", ex)
            }
        )

        // Insert all 'requires' dependencies
        targets.foreach(t => {
            val node = dependencies(t)
            try {
                // For each target resource...
                t.provides(phase)
                    .foreach(req =>
                        // ... identify all targets which require the resource
                        requiredResources
                            .filter(res => req.intersects(res._1))
                            .foreach(res => node += res._2)
                    )
            }
            catch {
                case NonFatal(ex) => throw new ExecutionException(s"Caught exception while resolving required resources of target '${t.identifier}'", ex)
            }
        })
    }
}
