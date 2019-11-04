/*
 * Copyright 2019 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec

import scala.collection.mutable

import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Phase


package object target {
    /**
      * Create ordering of specified targets, such that all dependencies are fullfilled
      * @param targets
      * @return
      */
    def orderTargets(targets: Seq[Target], phase:Phase) : Seq[Target] = {
        val logger = LoggerFactory.getLogger(classOf[Target])

        targets.foreach { t =>
            logger.debug(s"Analyzing build phase '$phase' of target '${t.identifier}'")
            t.requires(phase).foreach(r => logger.debug(s"  requires $r"))
            t.provides(phase).foreach(r => logger.debug(s"  provides $r"))
        }

        def normalize(target:Target, deps:Seq[TargetIdentifier]) : Seq[TargetIdentifier] = {
            deps.map(dep =>
                if (dep.project.nonEmpty)
                    dep
                else
                    TargetIdentifier(dep.name, Option(target.project).map(_.name))
            )
        }

        val targetIds = targets.map(_.identifier).toSet
        val targetsById = targets.map(t => (t.identifier, t)).toMap
        val producedResources = targets.flatMap(t =>
            try {
                t.provides(phase).map(id => (id,t.identifier))
            }
            catch {
                case ex:Exception => throw new RuntimeException(s"Caught exception while resolving provided resources of target '${t.identifier}'", ex)
            }
        )

        val nodes = mutable.Map(targets.map(t => t.identifier -> mutable.Set[TargetIdentifier]()):_*)

        // Process all 'after' dependencies
        targets.foreach(t => {
            val deps =  normalize(t, t.after).filter(targetIds.contains)
            val node = nodes(t.identifier)
            deps.foreach(d => node += d)
        })

        // Process all 'before' dependencies
        targets.foreach(t => {
            val deps = normalize(t, t.before).filter(targetIds.contains)
            deps.foreach(b => nodes(b) += t.identifier)
        })

        // Process all 'requires' dependencies
        targets.foreach(t => {
            val node = nodes(t.identifier)
            try {
                // For each target requirement...
                t.requires(phase)
                    .foreach(req =>
                        // ... identify all targets which produce the resource
                        producedResources
                            .filter(res => req.contains(res._1))
                            .foreach(res => node += res._2)
                    )
            }
            catch {
                case ex:Exception => throw new RuntimeException(s"Caught exception while resolving required resources of target '${t.identifier}'", ex)
            }
        })

        nodes.foreach { case(n,deps) =>
            logger.info(s"Dependencies of phase '$phase' of target '$n': ${deps.map(_.toString).mkString(",")}")
        }

        val order = mutable.ListBuffer[TargetIdentifier]()
        while (nodes.nonEmpty) {
            val candidate = nodes.find(_._2.isEmpty).map(_._1)
                .getOrElse({
                    val deps = nodes.map { case(k,v) => s"  $k <= ${v.toSeq.mkString(", ")}"}.mkString("\n")
                    logger.error(s"Cannot create target order due to cyclic dependencies:\n$deps")
                    throw new RuntimeException("Cannot create target order")
                })

            // Remove candidate
            nodes.remove(candidate)
            // Remove this target from all dependencies
            nodes.foreach { case (k,v) => v.remove(candidate) }
            // Append candidate to build sequence
            order.append(candidate)
        }

        order.map(targetsById.apply)
    }
}
