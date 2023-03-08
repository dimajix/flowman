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

package com.dimajix.flowman.documentation

import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.graph.Graph
import com.dimajix.flowman.graph.InputMapping
import com.dimajix.flowman.graph.ReadRelation
import com.dimajix.flowman.graph.TargetRef
import com.dimajix.flowman.graph.WriteRelation
import com.dimajix.flowman.model.Target


class TargetCollector extends AbstractCollector {
    private val logger = LoggerFactory.getLogger(getClass)

    override def collect(execution: Execution, graph: Graph, documentation: ProjectDoc): ProjectDoc = {
        val parent = documentation.reference
        val docs = graph.targets.map(t => document(execution, parent, t))
        documentation.copy(targets = docs)
    }

    /**
     * Creates a documentation of this target
     * @param execution
     * @param parent
     * @return
     */
    private def document(execution: Execution, parent:Reference, node:TargetRef) : TargetDoc = {
        val target = node.target
        logger.info(s"Collecting documentation for target '${target.identifier}'")

        val inputs = node.incoming.flatMap {
            case map: InputMapping =>
                val mapref = MappingReference.of(parent, map.mapping.identifier)
                val outref = MappingOutputReference(Some(mapref), map.pin)
                Some(outref)
            case read: ReadRelation =>
                val relref = RelationReference.of(parent, read.input.identifier)
                Some(relref)
            case _ => None
        }
        val outputs = node.outgoing.flatMap {
            case write:WriteRelation =>
                val relref = RelationReference.of(parent, write.output.identifier)
                Some(relref)
            case _ => None
        }

        val doc = TargetDoc(
            Some(parent),
            Some(target),
            description = target.description,
            inputs = inputs,
            outputs = outputs
        )
        val ref = doc.reference

        val phaseDocs = target.phases.toSeq.map { p =>
            TargetPhaseDoc(
                Some(ref),
                p,
                provides = target.provides(p).toSeq,
                requires = target.requires(p).toSeq
            )
        }

        doc.copy(phases=phaseDocs).merge(target.documentation)
    }
}
