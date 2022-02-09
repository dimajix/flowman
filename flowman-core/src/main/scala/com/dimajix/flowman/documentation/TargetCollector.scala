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

package com.dimajix.flowman.documentation

import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.graph.Graph
import com.dimajix.flowman.graph.TargetRef
import com.dimajix.flowman.model.Target


class TargetCollector extends Collector {
    override def collect(execution: Execution, graph: Graph, documentation: ProjectDoc): ProjectDoc = {
        val parent = documentation.reference
        val docs = graph.targets.map(t => t.target.identifier -> document(execution, parent, t)).toMap
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
        val doc = TargetDoc(
            Some(parent),
            target.identifier,
            target.description,
            Seq(),
            Seq(),
            Seq()
        )
        val ref = doc.reference

        val phaseDocs = target.phases.toSeq.map { p =>
            TargetPhaseDoc(
                Some(ref),
                p,
                None,
                target.provides(p).toSeq,
                target.requires(p).toSeq
            )
        }

        doc.copy(phases=phaseDocs).merge(target.documentation)
    }
}
