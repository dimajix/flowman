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

import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.graph.Graph


class TestCollector extends Collector {
    private val logger = LoggerFactory.getLogger(getClass)

    /**
     * This will execute all tests and change the documentation accordingly
     * @param execution
     * @param graph
     * @param documentation
     * @return
     */
    override def collect(execution: Execution, graph: Graph, documentation: ProjectDoc): ProjectDoc = {
        val resolver = new ReferenceResolver(graph)
        val executor = new TestExecutor(execution)
        val mappings = documentation.mappings.map { m =>
            resolver.resolve(m.reference) match {
                case None =>
                    // This should not happen - but who knows...
                    logger.warn(s"Cannot find mapping for document reference '${m.reference.toString}'")
                    m
                case Some(mapping) =>
                    executor.executeTests(mapping, m)
            }
        }
        val relations = documentation.relations.map { r =>
            resolver.resolve(r.reference) match {
                case None =>
                    // This should not happen - but who knows...
                    logger.warn(s"Cannot find relation for document reference '${r.reference.toString}'")
                    r
                case Some(relation) =>
                    executor.executeTests(relation, r)
            }
        }

        documentation.copy(
            mappings = mappings,
            relations = relations
        )
    }
}
