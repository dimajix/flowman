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
import com.dimajix.flowman.graph.InputMapping
import com.dimajix.flowman.graph.MappingRef
import com.dimajix.flowman.graph.RelationRef
import com.dimajix.flowman.graph.WriteRelation
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.Relation


class RelationCollector(
    executeTests:Boolean = true
) extends Collector {
    private val logger = LoggerFactory.getLogger(getClass)

    override def collect(execution: Execution, graph: Graph, documentation: ProjectDoc): ProjectDoc = {
        val parent = documentation.reference
        val docs = graph.relations.map(t => t.relation.identifier -> document(execution, parent, t)).toMap
        documentation.copy(relations = docs)
    }

    /**
     * Create a documentation for the relation.
     * @param execution
     * @param parent
     * @return
     */
    private def document(execution:Execution, parent:Reference, node:RelationRef) : RelationDoc = {
        val relation = node.relation
        logger.info(s"Collecting documentation for relation '${relation.identifier}'")

        val inputs = node.incoming.flatMap {
                case write:WriteRelation =>
                    write.input.incoming.flatMap {
                        case map: InputMapping =>
                            val mapref = MappingReference(Some(parent), map.input.name)
                            val outref = MappingOutputReference(Some(mapref), map.pin)
                            Some(outref)
                        case _ => None
                    }
                case _ => Seq()
            }

        val doc = RelationDoc(
            Some(parent),
            relation.identifier,
            relation.description,
            None,
            inputs,
            relation.provides.toSeq,
            Map()
        )
        val ref = doc.reference

        val desc = SchemaDoc.ofStruct(ref, relation.describe(execution))
        val schema = relation.schema.map { schema =>
            val fieldsDoc = SchemaDoc.ofFields(parent, schema.fields)
            SchemaDoc(
                Some(ref),
                schema.description,
                fieldsDoc.columns,
                Seq()
            )
        }
        val mergedSchema = desc.merge(schema)

        val result = doc.copy(schema = Some(mergedSchema)).merge(relation.documentation)
        if (executeTests)
            runTests(execution, relation, result)
        else
            result
    }

    private def runTests(execution: Execution, relation:Relation, doc:RelationDoc) : RelationDoc = {
        val executor = new TestExecutor(execution)
        executor.executeTests(relation, doc)
    }
}
