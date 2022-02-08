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
import com.dimajix.flowman.model.Relation


class RelationCollector extends Collector {
    override def collect(execution: Execution, graph: Graph, documentation: ProjectDoc): ProjectDoc = {
        val parent = documentation.reference
        val docs = graph.relations.map(t => t.relation.identifier -> document(execution, parent, t.relation)).toMap
        documentation.copy(relations = docs)
    }


    /**
     * Create a documentation for the relation.
     * @param execution
     * @param parent
     * @return
     */
    private def document(execution:Execution, parent:Reference, relation:Relation) : RelationDoc = {
        val doc = RelationDoc(
            Some(parent),
            relation.identifier,
            relation.description,
            None,
            relation.provides.toSeq,
            Map()
        )
        val ref = doc.reference

        val desc = SchemaDoc.ofStruct(ref, relation.describe(execution))
        val schemaDoc = relation.schema.map { schema =>
            val fieldsDoc = SchemaDoc.ofFields(parent, schema.fields)
            SchemaDoc(
                Some(ref),
                schema.description,
                fieldsDoc.columns,
                Seq()
            )
        }
        val mergedDoc = desc.merge(schemaDoc)

        doc.copy(schema = Some(mergedDoc)).merge(relation.documentation)
    }
}
