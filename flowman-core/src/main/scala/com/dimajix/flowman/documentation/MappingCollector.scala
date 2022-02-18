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

import scala.collection.mutable
import scala.util.control.NonFatal

import org.slf4j.LoggerFactory

import com.dimajix.common.ExceptionUtils.reasons
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.graph.Graph
import com.dimajix.flowman.graph.MappingRef
import com.dimajix.flowman.graph.ReadRelation
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.types.StructType


class MappingCollector extends Collector {
    private val logger = LoggerFactory.getLogger(getClass)

    override def collect(execution: Execution, graph: Graph, documentation: ProjectDoc): ProjectDoc = {
        val mappings = mutable.Map[MappingIdentifier, MappingDoc]()
        val parent = documentation.reference

        def getMappingDoc(node:MappingRef) : MappingDoc = {
            val mapping = node.mapping
            mappings.getOrElseUpdate(mapping.identifier, genDoc(node))
        }
        def getOutputDoc(mappingOutput:MappingOutputIdentifier) : Option[MappingOutputDoc] = {
            val mapping = mappingOutput.mapping
            val doc = mappings.getOrElseUpdate(mapping, genDoc(graph.mapping(mapping)))
            val output = mappingOutput.output
            doc.outputs.find(_.identifier.output == output)
        }
        def genDoc(node:MappingRef) : MappingDoc = {
            val mapping = node.mapping
            logger.info(s"Collecting documentation for mapping '${mapping.identifier}'")

            // Collect fundamental basis information
            val inputs = mapping.inputs.flatMap(in => getOutputDoc(in).map(in -> _)).toMap
            val doc = document(execution, parent, mapping, inputs)

            // Add additional inputs from non-mapping entities
            val incoming = node.incoming.collect {
                // TODO: The following logic is not correct in case of embedded relations. We would need an IdentityHashMap instead
                case ReadRelation(input, _, _) => documentation.relations.find(_.identifier == input.relation.identifier).map(_.reference)
            }.flatten
            doc.copy(inputs=doc.inputs ++ incoming)
        }

        val docs = graph.mappings.map(mapping => getMappingDoc(mapping))

        documentation.copy(mappings=docs)
    }

    /**
     * Generates a documentation for this mapping
     * @param execution
     * @param parent
     * @param inputs
     * @return
     */
    private def document(execution: Execution, parent:Reference, mapping:Mapping, inputs:Map[MappingOutputIdentifier,MappingOutputDoc]) : MappingDoc = {
        val inputSchemas = inputs.map(kv => kv._1 -> kv._2.schema.map(_.toStruct).getOrElse(StructType(Seq())))
        val doc = MappingDoc(
            Some(parent),
            mapping.identifier,
            inputs = inputs.map(_._2.reference).toSeq
        )
        val ref = doc.reference

        val outputs = try {
            val schemas = mapping.describe(execution, inputSchemas)
            schemas.map { case(output,schema) =>
                val doc = MappingOutputDoc(
                    Some(ref),
                    MappingOutputIdentifier(mapping.identifier, output)
                )
                val schemaDoc = SchemaDoc.ofStruct(doc.reference, schema)
                doc.copy(schema = Some(schemaDoc))
            }
        } catch {
            case NonFatal(ex) =>
                logger.warn(s"Error while inferring schema description of mapping '${mapping.identifier}': ${reasons(ex)}")
                mapping.outputs.map { output =>
                    MappingOutputDoc(
                        Some(ref),
                        MappingOutputIdentifier(mapping.identifier, output)
                    )
                }
        }

        doc.copy(outputs=outputs.toSeq).merge(mapping.documentation)
    }
}
