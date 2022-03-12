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

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import org.slf4j.LoggerFactory

import com.dimajix.common.ExceptionUtils.reasons
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.graph.Graph
import com.dimajix.flowman.graph.InputMapping
import com.dimajix.flowman.graph.MappingRef
import com.dimajix.flowman.graph.ReadRelation
import com.dimajix.flowman.graph.RelationRef
import com.dimajix.flowman.graph.WriteRelation
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.types.FieldValue


class RelationCollector extends Collector {
    private val logger = LoggerFactory.getLogger(getClass)

    override def collect(execution: Execution, graph: Graph, documentation: ProjectDoc): ProjectDoc = {
        val parent = documentation.reference
        val docs = graph.relations.map(t => document(execution, parent, t))
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
                            val mapref = MappingReference.of(parent, map.mapping.identifier)
                            val outref = MappingOutputReference(Some(mapref), map.pin)
                            Some(outref)
                        case _ => None
                    }
                case _ => Seq()
            }
        val inputPartitions = node.outgoing.flatMap {
                case read:ReadRelation =>
                    logger.debug(s"read partition ${relation.identifier}: ${read.input.identifier} ${read.partitions}")
                    Some(read.partitions)
                case _ => None
            }
        val outputPartitions = node.incoming.flatMap {
                case write:WriteRelation =>
                    logger.debug(s"write partition ${relation.identifier}: ${write.output.identifier} ${write.partition}")
                    Some(write.partition)
                case _ => None
            }

        // Recursively collect all sources from upstream mappings
        def collectMappingSources(map:MappingRef) : Seq[ResourceIdentifier] = {
            val direct = map.mapping.requires.toSeq
            val indirect = map.incoming.flatMap {
                case map:InputMapping =>
                    collectMappingSources(map.mapping)
                case _ => Seq.empty
            }
            (direct ++ indirect).distinct
        }

        val sources =  node.incoming.flatMap {
            case write:WriteRelation =>
                write.input.incoming.flatMap {
                    case map: InputMapping =>
                        collectMappingSources(map.mapping)
                    case rel: ReadRelation =>
                        rel.input.relation.provides.toSeq
                    case _ => Seq.empty
                }
            case _ => Seq.empty
        }.distinct

        val partitions = (inputPartitions ++ outputPartitions).foldLeft(Map.empty[String,FieldValue])((a,b) => a ++ b)

        val doc = RelationDoc(
            Some(parent),
            Some(relation),
            description = relation.description,
            inputs = inputs,
            provides = relation.provides.toSeq,
            requires = relation.requires.toSeq,
            sources = sources,
            partitions = partitions
        )
        val ref = doc.reference

        val schema = relation.schema.map { schema =>
                val fieldsDoc = SchemaDoc.ofFields(parent, schema.fields)
                SchemaDoc(
                    Some(ref),
                    description = schema.description,
                    columns = fieldsDoc.columns
                )
            }.orElse {
                // Try to infer schema from input
                getInputSchema(execution, ref, node)
            }
        val mergedSchema = {
            Try {
                SchemaDoc.ofStruct(ref, execution.describe(relation, partitions))
            } match {
                case Success(desc) =>
                    Some(desc.merge(schema))
                case Failure(ex) =>
                    logger.warn(s"Error while inferring schema description of relation '${relation.identifier}': ${reasons(ex)}")
                    schema
            }
        }

        doc.copy(schema = mergedSchema).merge(relation.documentation)
    }

    private def getInputSchema(execution:Execution, parent:Reference, node:RelationRef) : Option[SchemaDoc] = {
        // Try to infer schema from input
        val schema = node.incoming.flatMap {
            case write:WriteRelation =>
                write.input.incoming.flatMap {
                    case map: InputMapping =>
                        Try {
                            val mapout = map.input
                            execution.describe(mapout.mapping.mapping, mapout.output)
                        }.toOption
                    case _ => None
                }
            case _ => Seq()
        }.headOption

        schema.map { schema =>
            val fieldsDoc = SchemaDoc.ofFields(parent.parent.get, schema.fields)
            SchemaDoc(
                Some(parent),
                columns = fieldsDoc.columns
            )
        }
    }
}
