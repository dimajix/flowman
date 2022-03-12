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

import scala.annotation.tailrec

import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.NoSuchMappingException
import com.dimajix.flowman.execution.NoSuchRelationException
import com.dimajix.flowman.graph.Column
import com.dimajix.flowman.graph.Graph
import com.dimajix.flowman.graph.InputColumn
import com.dimajix.flowman.graph.InputMapping
import com.dimajix.flowman.graph.MappingOutput
import com.dimajix.flowman.graph.MappingRef
import com.dimajix.flowman.graph.Node
import com.dimajix.flowman.graph.RelationRef
import com.dimajix.flowman.graph.WriteRelation
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.RelationIdentifier


class LineageCollector extends Collector {
    private val logger = LoggerFactory.getLogger(getClass)

    override def collect(execution: Execution, graph: Graph, documentation: ProjectDoc): ProjectDoc = {
        def resolveMapping(mapping:MappingDoc) : MappingRef = {
            mapping.mapping match {
                case Some(mapping) => graph.mapping(mapping)
                case None => throw new NoSuchMappingException(MappingIdentifier.empty)
            }
        }
        def resolveRelation(relation:RelationDoc) : RelationRef = {
            relation.relation match {
                case Some(relation) => graph.relation(relation)
                case None => throw new NoSuchRelationException(RelationIdentifier.empty)
            }
        }
        def resolveColumn(column:Column) : ColumnReference = {
            column.parent match {
                case None => ColumnReference(None, column.name)
                case Some(col:Column) => ColumnReference(Some(resolveColumn(col)), column.name)
                case Some(map:MappingOutput) =>
                    documentation.mappings.find(_.mapping.exists(_ eq map.mapping.mapping)) match {
                        case Some(mapping) => ColumnReference(mapping.outputs.find(_.name == map.output).map(_.reference), column.name)
                        case None => ColumnReference(None, column.name)
                    }
                case Some(rel:RelationRef) =>
                    documentation.relations.find(_.relation.exists(_ eq rel.relation)) match {
                        case Some(relation) => ColumnReference(relation.schema.map(_.reference), column.name)
                        case None => ColumnReference(None, column.name)
                    }
                case _ => ColumnReference(None, column.name)
            }
        }
        def collectInputs(column:Column) : Seq[ColumnReference] = {
            @tailrec
            def isRelation(parent:Option[Node]) : Boolean = {
                parent match {
                    case Some(_:RelationRef) => true
                    case Some(col:Column) => isRelation(col.parent)
                    case _ => false
                }
            }
            column.incoming.flatMap {
                // Stop at columns which are relations
                case in: InputColumn if isRelation(in.input.parent) => Seq(resolveColumn(in.input))
                // Recursively forward input
                case in: InputColumn if in.input.incoming.nonEmpty => collectInputs(in.input)
                // resolve column if no more input is present
                case in: InputColumn => Seq(resolveColumn(in.input))
                case _ => Seq.empty
            }
        }
        def genColumnDoc(column:ColumnDoc, fields:Seq[Column], entity:String) : ColumnDoc = {
            fields.find(_.name == column.name) match {
                case Some(field) =>
                    val children = column.children.map(c => genColumnDoc(c, field.fields, entity))
                    val inputs = collectInputs(field).distinct
                    column.copy(children=children, inputs=inputs)
                case None =>
                    logger.warn(s"Cannot not find column '${column.name}' in inferred schema of $entity")
                    column
            }
        }
        def genOutputDoc(node:MappingOutput, output:MappingOutputDoc) : MappingOutputDoc = {
            val schema = output.schema.map { s =>
                val cols = s.columns.map(c => genColumnDoc(c, node.fields, s"mapping output '${node.identifier}'"))
                s.copy(columns = cols)
            }
            output.copy(schema=schema)
        }
        def genMappingDoc(doc:MappingDoc) : MappingDoc = {
            val node = resolveMapping(doc)
            val mapping = node.mapping
            logger.info(s"Collecting lineage documentation for mapping '${mapping.identifier}'")

            // Collect references columns
            val outputs = doc.outputs.map { out =>
                val mapOut = node.outputs.find(_.output == out.name)
                mapOut match {
                    case Some(mapOut) =>
                        genOutputDoc(mapOut, out)
                    case None =>
                        logger.warn(s"Cannot find output '${out.name}' in mapping '${mapping.identifier}'")
                        out
                }
            }

            doc.copy(outputs=outputs)
        }

        def genRelationDoc(doc:RelationDoc) : RelationDoc = {
            val node = resolveRelation(doc)
            val relation = node.relation
            logger.info(s"Collecting lineage documentation for relation '${relation.identifier}'")

            val fields = getInputColumns(node)
            val schema = doc.schema.map { s =>
                val cols = s.columns.map(c => genColumnDoc(c, fields, s"relation ${relation.identifier}"))
                s.copy(columns = cols)
            }
            doc.copy(schema=schema)
        }

        val mappingDocs = documentation.mappings.map(genMappingDoc)
        val relationDocs = documentation.relations.map(genRelationDoc)

        documentation.copy(mappings=mappingDocs, relations=relationDocs)
    }

    private def getInputColumns(node:RelationRef) : Seq[Column] = {
        if (node.fields.nonEmpty) {
            node.fields
        }
        else {
            // Relation doesn't have any fields, they were probably copied from mapping. So we do the same
            // for creating the lineage
            node.incoming.flatMap {
                case write: WriteRelation =>
                    write.input.incoming.flatMap {
                        case map: InputMapping =>
                            map.input.fields
                        case _ => None
                    }
                case _ => Seq()
            }
        }
    }
}
