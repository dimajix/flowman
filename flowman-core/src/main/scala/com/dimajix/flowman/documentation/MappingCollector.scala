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

import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.graph.Graph
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.types.StructType


class MappingCollector(
    executeTests:Boolean = true
) extends Collector {
    private val logger = LoggerFactory.getLogger(getClass)

    override def collect(execution: Execution, graph: Graph, documentation: ProjectDoc): ProjectDoc = {
        val mappings = mutable.Map[MappingIdentifier, MappingDoc]()
        val parent = documentation.reference

        def getMappingDoc(mapping:Mapping) : MappingDoc = {
            mappings.getOrElseUpdate(mapping.identifier, genDoc(mapping))
        }
        def getOutputDoc(mappingOutput:MappingOutputIdentifier) : Option[MappingOutputDoc] = {
            val mapping = mappingOutput.mapping
            val doc = mappings.getOrElseUpdate(mapping, genDoc(graph.mapping(mapping).mapping))
            val output = mappingOutput.output
            doc.outputs.find(_.identifier.output == output)
        }
        def genDoc(mapping:Mapping) : MappingDoc = {
            logger.info(s"Collecting documentation for mapping '${mapping.identifier}'")
            val inputs = mapping.inputs.flatMap(in => getOutputDoc(in).map(in -> _)).toMap
            document(execution, parent, mapping, inputs)
        }

        val docs = graph.mappings.map { mapping =>
            mapping.mapping.identifier -> getMappingDoc(mapping.mapping)
        }.toMap

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
        val schemas = mapping.describe(execution, inputSchemas)
        val doc = MappingDoc(
            Some(parent),
            mapping.identifier,
            None,
            inputs.map(_._2.reference).toSeq,
            Seq()
        )
        val ref = doc.reference

        val outputs = schemas.map { case(output,schema) =>
            val doc = MappingOutputDoc(
                Some(ref),
                MappingOutputIdentifier(mapping.identifier, output),
                None,
                None
            )
            val schemaDoc = SchemaDoc.ofStruct(doc.reference, schema)
            doc.copy(schema = Some(schemaDoc))
        }

        val result = doc.copy(outputs=outputs.toSeq).merge(mapping.documentation)
        if (executeTests)
            runTests(execution, mapping, result)
        else
            result
    }

    private def runTests(execution: Execution, mapping:Mapping, doc:MappingDoc) : MappingDoc = {
        val executor = new TestExecutor(execution)
        executor.executeTests(mapping, doc)
    }
}
