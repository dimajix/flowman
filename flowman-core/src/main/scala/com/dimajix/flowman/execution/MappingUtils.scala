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

package com.dimajix.flowman.execution

import scala.collection.mutable

import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

import com.dimajix.flowman.spec.MappingOutputIdentifier
import com.dimajix.flowman.spec.flow.Mapping
import com.dimajix.flowman.types.StructType


object MappingUtils {
    private val logger = LoggerFactory.getLogger(MappingUtils.getClass)

    def describe(mapping:Mapping, output:String) : Option[StructType] = {
        val schemaCache = mutable.Map[MappingOutputIdentifier, Option[StructType]]()

        def describe(mapping:Mapping, output:String) : Option[StructType] = {
            val oid = MappingOutputIdentifier(mapping.identifier, output)
            schemaCache.getOrElseUpdate(oid, {
                if (!mapping.outputs.contains(output))
                    throw new NoSuchMappingOutputException(oid)
                val context = mapping.context
                val deps = mapping.inputs
                    .flatMap(id => describe(context.getMapping(id.mapping), id.output).map(s => (id,s)))
                    .toMap

                // Only return a schema if all dependencies are present
                if (mapping.inputs.forall(d => deps.contains(d))) {
                    mapping.describe(deps, output)
                }
                else {
                    None
                }
            })
        }

        describe(mapping, output)
    }

    def describe(context:Context, output:MappingOutputIdentifier) : Option[StructType] = {
        val mapping = context.getMapping(output.mapping)
        describe(mapping, output.output)
    }
}
