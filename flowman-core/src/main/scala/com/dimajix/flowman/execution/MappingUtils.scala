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

import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spec.MappingOutputIdentifier
import com.dimajix.flowman.spec.ResourceIdentifier
import com.dimajix.flowman.spec.flow.Mapping
import com.dimajix.flowman.types.StructType


object MappingUtils {
    /**
      * Returns a list of physical resources required for reading this dataset
      * @return
      */
    def requires(mapping: Mapping) : Set[ResourceIdentifier] = {
        val resourceCache = mutable.Map[MappingIdentifier,Set[ResourceIdentifier]]()

        def colllect(instance:Mapping) : Unit = {
            resourceCache.getOrElseUpdate(instance.identifier, instance.requires)
            instance.inputs
                .map(in => instance.context.getMapping(in.mapping))
                .foreach(colllect)
        }

        colllect(mapping)
        resourceCache.values.flatten.toSet
    }

    /**
      * Returns a list of physical resources required for reading this dataset
      * @return
      */
    def requires(context:Context, mapping: MappingIdentifier) : Set[ResourceIdentifier] = {
        val instance = context.getMapping(mapping)
        requires(instance)
    }
}
