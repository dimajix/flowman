/*
 * Copyright 2018-2021 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec

import com.fasterxml.jackson.databind.jsontype.NamedType
import com.fasterxml.jackson.databind.{ObjectMapper => JacksonMapper}

import com.dimajix.flowman.spec.assertion.AssertionSpec
import com.dimajix.flowman.spec.catalog.CatalogSpec
import com.dimajix.flowman.spec.connection.ConnectionSpec
import com.dimajix.flowman.spec.dataset.DatasetSpec
import com.dimajix.flowman.spec.history.HistorySpec
import com.dimajix.flowman.spec.mapping.MappingSpec
import com.dimajix.flowman.spec.measure.MeasureSpec
import com.dimajix.flowman.spec.metric.MetricSinkSpec
import com.dimajix.flowman.spec.relation.RelationSpec
import com.dimajix.flowman.spec.schema.SchemaSpec
import com.dimajix.flowman.spec.target.TargetSpec
import com.dimajix.flowman.spi.ClassAnnotationScanner
import com.dimajix.flowman.util.{ObjectMapper => CoreObjectMapper}


/**
  * This singleton provides a preconfigured Jackson ObjectMapper which already contains all
  * extensions and can directly be used for reading flowman specification files
  */
object ObjectMapper extends CoreObjectMapper {
    /**
      * Create a new Jackson ObjectMapper
      * @return
      */
    override def mapper : JacksonMapper = {
        // Ensure that all extensions are loaded
        ClassAnnotationScanner.load()

        val stateStoreTypes = HistorySpec.subtypes.map(kv => new NamedType(kv._2, kv._1))
        val catalogTypes = CatalogSpec.subtypes.map(kv => new NamedType(kv._2, kv._1))
        val monitorTypes = HistorySpec.subtypes.map(kv => new NamedType(kv._2, kv._1))
        val relationTypes = RelationSpec.subtypes.map(kv => new NamedType(kv._2, kv._1))
        val mappingTypes = MappingSpec.subtypes.map(kv => new NamedType(kv._2, kv._1))
        val targetTypes = TargetSpec.subtypes.map(kv => new NamedType(kv._2, kv._1))
        val schemaTypes = SchemaSpec.subtypes.map(kv => new NamedType(kv._2, kv._1))
        val connectionTypes = ConnectionSpec.subtypes.map(kv => new NamedType(kv._2, kv._1))
        val assertionTypes = AssertionSpec.subtypes.map(kv => new NamedType(kv._2, kv._1))
        val measureTypes = MeasureSpec.subtypes.map(kv => new NamedType(kv._2, kv._1))
        val datasetTypes = DatasetSpec.subtypes.map(kv => new NamedType(kv._2, kv._1))
        val metricSinkTypes = MetricSinkSpec.subtypes.map(kv => new NamedType(kv._2, kv._1))
        val mapper = super.mapper
        mapper.registerSubtypes(stateStoreTypes:_*)
        mapper.registerSubtypes(catalogTypes:_*)
        mapper.registerSubtypes(monitorTypes:_*)
        mapper.registerSubtypes(relationTypes:_*)
        mapper.registerSubtypes(mappingTypes:_*)
        mapper.registerSubtypes(targetTypes:_*)
        mapper.registerSubtypes(schemaTypes:_*)
        mapper.registerSubtypes(connectionTypes:_*)
        mapper.registerSubtypes(assertionTypes:_*)
        mapper.registerSubtypes(measureTypes:_*)
        mapper.registerSubtypes(datasetTypes:_*)
        mapper.registerSubtypes(metricSinkTypes:_*)
        mapper
    }
}
