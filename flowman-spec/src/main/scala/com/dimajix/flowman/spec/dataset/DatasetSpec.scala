/*
 * Copyright 2019-2020 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.dataset

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo

import com.dimajix.common.TypeRegistry
import com.dimajix.flowman.annotation.DatasetType
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.model.Dataset
import com.dimajix.flowman.spec.Spec
import com.dimajix.flowman.spi.ClassAnnotationHandler


object DatasetSpec extends TypeRegistry[DatasetSpec] {
}

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind", visible = true)
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "file", value = classOf[FileDatasetSpec]),
    new JsonSubTypes.Type(name = "mapping", value = classOf[MappingDatasetSpec]),
    new JsonSubTypes.Type(name = "relation", value = classOf[RelationDatasetSpec])
))
abstract class DatasetSpec extends Spec[Dataset] {
    @JsonProperty(value="kind", required = true) protected var kind: String = _

    override def instantiate(context:Context) : Dataset

    /**
      * Returns a set of common properties
      * @param context
      * @return
      */
    protected def instanceProperties(context:Context, name:String) : Dataset.Properties = {
        require(context != null)
        Dataset.Properties(
            context,
            context.namespace,
            context.project,
            kind + "(" + name + ")",
            kind,
            Map()
        )
    }
}



class DatasetSpecAnnotationHandler extends ClassAnnotationHandler {
    override def annotation: Class[_] = classOf[DatasetType]

    override def register(clazz: Class[_]): Unit =
        DatasetSpec.register(clazz.getAnnotation(classOf[DatasetType]).kind(), clazz.asInstanceOf[Class[_ <: DatasetSpec]])
}
