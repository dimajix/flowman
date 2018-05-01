/*
 * Copyright 2018 Kaya Kupferschmidt
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

import java.io.File

import scala.reflect.ClassTag

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.{ObjectMapper => JacksonMapper}
import com.fasterxml.jackson.databind.jsontype.NamedType
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import com.dimajix.flowman.spec.flow.Mapping
import com.dimajix.flowman.spec.model.Relation
import com.dimajix.flowman.spec.output.Output
import com.dimajix.flowman.spec.schema.Schema


object ObjectMapper {
    def mapper : JacksonMapper = {
        val relationTypes = Relation.subtypes.map(kv => new NamedType(kv._2, kv._1))
        val mappingTypes = Mapping.subtypes.map(kv => new NamedType(kv._2, kv._1))
        val outputTypes = Output.subtypes.map(kv => new NamedType(kv._2, kv._1))
        val schemaTypes = Schema.subtypes.map(kv => new NamedType(kv._2, kv._1))
        val mapper = new JacksonMapper(new YAMLFactory())
        mapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true)
        mapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true)
        mapper.registerModule(DefaultScalaModule)
        mapper.registerSubtypes(relationTypes:_*)
        mapper.registerSubtypes(mappingTypes:_*)
        mapper.registerSubtypes(outputTypes:_*)
        mapper.registerSubtypes(schemaTypes:_*)
        mapper
    }

    def read[T:ClassTag](file:File) : T = {
        val ctag = implicitly[reflect.ClassTag[T]]
        mapper.readValue(file, ctag.runtimeClass.asInstanceOf[Class[T]])
    }
    def parse[T:ClassTag](spec:String) : T = {
        val ctag = implicitly[reflect.ClassTag[T]]
        mapper.readValue(spec, ctag.runtimeClass.asInstanceOf[Class[T]])
    }
}
