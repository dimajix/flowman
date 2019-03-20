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

import java.io.InputStream
import java.net.URL

import scala.reflect.ClassTag

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.jsontype.NamedType
import com.fasterxml.jackson.databind.{ObjectMapper => JacksonMapper}
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import com.dimajix.flowman.hadoop.File
import com.dimajix.flowman.spec.catalog.CatalogProvider
import com.dimajix.flowman.spec.state.StateStoreProvider
import com.dimajix.flowman.spec.connection.Connection
import com.dimajix.flowman.spec.flow.Mapping
import com.dimajix.flowman.spec.model.Relation
import com.dimajix.flowman.spec.target.Target
import com.dimajix.flowman.spec.schema.Schema
import com.dimajix.flowman.spec.task.Task
import com.dimajix.flowman.spi.Registration


/**
  * This singleton provides a preconfigured Jackson ObjectMapper which already contains all
  * extensions and can directly be used for reading flowman specification files
  */
object ObjectMapper {
    /**
      * Create a new Jackson ObjectMapper
      * @return
      */
    def mapper : JacksonMapper = {
        Registration.load()
        val stateStoreTypes = StateStoreProvider.subtypes.map(kv => new NamedType(kv._2, kv._1))
        val catalogTypes = CatalogProvider.subtypes.map(kv => new NamedType(kv._2, kv._1))
        val monitorTypes = StateStoreProvider.subtypes.map(kv => new NamedType(kv._2, kv._1))
        val relationTypes = Relation.subtypes.map(kv => new NamedType(kv._2, kv._1))
        val mappingTypes = Mapping.subtypes.map(kv => new NamedType(kv._2, kv._1))
        val outputTypes = Target.subtypes.map(kv => new NamedType(kv._2, kv._1))
        val schemaTypes = Schema.subtypes.map(kv => new NamedType(kv._2, kv._1))
        val taskTypes = Task.subtypes.map(kv => new NamedType(kv._2, kv._1))
        val connectionTypes = Connection.subtypes.map(kv => new NamedType(kv._2, kv._1))
        val mapper = new JacksonMapper(new YAMLFactory())
        mapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true)
        mapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true)
        mapper.registerModule(DefaultScalaModule)
        mapper.registerSubtypes(stateStoreTypes:_*)
        mapper.registerSubtypes(catalogTypes:_*)
        mapper.registerSubtypes(monitorTypes:_*)
        mapper.registerSubtypes(relationTypes:_*)
        mapper.registerSubtypes(mappingTypes:_*)
        mapper.registerSubtypes(outputTypes:_*)
        mapper.registerSubtypes(schemaTypes:_*)
        mapper.registerSubtypes(taskTypes:_*)
        mapper.registerSubtypes(connectionTypes:_*)
        mapper
    }

    def read[T:ClassTag](file:File) : T = {
        val input = file.open()
        try {
            read[T](input)
        }
        finally {
            input.close()
        }
    }
    def read[T:ClassTag](url:URL) : T = {
        val con = url.openConnection()
        val input = con.getInputStream
        try {
            con.setUseCaches(false)
            read[T](input)
        }
        finally {
            input.close()
        }
    }
    def read[T:ClassTag](file:java.io.File) : T = {
        val ctag = implicitly[reflect.ClassTag[T]]
        mapper.readValue(file, ctag.runtimeClass.asInstanceOf[Class[T]])
    }
    def read[T:ClassTag](stream:InputStream) : T = {
        val ctag = implicitly[reflect.ClassTag[T]]
        mapper.readValue(stream, ctag.runtimeClass.asInstanceOf[Class[T]])
    }
    def parse[T:ClassTag](spec:String) : T = {
        val ctag = implicitly[reflect.ClassTag[T]]
        mapper.readValue(spec, ctag.runtimeClass.asInstanceOf[Class[T]])
    }
}
