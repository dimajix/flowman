/*
 * Copyright 2018-2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.util

import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.io.StringWriter
import java.io.Writer
import java.net.URL

import scala.reflect.ClassTag

import com.fasterxml.jackson.core.JsonGenerationException
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.{ObjectMapper => JacksonMapper}
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import com.dimajix.flowman.hadoop.File


/**
  * This singleton provides a preconfigured Jackson ObjectMapper which already contains all
  * extensions and can directly be used for reading flowman specification files
  */
class ObjectMapper {
    /**
      * Create a new Jackson ObjectMapper
      * @return
      */
    def mapper : JacksonMapper = {
        val mapper = new JacksonMapper(new YAMLFactory())
        mapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true)
        mapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true)
        mapper.registerModule(DefaultScalaModule)
        mapper
    }

    @throws[IOException]
    @throws[JsonProcessingException]
    @throws[JsonMappingException]
    def read[T:ClassTag](file:File) : T = {
        val input = file.open()
        try {
            read[T](input)
        }
        finally {
            input.close()
        }
    }
    @throws[IOException]
    @throws[JsonProcessingException]
    @throws[JsonMappingException]
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
    @throws[IOException]
    @throws[JsonProcessingException]
    @throws[JsonMappingException]
    def read[T:ClassTag](file:java.io.File) : T = {
        val ctag = implicitly[reflect.ClassTag[T]]
        mapper.readValue(file, ctag.runtimeClass.asInstanceOf[Class[T]])
    }
    @throws[IOException]
    @throws[JsonProcessingException]
    @throws[JsonMappingException]
    def read[T:ClassTag](stream:InputStream) : T = {
        val ctag = implicitly[reflect.ClassTag[T]]
        mapper.readValue(stream, ctag.runtimeClass.asInstanceOf[Class[T]])
    }
    @throws[JsonProcessingException]
    @throws[JsonMappingException]
    def parse[T:ClassTag](spec:String) : T = {
        val ctag = implicitly[reflect.ClassTag[T]]
        mapper.readValue(spec, ctag.runtimeClass.asInstanceOf[Class[T]])
    }

    @throws[IOException]
    @throws[JsonGenerationException]
    @throws[JsonMappingException]
    def write[T:ClassTag](value:T) : String = {
        val out = new StringWriter()
        mapper.writeValue(out, value)
        out.toString
    }
    @throws[IOException]
    @throws[JsonGenerationException]
    @throws[JsonMappingException]
    def write[T:ClassTag](writer:Writer, value:T) : Unit = {
        mapper.writeValue(writer, value)
    }
    @throws[IOException]
    @throws[JsonGenerationException]
    @throws[JsonMappingException]
    def write[T:ClassTag](stream:OutputStream, value:T) : Unit = {
        mapper.writeValue(stream, value)
    }
    @throws[IOException]
    @throws[JsonGenerationException]
    @throws[JsonMappingException]
    def write[T:ClassTag](file:java.io.File, value:T) : Unit = {
        mapper.writeValue(file, value)
    }
}


object ObjectMapper extends ObjectMapper {
}
