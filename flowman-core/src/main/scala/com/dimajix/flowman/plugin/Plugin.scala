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

package com.dimajix.flowman.plugin

import java.io.File
import java.net.URL

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.{ObjectMapper => JacksonMapper}
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.slf4j.LoggerFactory


object Plugin {
    class Reader {
        private val logger = LoggerFactory.getLogger(classOf[Plugin])

        private lazy val mapper = {
            val mapper = new JacksonMapper(new YAMLFactory())
            mapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true)
            mapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true)
            mapper.registerModule(DefaultScalaModule)
            mapper
        }

        private def loadFile(file:File) : Plugin = {
            logger.info(s"Reading plugin descriptor ${file.toString}")
            val plugin = mapper.readValue(file, classOf[Plugin])
            plugin._filename = file.getAbsoluteFile
            plugin._basedir = file.getAbsoluteFile.getParentFile
            plugin
        }

        /**
          * Loads a single file or a whole directory (non recursibely)
          *
          * @param file
          * @return
          */
        def file(file:File) : Plugin = {
            if (file.isDirectory) {
                logger.info(s"Reading plugin in directory ${file.toString}")
                loadFile(new File(file, "plugin.yml"))
            }
            else {
                loadFile(file)
            }
        }
        /**
          * Loads a single file or a whole directory (non recursibely)
          *
          * @param filename
          * @return
          */
        def file(filename:String) : Plugin = {
            file(new File(filename))
        }

        def url(url:URL) : Plugin = {
            logger.info(s"Reading plugin descriptor from url ${url.toString}")
            val con = url.openConnection()
            con.setUseCaches(false)
            mapper.readValue(con.getInputStream, classOf[Plugin])
        }

        def string(text:String) : Plugin = {
            mapper.readValue(text, classOf[Plugin])
        }
    }

    def read = new Reader
}


class Plugin {
    @JsonProperty(value="name") private var _name: String = _
    @JsonProperty(value="description") private var _description: String = ""
    @JsonProperty(value="version") private var _version: String = ""
    @JsonProperty(value="isolation") private var _isolation: String = "false"
    @JsonProperty(value="jars") private var _jars: Seq[String] = Seq()

    private var _basedir: File = new File("")
    private var _filename: File = new File("")

    def name : String = _name
    def description : String = _description
    def version : String = _version
    def filename : File = _filename
    def basedir : File = _basedir

    def jars : Seq[File] = _jars.map(name => new File(_basedir, name))

    def load() : Unit = ???
}
