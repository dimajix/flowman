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
import org.slf4j.LoggerFactory

import com.dimajix.flowman.util.ObjectMapper


object Plugin {
    class Reader {
        private val logger = LoggerFactory.getLogger(classOf[Plugin])

        private def loadFile(file:File) : Plugin = {
            logger.info(s"Reading plugin descriptor ${file.toString}")
            ObjectMapper.read[PluginSpec](file)
                .instantiate(Some(file))
        }

        /**
          * Loads a single file or a whole directory (non recursibely)
          *
          * @param file
          * @return
          */
        def file(file:File) : Plugin = {
            if (file.isDirectory) {
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
            ObjectMapper.read[PluginSpec](url)
                .instantiate()
        }

        def string(text:String) : Plugin = {
            ObjectMapper.parse[PluginSpec](text)
                .instantiate()
        }
    }

    def read = new Reader
}


final case class Plugin(
    name : String,
    description : Option[String] = None,
    version : Option[String] = None,
    filename : Option[File] = None,
    basedir : Option[File] = None,
    jars : Seq[File] = Seq()
)


class PluginSpec {
    @JsonProperty(value="name", required = true) private var name: String = _
    @JsonProperty(value="description", required = false) private var description: Option[String] = None
    @JsonProperty(value="version", required = false) private var version: Option[String] = None
    @JsonProperty(value="isolation", required = false) private var isolation: String = "false"
    @JsonProperty(value="jars", required = false) private var jars: Seq[String] = Seq()

    def instantiate(filename:Option[File]=None) : Plugin = {
        val absFilename = filename.map(_.getAbsoluteFile)
        val parentDirectory = filename.map(_.getParentFile)
        Plugin(
            name,
            description,
            version,
            absFilename,
            parentDirectory,
            jars.map(name => parentDirectory.map(path => new File(path, name)).getOrElse(new File(name)))
        )
    }
}
