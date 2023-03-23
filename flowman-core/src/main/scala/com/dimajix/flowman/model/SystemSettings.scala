/*
 * Copyright (C) 2018 The Flowman Authors
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

package com.dimajix.flowman.model

import java.io.File
import java.io.InputStream
import java.net.URL

import com.fasterxml.jackson.annotation.JsonProperty
import org.slf4j.LoggerFactory

import com.dimajix.flowman.util.ObjectMapper


object SystemSettings {
    class Builder {
        private val system = new SystemSettings

        def build() : SystemSettings = system
    }

    class Reader {
        private val logger = LoggerFactory.getLogger(classOf[SystemSettings])

        def file(file: File): SystemSettings = {
            logger.info(s"Reading Flowman system settings file ${file.toString}")
            ObjectMapper.read[SystemSettings](file)
        }
        def file(filename:String) : SystemSettings = {
            file(new File(filename))
        }
        def stream(stream:InputStream) : SystemSettings = {
            ObjectMapper.read[SystemSettings](stream)
        }
        def url(url:URL) : SystemSettings = {
            logger.info(s"Reading Flowman system settings from url ${url.toString}")
            val con = url.openConnection()
            con.setUseCaches(false)
            stream(con.getInputStream)
        }
        def string(text:String) : SystemSettings = {
            ObjectMapper.parse[SystemSettings](text)
        }
        def default() : SystemSettings = {
            logger.info(s"Using Flowman default system settings")
            new SystemSettings
        }
    }

    def read = new Reader

    def builder() = new Builder
}


class SystemSettings {
    @JsonProperty(value="plugins") private var _plugins: Seq[String] = Seq()

    def plugins : Seq[String] = _plugins
}
