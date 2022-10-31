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

package com.dimajix.flowman.spec

import java.io.IOException
import java.io.InputStream

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.JsonMappingException

import com.dimajix.flowman.fs.File
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.spi.ModuleReader


class YamlModuleReader extends ModuleReader {

    override def name: String = "yaml module reader"

    override def format: String = "yaml"

    /**
     * Returns true if a given format is supported by this reader
     * @param format
     * @return
     */
    override def supports(format: String): Boolean = format == "yaml" || format == "yml"

    /**
     * Returns a list of glob patterns to be used for finding files.
     *
     * @return
     */
    override def globPatterns: Seq[String] = Seq("*.yml", "*.yaml")

    /**
     * Loads a single file or a whole directory (non recursibely)
     *
     * @param file
     * @return
     */
    @throws[IOException]
    @throws[JsonProcessingException]
    @throws[JsonMappingException]
    override def file(file:File) : Module = {
        ObjectMapper.read[ModuleSpec](file).instantiate()
    }

    @throws[IOException]
    @throws[JsonProcessingException]
    @throws[JsonMappingException]
    override def stream(stream:InputStream) : Module = {
        ObjectMapper.read[ModuleSpec](stream).instantiate()
    }

    @throws[IOException]
    @throws[JsonProcessingException]
    @throws[JsonMappingException]
    override def string(text:String) : Module = {
        ObjectMapper.parse[ModuleSpec](text).instantiate()
    }
}
