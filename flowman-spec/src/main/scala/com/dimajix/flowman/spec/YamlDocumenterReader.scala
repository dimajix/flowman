/*
 * Copyright (C) 2022 The Flowman Authors
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

import com.dimajix.flowman.documentation.Documenter
import com.dimajix.flowman.fs.File
import com.dimajix.flowman.model.Prototype
import com.dimajix.flowman.spec.documentation.DocumenterSpec
import com.dimajix.flowman.spi.DocumenterReader


class YamlDocumenterReader extends DocumenterReader {
    /**
     * Returns the human readable name of the documenter file format
     *
     * @return
     */
    override def name: String = "yaml documenter settings reader"

    /**
     * Returns the internally used short name of the documenter file format
     *
     * @return
     */
    override def format: String = "yaml"

    override def supports(format: String): Boolean = format == "yaml" || format == "yml"

    /**
     * Loads a [[Documenter]] from the given file
     *
     * @param file
     * @return
     */
    override def file(file: File): Prototype[Documenter] = {
        if (file.isDirectory()) {
            this.file(file / "documentation.yml")
        }
        else {
            ObjectMapper.read[DocumenterSpec](file)
        }
    }

    /**
     * Loads a [[Documenter]] from the given String
     *
     * @param file
     * @return
     */
    override def string(text: String): Prototype[Documenter] = {
        ObjectMapper.parse[DocumenterSpec](text)
    }
}
