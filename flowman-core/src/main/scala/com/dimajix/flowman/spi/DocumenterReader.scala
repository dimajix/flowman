/*
 * Copyright 2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.spi

import java.io.IOException

import com.dimajix.flowman.documentation.Documenter
import com.dimajix.flowman.hadoop.File
import com.dimajix.flowman.model.Prototype


abstract class DocumenterReader {
    /**
     * Returns the human readable name of the documenter file format
     * @return
     */
    def name: String

    /**
     * Returns the internally used short name of the documenter file format
     * @return
     */
    def format: String

    /**
     * Returns true if a given format is supported by this reader
     * @param format
     * @return
     */
    def supports(format: String): Boolean = this.format == format

    /**
     * Loads a [[Documenter]] from the given file
     * @param file
     * @return
     */
    @throws[IOException]
    def file(file: File): Prototype[Documenter]

    /**
     * Loads a [[Documenter]] from the given String
     * @param file
     * @return
     */
    @throws[IOException]
    def string(text: String): Prototype[Documenter]
}
