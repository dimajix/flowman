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

import com.dimajix.flowman.hadoop.File
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.spi.ProjectReader


class YamlProjectReader extends ProjectReader {

    override def name: String = "yaml project reader"

    override def format: String = "yaml"

    override def supports(format: String): Boolean = format == "yaml" || format == "yml"

    /**
     * Loads a single file or a whole directory (non recursibely)
     *
     * @param file
     * @return
     */
    @throws[IOException]
    override def file(file:File) : Project = {
        ObjectMapper.read[ProjectSpec](file).instantiate()
    }

    @throws[IOException]
    override def string(text:String) : Project = {
        ObjectMapper.parse[ProjectSpec](text).instantiate()
    }
}
