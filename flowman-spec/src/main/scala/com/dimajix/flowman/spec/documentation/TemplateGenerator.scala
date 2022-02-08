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

package com.dimajix.flowman.spec.documentation

import java.net.URL
import java.nio.charset.Charset

import com.google.common.io.Resources

import com.dimajix.flowman.documentation.BaseGenerator
import com.dimajix.flowman.documentation.ProjectDoc
import com.dimajix.flowman.documentation.ProjectDocWrapper
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution


abstract class TemplateGenerator(
    template:URL
) extends BaseGenerator {
    override def generate(context:Context, execution: Execution, documentation: ProjectDoc): Unit = {
        val temp = loadResource("project.vtl")
        val result = context.evaluate(temp, Map("project" -> ProjectDocWrapper(documentation)))
        println(result)
    }

    private def loadResource(name: String): String = {
        val path = template.getPath
        val url =
            if (path.endsWith("/"))
                new URL(template.toString + name)
            else
                new URL(template.toString + "/" + name)
        Resources.toString(url, Charset.forName("UTF-8"))
    }
}
