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

package com.dimajix.flowman.spec.documentation

import org.apache.hadoop.fs.Path

import com.dimajix.flowman.documentation.Documenter
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.model.Project


object DocumenterLoader {
    def load(context: Context, project: Project): Documenter = {
        project.basedir.flatMap { basedir =>
            val docpath = basedir / "documentation.yml"
            if (docpath.isFile()) {
                val file = Documenter.read.file(docpath)
                Some(file.instantiate(context))
            }
            else {
                Some(defaultDocumenter((basedir / "generated-documentation").path))
            }
        }.getOrElse {
            defaultDocumenter(new Path("/tmp/flowman/generated-documentation"))
        }
    }

    private def defaultDocumenter(outputDir: Path): Documenter = {
        val generators = Seq(
            new FileGenerator(outputDir)
        )
        Documenter.read.default().copy(generators = generators)
    }
}
