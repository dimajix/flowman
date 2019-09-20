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

package com.dimajix.flowman.spec.task

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.target.TargetSpec


case class DeleteFileTask(
    instanceProperties:Task.Properties,
    path: Path,
    recursive: Boolean
) extends BaseTask {
    private val logger = LoggerFactory.getLogger(classOf[DeleteFileTask])

    override def execute(executor:Executor) : Boolean = {
        val fs = executor.fs
        val file = fs.file(path)
        logger.info(s"Deleting remote file '$file' (recursive=$recursive)")
        file.delete(recursive)
        true
    }
}



class DeleteFileTaskSpec extends TaskSpec {
    @JsonProperty(value = "path", required = true) private var _path: String = ""
    @JsonProperty(value = "recursive", required = false) private var _recusrive: String = "false"

    override def instantiate(context: Context): DeleteFileTask = {
        DeleteFileTask(
            instanceProperties(context),
            new Path(context.evaluate(_path)),
            context.evaluate(_recusrive).toBoolean
        )
    }
}
