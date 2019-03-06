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
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.hadoop.FileSystem


class DeleteFileTask extends BaseTask {
    private val logger = LoggerFactory.getLogger(classOf[DeleteFileTask])

    @JsonProperty(value="path", required=true) private var _path:String = ""
    @JsonProperty(value="recursive", required=false) private var _recusrive:String = "false"

    def path(implicit context:Context) : String = context.evaluate(_path)
    def recursive(implicit context:Context) : Boolean = context.evaluate(_recusrive).toBoolean

    override def execute(executor:Executor) : Boolean = {
        implicit val context = executor.context
        val fs = new FileSystem(executor.hadoopConf)
        val file = fs.file(path)
        logger.info(s"Deleting remote file '$file' (recursive=$recursive)")
        file.delete(recursive)
        true
    }
}
