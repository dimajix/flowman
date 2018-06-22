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
import com.dimajix.flowman.fs.FileSystem


class PutFileTask extends BaseTask {
    private val logger = LoggerFactory.getLogger(classOf[PutFileTask])

    @JsonProperty(value="source", required=true) private var _source:String = ""
    @JsonProperty(value="target", required=true) private var _target:String = ""
    @JsonProperty(value="overwrite", required=false) private var _overwrite:String = "true"

    def source(implicit context:Context) : String = context.evaluate(_source)
    def target(implicit context:Context) : String = context.evaluate(_target)
    def overwrite(implicit context:Context) : Boolean = context.evaluate(_overwrite).toBoolean

    override def execute(executor:Executor) : Boolean = {
        implicit val context = executor.context
        val fs = new FileSystem(executor.hadoopConf)
        val src = fs.local(source)
        val dst = fs.remote(target)
        logger.info(s"Putting local file '$src' to remote destination '$dst' (overwrite=$overwrite)")
        src.copy(dst, overwrite)
        true
    }
}
