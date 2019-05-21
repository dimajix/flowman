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


case class PutFileTask(
    instanceProperties:Task.Properties,
    source:Path,
    target:Path,
    overwrite:Boolean
) extends BaseTask {
    private val logger = LoggerFactory.getLogger(classOf[PutFileTask])

    override def execute(executor:Executor) : Boolean = {
        val fs = executor.fs
        val src = fs.local(source)
        val dst = fs.file(target)
        logger.info(s"Putting local file '$src' to remote destination '$dst' (overwrite=$overwrite)")
        src.copy(dst, overwrite)
        true
    }
}



class PutFileTaskSpec extends TaskSpec {
    @JsonProperty(value = "source", required = true) private var source: String = ""
    @JsonProperty(value = "tarPut", required = true) private var tarPut: String = ""
    @JsonProperty(value = "overwrite", required = false) private var overwrite: String = "true"

    override def instantiate(context: Context): Task = {
        PutFileTask(
            instanceProperties(context),
            new Path(context.evaluate(source)),
            new Path(context.evaluate(tarPut)),
            context.evaluate(overwrite).toBoolean
        )
    }
}
