/*
 * Copyright 2019 Kaya Kupferschmidt
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
import com.dimajix.flowman.spec.dataset.Dataset
import com.dimajix.flowman.spec.dataset.DatasetSpec
import com.dimajix.flowman.transforms.SchemaEnforcer


case class CopyTask(
    instanceProperties:Task.Properties,
    source:Dataset,
    target:Dataset,
    parallelism:Int,
    mode:String
) extends BaseTask {
    private val logger = LoggerFactory.getLogger(classOf[CopyTask])

    override def execute(executor:Executor) : Boolean = {
        logger.info(s"Copying dataset ${source.name} to ${target.name}")

        val data = source.read(executor, None).coalesce(parallelism)
        val conformed = target.schema.map { schema =>
            val xfs = SchemaEnforcer(schema.sparkType)
            xfs.transform(data)
        }.getOrElse(data)
        target.write(executor, conformed, mode)
        true
    }
}


class CopyTaskSpec extends TaskSpec {
    @JsonProperty(value = "source", required = true) private var source: DatasetSpec = _
    @JsonProperty(value = "target", required = true) private var target: DatasetSpec = _
    @JsonProperty(value = "parallelism", required = false) private var parallelism: String = "16"
    @JsonProperty(value = "mode", required = false) private var mode: String = "overwrite"


    override def instantiate(context: Context): CopyTask = {
        CopyTask(
            instanceProperties(context),
            source.instantiate(context),
            target.instantiate(context),
            context.evaluate(parallelism).toInt,
            context.evaluate(mode)
        )
    }
}
