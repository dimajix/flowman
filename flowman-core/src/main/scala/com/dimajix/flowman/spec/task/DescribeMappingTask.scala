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
import com.dimajix.flowman.spec.MappingIdentifier


object DescribeMappingTask {
    def apply(mapping:String) : DescribeMappingTask = {
        DescribeMappingTask(
            Task.Properties(null),
            MappingIdentifier(mapping)
        )
    }
}

case class DescribeMappingTask(
    instanceProperties:Task.Properties,
    mapping:MappingIdentifier
) extends BaseTask {
    private val logger = LoggerFactory.getLogger(classOf[DescribeMappingTask])

    override def execute(executor:Executor) : Boolean = {
        logger.info(s"Describing mapping '$mapping")

        val df = executor.instantiate(mapping)
        df.printSchema()
        true
    }
}



class DescribeMappingTaskSpec extends TaskSpec {
    @JsonProperty(value = "mapping", required = true) private var _mapping: String = _


    override def instantiate(context: Context): DescribeMappingTask = {
        DescribeMappingTask(
            instanceProperties(context),
            MappingIdentifier.parse(context.evaluate(_mapping))
        )
    }
}
