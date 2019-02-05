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
        val task = new DescribeMappingTask
        task._mapping = mapping
        task
    }
}


class DescribeMappingTask extends BaseTask {
    private val logger = LoggerFactory.getLogger(classOf[DescribeMappingTask])

    @JsonProperty(value="mapping", required=true) private var _mapping:String = _

    def mapping(implicit context: Context) : MappingIdentifier = MappingIdentifier.parse(context.evaluate(_mapping))

    override def execute(executor:Executor) : Boolean = {
        implicit val context = executor.context
        val identifier = this.mapping
        logger.info(s"Describing mapping '$identifier")

        val df = executor.instantiate(identifier)
        df.printSchema()
        true
    }
}
