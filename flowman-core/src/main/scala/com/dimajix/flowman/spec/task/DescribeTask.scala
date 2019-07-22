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


case class DescribeTask(
    instanceProperties:Task.Properties,
    dataset:Dataset,
    useSpark:Boolean
) extends BaseTask {
    private val logger = LoggerFactory.getLogger(classOf[DescribeTask])

    override def execute(executor:Executor) : Boolean = {
        logger.info(s"Describing dataset '$dataset")

        if (useSpark) {
            val df = dataset.read(executor, None)
            df.printSchema()
        }
        else {
            val schema = dataset.schema
            schema match {
                case Some(s) => s.printTree()
                case None => logger.error(s"Cannot infer schema of dataset '$dataset'")
            }
        }
        true
    }
}

class DescribeTaskSpec extends TaskSpec {
    @JsonProperty(value = "input", required = true) private var dataset: DatasetSpec = _

    override def instantiate(context: Context): DescribeTask = {
        DescribeTask(
            instanceProperties(context),
            dataset.instantiate(context),
            true
        )
    }
}
