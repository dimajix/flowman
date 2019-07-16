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

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.MappingOutputIdentifier
import com.dimajix.flowman.spec.target.FileTarget
import com.dimajix.flowman.spec.target.Target


object SaveMappingTask {
    def apply(context: Context,
              mapping:MappingOutputIdentifier,
              location:Path,
              format:String="csv",
              options:Map[String,String]=Map(),
              mode:String="overwrite") : SaveMappingTask = {
        SaveMappingTask(
            Task.Properties(context),
            mapping,
            location,
            format,
            options,
            mode,
            8,
            false
        )
    }
}


case class SaveMappingTask(
    instanceProperties: Task.Properties,
    mapping:MappingOutputIdentifier,
    location:Path,
    format:String,
    options:Map[String,String],
    mode: String,
    parallelism: Int,
    rebalance: Boolean) extends BaseTask {
    private val logger = LoggerFactory.getLogger(classOf[SaveMappingTask])

    /**
      * Abstract method which will perform the given task.
      *
      * @param executor
      */
    override def execute(executor: Executor): Boolean = {
        logger.info(s"Saving contents of mapping '$mapping' to '$location'")

        val target = FileTarget(
            Target.Properties(context).copy(input = mapping),
            location,
            format,
            options,
            mode,
            8,
            rebalance
        )

        // Instantiate all required dependencies (there should only be one)
        val dependencies = target.dependencies
            .map(d => (d, context.getMapping(d.mapping)))
            .map { case (id, mapping) => (id, executor.instantiate(mapping, id.output)) }
            .toMap

        target.build(executor, dependencies)
        true
    }
}

class SaveMappingTaskSpec extends TaskSpec {
    @JsonProperty(value="mapping", required=true) private var mapping:String = _
    @JsonProperty(value="location", required=true) private var location:String = _
    @JsonProperty(value="format", required=false) private var format:String = "csv"
    @JsonProperty(value="mode", required=false) private var mode:String = "overwrite"
    @JsonProperty(value="options", required=false) private var options:Map[String,String] = Map()
    @JsonProperty(value="parallelism", required=false) private var parallelism:String = "16"
    @JsonProperty(value="rebalance", required=false) private var rebalance:String = "false"

    override def instantiate(context: Context): SaveMappingTask = {
        SaveMappingTask(
            instanceProperties(context),
            MappingOutputIdentifier(mapping),
            new Path(context.evaluate(location)),
            context.evaluate(format),
            context.evaluate(options),
            context.evaluate(mode),
            context.evaluate(parallelism).toInt,
            context.evaluate(rebalance).toBoolean
        )
    }
}
