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
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.schema.Schema
import com.dimajix.flowman.spec.schema.SchemaSpec
import com.dimajix.flowman.types.SchemaWriter


case class SaveSchemaTask(
    instanceProperties: Task.Properties,
    schema: Schema,
    location:Path,
    format:String
) extends BaseTask {
    private val logger = LoggerFactory.getLogger(classOf[SaveSchemaTask])

    /**
      * Main method which will perform the given task.
      *
      * @param executor
      */
    override def execute(executor: Executor): Boolean = {
        val file = context.fs.local(location)
        new SchemaWriter(schema.fields).format(format).save(file)
        true
    }
}

class SaveSchemaTaskSpec extends TaskSpec {
    @JsonProperty(value="schema", required=true) private var schema:SchemaSpec = _
    @JsonProperty(value="location", required=true) private var location:String = _
    @JsonProperty(value="format", required=false) private var format:String = "avro"

    override def instantiate(context: Context): SaveSchemaTask = {
        SaveSchemaTask(
            instanceProperties(context),
            schema.instantiate(context),
            new Path(context.evaluate(location)),
            context.evaluate(format)
        )
    }
}
