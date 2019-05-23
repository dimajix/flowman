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


object CreateDatabaseTask {
    def apply(context: Context, database:String, ignoreIfExists:Boolean) : CreateDatabaseTask = {
        CreateDatabaseTask(
            Task.Properties(context),
            database,
            ignoreIfExists
        )
    }
}

case class CreateDatabaseTask(
    instanceProperties:Task.Properties,
    database:String,
    ignoreIfExists:Boolean
) extends BaseTask {
    private val logger = LoggerFactory.getLogger(classOf[CreateDatabaseTask])

    /**
      * Instantiates the specified database
      *
      * @param executor
      * @return
      */
    override def execute(executor:Executor) : Boolean = {
        require(executor != null)

        logger.info(s"Creating Hive database '$database'")
        executor.catalog.createDatabase(database, ignoreIfExists)
        true
    }
}



class CreateDatabaseTaskSpec extends TaskSpec {
    @JsonProperty(value = "database", required = true) private var database: String = ""
    @JsonProperty(value = "ignoreIfExists", required = false) private var ignoreIfExists: String = "false"

    override def instantiate(context: Context): Task = {
        CreateDatabaseTask(
            instanceProperties(context),
            context.evaluate(database),
            context.evaluate(ignoreIfExists).toBoolean
        )
    }
}
