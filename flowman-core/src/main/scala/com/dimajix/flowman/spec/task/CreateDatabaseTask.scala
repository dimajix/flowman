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
    def apply(database:String, ignoreIfExists:Boolean) : CreateDatabaseTask = {
        val result = new CreateDatabaseTask
        result._database = database
        result._ignoreIfExists = ignoreIfExists.toString
        result
    }
}

class CreateDatabaseTask extends BaseTask {
    private val logger = LoggerFactory.getLogger(classOf[CreateDatabaseTask])

    @JsonProperty(value="database", required=true) private var _database:String = ""
    @JsonProperty(value="ignoreIfExists", required=false) private var _ignoreIfExists:String = "false"

    def database(implicit context: Context) : String = context.evaluate(_database)
    def ignoreIfExists(implicit context: Context) : Boolean = context.evaluate(_ignoreIfExists).toBoolean

    /**
      * Instantiates the specified database
      *
      * @param executor
      * @return
      */
    override def execute(executor:Executor) : Boolean = {
        require(executor != null)

        implicit val context = executor.context
        val database = this.database
        val ignoreIfExists = this.ignoreIfExists
        logger.info(s"Creating Hive database '$database'")

        executor.catalog.createDatabase(database, ignoreIfExists)
        true
    }
}
