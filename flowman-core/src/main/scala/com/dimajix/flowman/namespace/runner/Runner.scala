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

package com.dimajix.flowman.namespace.runner

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo

import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.task.Job
import com.dimajix.flowman.spec.task.JobStatus
import com.dimajix.flowman.spi.ExtensionRegistry


object Runner extends ExtensionRegistry[Runner] {
}


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind")
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "simple", value = classOf[SimpleRunner]),
    new JsonSubTypes.Type(name = "logged", value = classOf[JdbcLoggedRunner])
))
abstract class Runner {
    def execute(executor: Executor, job:Job, args:Map[String,String] = Map()) : JobStatus
}
