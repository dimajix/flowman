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

package com.dimajix.flowman.namespace.monitor

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.spec.task.Job
import com.dimajix.flowman.spi.TypeRegistry



object Monitor extends TypeRegistry[Monitor] {
}


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind")
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "null", value = classOf[NullMonitor]),
    new JsonSubTypes.Type(name = "jdbc", value = classOf[JdbcMonitor])
))
abstract class Monitor {
    /**
      * Performs some check, if the run is required
      * @param context
      * @return
      */
    def check(context:Context, job:Job, args:Map[String,String]) : Boolean

    /**
      * Starts the run and returns a token, which can be anything
      *
      * @param context
      * @return
      */
    def start(context:Context, job:Job, args:Map[String,String]) : Object

    /**
      * Marks a run as a success
      *
      * @param context
      * @param token
      */
    def success(context: Context, token:Object) : Unit

    /**
      * Marks a run as a failure
      *
      * @param context
      * @param token
      */
    def failure(context: Context, token:Object) : Unit

    /**
      * Marks a run as a failure
      *
      * @param context
      * @param token
      */
    def aborted(context: Context, token:Object) : Unit

    /**
      * Marks a run as being skipped
      *
      * @param context
      * @param token
      */
    def skipped(context: Context, token:Object) : Unit
}
