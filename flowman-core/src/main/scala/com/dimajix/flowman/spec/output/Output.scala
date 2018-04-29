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

package com.dimajix.flowman.spec.output

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import org.apache.spark.sql.DataFrame

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.TableIdentifier
import com.dimajix.flowman.spi.Scanner


object Output {
    def subtypes : Seq[(String,Class[_ <: Output])] = Scanner.outputs
}


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "blackhole", value = classOf[BlackholeOutput]),
    new JsonSubTypes.Type(name = "count", value = classOf[CountOutput]),
    new JsonSubTypes.Type(name = "dump", value = classOf[DumpOutput]),
    new JsonSubTypes.Type(name = "local", value = classOf[LocalOutput]),
    new JsonSubTypes.Type(name = "relation", value = classOf[RelationOutput]))
)
abstract class Output {
    def enabled(implicit context:Context) : Boolean

    /**
      * Returns the dependencies of this mapping, which is exactly one input table
      *
      * @param context
      * @return
      */
    def dependencies(implicit context: Context) : Array[TableIdentifier]

    /**
      * Abstract method which will perform the output operation. All required tables need to be
      * registered as temporary tables in the Spark session before calling the execute method.
      *
      * @param executor
      */
    def execute(executor:Executor, input:Map[TableIdentifier,DataFrame]) : Unit
}
