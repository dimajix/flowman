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

package com.dimajix.flowman.spec.target

import java.util.Locale

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.OutputMode
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spec.RelationIdentifier


class StreamTarget extends BaseTarget {
    private val logger = LoggerFactory.getLogger(classOf[StreamTarget])
    private lazy val ts =  System.currentTimeMillis()

    @JsonProperty(value="relatiom", required=true) private var _relation:String = _
    @JsonProperty(value="mode", required=false) private var _mode:String = OutputMode.Update().toString
    @JsonProperty(value="checkpointLocation", required=false) private var _checkpointLocation:String = _
    @JsonProperty(value="parallelism", required=false) private var _parallelism:String = "16"

    def relation(implicit context: Context) : RelationIdentifier = RelationIdentifier.parse(context.evaluate(_relation))
    def mode(implicit context: Context) : OutputMode = context.evaluate(_mode).toUpperCase(Locale.ROOT) match {
        case "APPEND" => OutputMode.Append()
        case "COMPLETE" => OutputMode.Complete()
        case "UPDATE" => OutputMode.Update()
        case mode:String => throw new IllegalArgumentException(s"Unsupported output mode '$mode'")
    }
    def parallelism(implicit context: Context) : Integer = context.evaluate(_parallelism).toInt
    def checkpointLocation(implicit context: Context) : Path = new Path(
        Option(context.evaluate(_checkpointLocation))
            .map(_.trim)
            .filter(_.nonEmpty)
            .getOrElse("/tmp/flowman-streaming-sink-" + name + "-" + ts)
    )

    /**
      * Abstract method which will perform the output operation. All required tables need to be
      * registered as temporary tables in the Spark session before calling the execute method.
      *
      * @param executor
      */
    override def execute(executor: Executor, tables: Map[MappingIdentifier, DataFrame]): Unit = {
        implicit var context = executor.context
        val target = this.relation
        val input = this.input
        val mode = this.mode
        val checkpointLocation = this.checkpointLocation

        logger.info(s"Writing mapping '$input' to streaming relation '$target' using mode '$mode' and checkpoint location '$checkpointLocation'")
        val relation = context.getRelation(target)
        val table = tables(input).coalesce(parallelism)
        relation.writeStream(executor, table, mode, checkpointLocation)
    }
}
