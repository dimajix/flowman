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

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spec.MappingOutputIdentifier


case class ConsoleTarget(
    instanceProperties:Target.Properties,
    limit:Int,
    header:Boolean,
    columns:Seq[String]
) extends BaseTarget {
    private val logger = LoggerFactory.getLogger(classOf[ConsoleTarget])

    /**
      * Build the "console" target by dumping records to stdout
      *
      * @param executor
      * @param input
      */
    override def build(executor:Executor, input:Map[MappingOutputIdentifier,DataFrame]) : Unit = {
        val dfIn = input(instanceProperties.input)
        val dfOut = if (columns.nonEmpty)
            dfIn.select(columns.map(c => dfIn(c)):_*)
        else
            dfIn

        val result = dfOut.limit(limit).collect()
        if (header) {
            println(dfOut.columns.mkString(","))
        }
        result.foreach(record => println(record.mkString(",")))
    }

    /**
      * Clean operation of dump essentially is a no-op
      * @param executor
      */
    override def clean(executor: Executor): Unit = {

    }
}



class ConsoleTargetSpec extends TargetSpec {
    @JsonProperty(value="limit", required=true) private var limit:String = "100"
    @JsonProperty(value="header", required=true) private var header:String = "true"
    @JsonProperty(value="columns", required=true) private var columns:Seq[String] = Seq()


    override def instantiate(context: Context): Target = {
        ConsoleTarget(
            instanceProperties(context),
            context.evaluate(limit).toInt,
            context.evaluate(header).toBoolean,
            columns.map(context.evaluate)
        )
    }

}