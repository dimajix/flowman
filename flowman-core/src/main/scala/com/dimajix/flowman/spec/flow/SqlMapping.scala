/*
 * Copyright 2018-2019 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.flow

import java.io.StringWriter
import java.net.URL
import java.nio.charset.Charset

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.DataFrame

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.hadoop.File
import com.dimajix.flowman.spec.MappingOutputIdentifier
import com.dimajix.spark.sql.SqlParser


case class SqlMapping(
    instanceProperties:Mapping.Properties,
    sql:String,
    file:File,
    url:URL
)
extends BaseMapping {
    /**
      * Executes this MappingType and returns a corresponding DataFrame
      *
      * @param executor
      * @param input
      * @return
      */
    override def execute(executor:Executor, input:Map[MappingOutputIdentifier,DataFrame]) : Map[String,DataFrame] = {
        require(executor != null)
        require(input != null)

        // Register all input DataFrames as temp views
        input.foreach(kv => kv._2.createOrReplaceTempView(kv._1.name))
        // Execute query
        val result = executor.spark.sql(statement)
        // Call SessionCatalog.dropTempView to avoid unpersisting the possibly cached dataset.
        input.foreach(kv => executor.spark.sessionState.catalog.dropTempView(kv._1.name))

        Map("main" -> result)
    }

    /**
      * Resolves all dependencies required to build the SQL
      *
      * @return
      */
    override def inputs : Seq[MappingOutputIdentifier] = {
        SqlParser.resolveDependencies(statement).map(MappingOutputIdentifier.parse)
    }

    private def statement : String = {
        if (sql != null && sql.nonEmpty) {
            sql
        }
        else if (file != null) {
            val input = file.open()
            try {
                val writer = new StringWriter()
                IOUtils.copy(input, writer, Charset.forName("UTF-8"))
                writer.toString
            }
            finally {
                input.close()
            }
        }
        else if (url != null) {
            IOUtils.toString(url)
        }
        else {
            throw new IllegalArgumentException("SQL mapping needs either 'sql', 'file' or 'url'")
        }
    }
}



class SqlMappingSpec extends MappingSpec {
    @JsonProperty(value="sql", required=false) private var sql:String = _
    @JsonProperty(value="file", required=false) private var file:String = _
    @JsonProperty(value="url", required=false) private var url: String = _

    /**
      * Creates the instance of the specified Mapping with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context): SqlMapping = {
        SqlMapping(
            instanceProperties(context),
            context.evaluate(sql),
            Option(file).map(context.evaluate).filter(_.nonEmpty).map(context.fs.file).orNull,
            Option(url).map(context.evaluate).filter(_.nonEmpty).map(u => new URL(u)).orNull
        )
    }
}
