/*
 * Copyright (C) 2022 The Flowman Authors
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

package com.dimajix.flowman.spec.mapping

import java.net.URL
import java.util.Locale

import scala.annotation.tailrec

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.DataFrame

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.ExecutionException
import com.dimajix.flowman.fs.File
import com.dimajix.flowman.fs.FileUtils
import com.dimajix.flowman.model.BaseMapping
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.types.StructType
import com.dimajix.spark.sql.DataFrameBuilder
import com.dimajix.spark.sql.DataFrameUtils
import com.dimajix.spark.sql.DataFrameUtils.withTempView
import com.dimajix.spark.sql.DataFrameUtils.withTempViews
import com.dimajix.spark.sql.SqlParser


final case class IterativeSqlMapping(
    instanceProperties:Mapping.Properties,
    input:MappingOutputIdentifier,
    sql:Option[String],
    file:Option[File] = None,
    url:Option[URL] = None,
    maxIterations:Int = 99
)
extends BaseMapping {
    /**
     * Resolves all dependencies required to build the SQL
     *
     * @return
     */
    override def inputs : Set[MappingOutputIdentifier] = dependencies

    /**
     * Executes this MappingType and returns a corresponding DataFrame
     *
     * @param execution
     * @param input
     * @return
     */
    override def execute(execution:Execution, input:Map[MappingOutputIdentifier,DataFrame]) : Map[String,DataFrame] = {
        require(execution != null)
        require(input != null)

        val statement = this.statement

        @tailrec
        def fix(in:DataFrame, iteration:Int=1) : DataFrame = {
            if (iteration > maxIterations)
                throw new ExecutionException(s"Recursive mapping '$identifier' exceeded maximum iterations $maxIterations")

            val result = nextDf(statement, in)
            if (!DataFrameUtils.compare(in, result))
                fix(result.localCheckpoint(false), iteration+1)
            else
                result
        }

        // Register all input DataFrames as temp views
        val result = withTempViews(input.map(kv => kv._1.name -> kv._2)) {
            val first = nextDf(statement, input(this.input))
            fix(first)
        }

        Map("main" -> result)
    }

    private def nextDf(statement:String, prev:DataFrame) : DataFrame = {
        val spark = prev.sparkSession
        withTempView("__this__", prev) {
            spark.sql(statement)
        }
    }


    /**
     * Returns the schema as produced by this mapping, relative to the given input schema. The map might not contain
     * schema information for all outputs, if the schema cannot be inferred.
     * @param input
     * @return
     */
    override def describe(execution: Execution, input: Map[MappingOutputIdentifier, StructType]): Map[String, StructType] = {
        require(execution != null)
        require(input != null)

        val spark = execution.spark

        // Create dummy data frames
        val replacements = input.map { case (id,schema) =>
            id.name -> DataFrameBuilder.singleRow(spark, schema.sparkType)
        }
        val firstDf = replacements(this.input.name)

        val result = withTempViews(replacements) {
            nextDf(statement, firstDf)
        }

        // Apply documentation
        val schemas = Map("main" -> StructType.of(result.schema))
        applyDocumentation(schemas)
    }

    private lazy val dependencies = {
        SqlParser.resolveDependencies(statement)
            .filter(_.toLowerCase(Locale.ROOT) != "__this__")
            .map(MappingOutputIdentifier.parse) + input
    }
    private lazy val statement : String = {
        if (sql.exists(_.nonEmpty)) {
            sql.get
        }
        else if (file.nonEmpty) {
            FileUtils.toString(file.get)
        }
        else if (url.nonEmpty) {
            IOUtils.toString(url.get, "UTF-8")
        }
        else {
            throw new IllegalArgumentException("SQL mapping needs either 'sql', 'file' or 'url'")
        }
    }
}


class IterativeSqlMappingSpec extends MappingSpec {
    @JsonProperty(value="input", required = true) private var input: String = _
    @JsonProperty(value="sql", required=false) private var sql:Option[String] = None
    @JsonProperty(value="file", required=false) private var file:Option[String] = None
    @JsonProperty(value="url", required=false) private var url: Option[String] = None
    @JsonProperty(value="maxIterations", required=false) private var maxIterations: String = "99"

    /**
     * Creates the instance of the specified Mapping with all variable interpolation being performed
     * @param context
     * @return
     */
    override def instantiate(context: Context, properties:Option[Mapping.Properties] = None): IterativeSqlMapping = {
        IterativeSqlMapping(
            instanceProperties(context, properties),
            MappingOutputIdentifier(context.evaluate(input)),
            context.evaluate(sql),
            file.map(context.evaluate).filter(_.nonEmpty).map(p => context.fs.file(p)),
            url.map(context.evaluate).filter(_.nonEmpty).map(u => new URL(u)),
            context.evaluate(maxIterations).toInt
        )
    }
}
