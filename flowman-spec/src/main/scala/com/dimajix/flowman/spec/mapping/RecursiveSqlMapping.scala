/*
 * Copyright 2018-2022 Kaya Kupferschmidt
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

import java.io.StringWriter
import java.net.URL
import java.nio.charset.Charset
import java.util.Locale

import scala.annotation.tailrec

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.UnaryNode
import org.apache.spark.sql.catalyst.plans.logical.Union

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.model.BaseMapping
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.types.StructType
import com.dimajix.spark.sql.DataFrameBuilder
import com.dimajix.spark.sql.DataFrameUtils
import com.dimajix.spark.sql.DataFrameUtils.withTempView
import com.dimajix.spark.sql.DataFrameUtils.withTempViews
import com.dimajix.spark.sql.SqlParser

case class RecursiveSqlMapping(
    instanceProperties:Mapping.Properties,
    sql:Option[String],
    file:Option[Path],
    url:Option[URL]
)
extends BaseMapping {
    /**
      * Resolves all dependencies required to build the SQL
      *
      * @return
      */
    override def inputs : Set[MappingOutputIdentifier] = {
        SqlParser.resolveDependencies(statement)
            .filter(_.toLowerCase(Locale.ROOT) != "__this__")
            .map(MappingOutputIdentifier.parse)
    }

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
        def fix(in:DataFrame, inCount:Long) : DataFrame = {
            val result = nextDf(statement, in)
            val resultCount = result.count()
            if (resultCount != inCount)
                fix(result, resultCount)
            else
                result
        }

        // Register all input DataFrames as temp views
        val result = withTempViews(input.map(kv => kv._1.name -> kv._2)) {
            val first = firstDf(execution.spark, statement)
            fix(first, first.count())
        }

        Map("main" -> result)
    }

    private def firstDf(spark:SparkSession, statement:String) : DataFrame = {
        @tailrec
        def findUnion(plan:LogicalPlan) : LogicalPlan = {
            plan match {
                case union:Union => union
                case node:UnaryNode =>findUnion(node.child)
            }
        }

        val plan = SqlParser.parsePlan(statement)
        val union = findUnion(plan)
        val firstChild = union.children.head
        val resolvedStart = spark.sessionState.analyzer.execute(firstChild)
        new Dataset[Row](spark, firstChild, RowEncoder(resolvedStart.schema))
    }
    private def nextDf(statement:String, prev:DataFrame) : DataFrame = {
        val spark = prev.sparkSession
        withTempView("__this__", prev) {
            spark.sql(statement).localCheckpoint(false)
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
        val statement = this.statement

        // Create dummy data frames
        val replacements = input.map { case (id,schema) =>
            id.name -> DataFrameBuilder.singleRow(spark, schema.sparkType)
        }

        val result = withTempViews(replacements) {
            firstDf(spark, statement)
        }

        // Apply documentation
        val schemas = Map("main" -> StructType.of(result.schema))
        applyDocumentation(schemas)
    }

    private def statement : String = {
        if (sql.exists(_.nonEmpty)) {
            sql.get
        }
        else if (file.nonEmpty) {
            val fs = context.fs
            val input = fs.file(file.get).open()
            try {
                val writer = new StringWriter()
                IOUtils.copy(input, writer, Charset.forName("UTF-8"))
                writer.toString
            }
            finally {
                input.close()
            }
        }
        else if (url.nonEmpty) {
            IOUtils.toString(url.get, "UTF-8")
        }
        else {
            throw new IllegalArgumentException("SQL mapping needs either 'sql', 'file' or 'url'")
        }
    }
}


class RecursiveSqlMappingSpec extends MappingSpec {
    @JsonProperty(value="sql", required=false) private var sql:Option[String] = None
    @JsonProperty(value="file", required=false) private var file:Option[String] = None
    @JsonProperty(value="url", required=false) private var url: Option[String] = None

    /**
      * Creates the instance of the specified Mapping with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context): RecursiveSqlMapping = {
        RecursiveSqlMapping(
            instanceProperties(context),
            context.evaluate(sql),
            file.map(context.evaluate).filter(_.nonEmpty).map(p => new Path(p)),
            url.map(context.evaluate).filter(_.nonEmpty).map(u => new URL(u))
        )
    }
}
