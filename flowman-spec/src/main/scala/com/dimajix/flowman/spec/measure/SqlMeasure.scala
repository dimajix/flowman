/*
 * Copyright 2021-2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.measure

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.ByteType
import org.apache.spark.sql.types.CharType
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.NumericType
import org.apache.spark.sql.types.ShortType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.VarcharType
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.model.BaseMeasure
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Measure
import com.dimajix.flowman.model.MeasureResult
import com.dimajix.flowman.model.Measurement
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.spark.sql.DataFrameUtils
import com.dimajix.spark.sql.SqlParser


case class SqlMeasure(
    override val instanceProperties:Measure.Properties,
    query: String
)
extends BaseMeasure {
    private val logger = LoggerFactory.getLogger(classOf[SqlMeasure])

    /**
     * Returns a list of physical resources required by this measure. This list will only be non-empty for assertions
     * which actually read from physical data.
     *
     * @return
     */
    override def requires: Set[ResourceIdentifier] = Set()

    /**
     * Returns the dependencies (i.e. names of tables in the Dataflow model)
     *
     * @return
     */
    override def inputs: Seq[MappingOutputIdentifier] = {
        SqlParser.resolveDependencies(query).toSeq.map(MappingOutputIdentifier.parse)
    }

    /**
     * Executes this [[Measure]] and returns a corresponding DataFrame.
     *
     * @param execution
     * @param input
     * @return
     */
    override def execute(execution: Execution, input: Map[MappingOutputIdentifier, DataFrame]): MeasureResult = {
        MeasureResult.of(this) {
            DataFrameUtils.withTempViews(input.map(kv => kv._1.name -> kv._2)) {
                val result = execution.spark.sql(query)
                val schema = result.schema
                val (labelColumns,metricColumns) = schema.fields.partition { field => field.dataType match {
                    case _:NumericType => false
                    case StringType => true
                    case VarcharType(_) => true
                    case CharType(_) => true
                    case BooleanType => true
                    case dt => throw new IllegalArgumentException(s"Unsupported column type $dt in column ${field.name}")
                }}

                val maxRecords = 100
                val allRows = result.take(maxRecords + 1)
                if (allRows.length > maxRecords) {
                    logger.warn(s"Result of measure '$name' conatins more than $maxRecords records. Only the first $maxRecords records will be used")
                }

                val metadata = this.metadata.asMap
                allRows.take(maxRecords)
                    .flatMap { row =>
                        val labels = metadata ++ extractLabels(row, labelColumns)
                        val metrics = extractMetrics(row, metricColumns)
                        metrics.map { case(name,value) =>
                            Measurement(name, labels, value)
                        }
                    }
            }
        }
    }

    private def extractLabels(row:Row, labels:Seq[StructField]) : Map[String,String] = {
        labels.map { field =>
            val value = field.dataType match {
                case StringType => row.getAs[String](field.name)
                case CharType(_) => row.getAs[String](field.name)
                case VarcharType(_) => row.getAs[String](field.name)
                case BooleanType => row.getAs[Boolean](field.name).toString
            }
            field.name -> value
        }.toMap
    }

    private def extractMetrics(row:Row, metrics:Seq[StructField]) : Map[String,Double] = {
        metrics.map { field =>
            val value = field.dataType match {
                case ByteType => row.getAs[Byte](field.name).toDouble
                case ShortType => row.getAs[Short](field.name).toDouble
                case IntegerType => row.getAs[Int](field.name).toDouble
                case LongType => row.getAs[Long](field.name).toDouble
                case FloatType => row.getAs[Float](field.name).toDouble
                case DoubleType => row.getAs[Double](field.name)
                case _:DecimalType => row.getAs[BigDecimal](field.name).toDouble
            }
            field.name -> value
        }.toMap
    }
}


class SqlMeasureSpec extends MeasureSpec {
    @JsonProperty(value="query", required=true) private var query:String = ""
    @JsonProperty(value="expected", required=false) private var expected:Seq[Array[String]] = Seq()

    override def instantiate(context: Context, properties:Option[Measure.Properties] = None): SqlMeasure = {
        SqlMeasure(
            instanceProperties(context, properties),
            context.evaluate(query)
        )
    }
}
