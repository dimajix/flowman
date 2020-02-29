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

package com.dimajix.flowman.spec.mapping

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions.coalesce
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StringType

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.model.BaseMapping
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.spec.schema.SchemaSpec
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.{types => ftypes}


case class ExtractJsonMapping(
    instanceProperties:Mapping.Properties,
    input:MappingOutputIdentifier,
    column: String,
    schema: Schema,
    parseMode: String,
    allowComments: Boolean,
    allowUnquotedFieldNames: Boolean,
    allowSingleQuotes: Boolean,
    allowNumericLeadingZeros: Boolean,
    allowNonNumericNumbers: Boolean,
    allowBackslashEscapingAnyCharacter: Boolean,
    allowUnquotedControlChars: Boolean
) extends BaseMapping {
    /**
      * Returns the dependencies (i.e. names of tables in the Dataflow model)
      *
      * @return
      */
    override def inputs : Seq[MappingOutputIdentifier] = {
        Seq(input)
    }


    /**
      * Lists all outputs of this mapping. Every mapping should have one "main" output
      *
      * @return
      */
    override def outputs: Seq[String] = Seq("main", "error")

    /**
      * Executes this MappingType and returns a corresponding DataFrame
      *
      * @param executor
      * @param deps
      * @return
      */
    override def execute(executor: Executor, deps: Map[MappingOutputIdentifier, DataFrame]): Map[String,DataFrame] = {
        require(executor != null)
        require(deps != null)

        val spark = executor.spark
        val corruptedColumn = "_flowman_corrupted_column"
        val sparkSchema = Option(schema).map(schema => schema.sparkSchema.add(corruptedColumn, StringType)).orNull
        val table = deps(this.input)
        val result = spark.read
            .schema(sparkSchema)
            .option("mode", parseMode)
            .option("columnNameOfCorruptRecord", corruptedColumn)
            .option("allowComments", allowComments)
            .option("allowUnquotedFieldNames", allowUnquotedFieldNames)
            .option("allowSingleQuotes", allowSingleQuotes)
            .option("allowNumericLeadingZeros", allowNumericLeadingZeros)
            .option("allowNonNumericNumbers", allowNonNumericNumbers)
            .option("allowBackslashEscapingAnyCharacter", allowBackslashEscapingAnyCharacter)
            .option("allowUnquotedControlChars", allowUnquotedControlChars)
            .json(table.select(table(column).cast(StringType)).as[String](Encoders.STRING))

        // If no schema is specified, Spark will only add the error column if an error actually occurred
        val (mainResult, errorResult) =
            if (result.columns.contains(corruptedColumn)) {
                (result
                    .filter(result(corruptedColumn).isNull)
                    .drop(corruptedColumn),
                result
                    .filter(result(corruptedColumn).isNotNull)
                    .select(coalesce(result(corruptedColumn), lit("")).alias("record"))
                )
            }
            else {
                (result, spark.emptyDataFrame.withColumn("record", lit("")))
            }

        Map(
            "main" -> mainResult,
            "error" -> errorResult
        )
    }

    /**
      * Returns the schema as produced by this mapping, relative to the given input schema
      * @param input
      * @return
      */
    override def describe(executor:Executor, input:Map[MappingOutputIdentifier,ftypes.StructType]) : Map[String,ftypes.StructType] = {
        require(executor != null)
        require(input != null)

        val mainSchema = ftypes.StructType(if (schema != null) schema.fields else Seq())
        val errorSchema = ftypes.StructType(Seq(Field("record", ftypes.StringType, false)))
        Map(
            "main" -> mainSchema,
            "error" -> errorSchema
        )
    }
}



class ExtractJsonMappingSpec extends MappingSpec {
    @JsonProperty(value = "input", required = true) private var input: String = _
    @JsonProperty(value = "column", required = true) private var column: String = _
    @JsonProperty(value = "schema", required = false) private var schema: SchemaSpec = _
    @JsonProperty(value = "parseMode", required = false) private var parseMode: String = "PERMISSIVE"
    @JsonProperty(value = "allowComments", required = false) private var allowComments: String = "false"
    @JsonProperty(value = "allowUnquotedFieldNames", required = false) private var allowUnquotedFieldNames: String = "false"
    @JsonProperty(value = "allowSingleQuotes", required = false) private var allowSingleQuotes: String = "true"
    @JsonProperty(value = "allowNumericLeadingZeros", required = false) private var allowNumericLeadingZeros: String = "false"
    @JsonProperty(value = "allowNonNumericNumbers", required = false) private var allowNonNumericNumbers: String = "true"
    @JsonProperty(value = "allowBackslashEscapingAnyCharacter", required = false) private var allowBackslashEscapingAnyCharacter: String = "false"
    @JsonProperty(value = "allowUnquotedControlChars", required = false) private var allowUnquotedControlChars: String = "false"

    /**
      * Creates the instance of the specified Mapping with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context): ExtractJsonMapping = {
        ExtractJsonMapping(
            instanceProperties(context),
            MappingOutputIdentifier(context.evaluate(input)),
            context.evaluate(column),
            if (schema != null) schema.instantiate(context) else null,
            context.evaluate(parseMode),
            context.evaluate(allowComments).toBoolean,
            context.evaluate(allowUnquotedFieldNames).toBoolean,
            context.evaluate(allowSingleQuotes).toBoolean,
            context.evaluate(allowNumericLeadingZeros).toBoolean,
            context.evaluate(allowNonNumericNumbers).toBoolean,
            context.evaluate(allowBackslashEscapingAnyCharacter).toBoolean,
            context.evaluate(allowUnquotedControlChars).toBoolean
        )
    }
}
