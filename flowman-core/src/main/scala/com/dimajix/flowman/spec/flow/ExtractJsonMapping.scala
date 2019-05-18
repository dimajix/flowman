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

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types.StringType

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spec.schema.Schema
import com.dimajix.flowman.{types => ftypes}


case class ExtractJsonMapping(
    instanceProperties:Mapping.Properties,
    input:MappingIdentifier,
    column: String,
    schema: Schema,
    parseMode: String,
    corruptedColumn: String,
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
    override def dependencies : Array[MappingIdentifier] = {
        Array(input)
    }

    /**
      * Executes this MappingType and returns a corresponding DataFrame
      *
      * @param executor
      * @param input
      * @return
      */
    override def execute(executor: Executor, input: Map[MappingIdentifier, DataFrame]): DataFrame = {
        val spark = executor.spark
        val sparkSchema = Option(schema).map(schema => schema.sparkSchema).orNull
        val table = input(this.input)
        spark.read
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
    }

    /**
      * Returns the schema as produced by this mapping, relative to the given input schema
      * @param context
      * @param input
      * @return
      */
    override def describe(input:Map[MappingIdentifier,ftypes.StructType]) : ftypes.StructType = {
        require(input != null)

        ftypes.StructType(schema.fields)
    }
}



class ExtractJsonMappingSpec extends MappingSpec {
    @JsonProperty(value = "input", required = true) private var input: String = _
    @JsonProperty(value = "column", required = true) private var column: String = _
    @JsonProperty(value = "schema", required = false) private var schema: Schema = _
    @JsonProperty(value = "parseMode", required = false) private var parseMode: String = "PERMISSIVE"
    @JsonProperty(value = "corruptedColumn", required = false) private var corruptedColumn: String = "_corrupt_record"
    @JsonProperty(value = "allowComments", required = false) private var allowComments: String = "false"
    @JsonProperty(value = "allowUnquotedFieldNames", required = false) private var allowUnquotedFieldNames: String = "false"
    @JsonProperty(value = "allowSingleQuotes", required = false) private var allowSingleQuotes: String = "true"
    @JsonProperty(value = "allowNumericLeadingZeros", required = false) private var allowNumericLeadingZeros: String = "false"
    @JsonProperty(value = "allowNonNumericNumbers", required = false) private var allowNonNumericNumbers: String = "true"
    @JsonProperty(value = "allowBackslashEscapingAnyCharacter", required = false) private var allowBackslashEscapingAnyCharacter: String = "false"
    @JsonProperty(value = "allowUnquotedControlChars", required = false) private var allowUnquotedControlChars: String = "false"

    override def instantiate(context: Context): ExtractJsonMapping = {
        ExtractJsonMapping(
            instanceProperties(context),
            MappingIdentifier(context.evaluate(this.input)),
            context.evaluate(this.column),
            this.schema,
            context.evaluate(this.parseMode),
            context.evaluate(this.corruptedColumn),
            context.evaluate(this.allowComments).toBoolean,
            context.evaluate(this.allowUnquotedFieldNames).toBoolean,
            context.evaluate(this.allowSingleQuotes).toBoolean,
            context.evaluate(this.allowNumericLeadingZeros).toBoolean,
            context.evaluate(this.allowNonNumericNumbers).toBoolean,
            context.evaluate(this.allowBackslashEscapingAnyCharacter).toBoolean,
            context.evaluate(this.allowUnquotedControlChars).toBoolean
        )
    }
}