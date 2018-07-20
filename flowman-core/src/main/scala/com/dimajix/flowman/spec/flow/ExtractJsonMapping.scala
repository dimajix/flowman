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

package com.dimajix.flowman.spec.flow

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types.StructType

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spec.schema.Schema


class ExtractJsonMapping extends BaseMapping {
    @JsonProperty(value="input", required=true) private var _input:String = _
    @JsonProperty(value="column", required=true) private var _column:String = _
    @JsonProperty(value="schema", required=false) private var _schema: Schema = _
    @JsonProperty(value="parseMode", required=false) private var _parseMode: String = "PERMISSIVE"
    @JsonProperty(value="corruptedColumn", required=false) private var _corruptedColumn: String = "_corrupt_record"
    @JsonProperty(value="allowComments", required=false) private var _allowComments: String = "false"
    @JsonProperty(value="allowUnquotedFieldNames", required=false) private var _allowUnquotedFieldNames: String = "false"
    @JsonProperty(value="allowSingleQuotes", required=false) private var _allowSingleQuotes: String = "true"
    @JsonProperty(value="allowNumericLeadingZeros", required=false) private var _allowNumericLeadingZeros: String = "false"
    @JsonProperty(value="allowNonNumericNumbers", required=false) private var _allowNonNumericNumbers: String = "true"
    @JsonProperty(value="allowBackslashEscapingAnyCharacter", required=false) private var _allowBackslashEscapingAnyCharacter: String = "false"
    @JsonProperty(value="allowUnquotedControlChars", required=false) private var _allowUnquotedControlChars: String = "false"

    def input(implicit context: Context) : MappingIdentifier = MappingIdentifier.parse(context.evaluate(_input))
    def column(implicit context: Context) : String = context.evaluate(_column)
    def schema(implicit context: Context) : Schema = _schema
    def parseMode(implicit context: Context) : String = context.evaluate(_parseMode)
    def corruptedColumn(implicit context: Context) : String = context.evaluate(_corruptedColumn)
    def allowComments(implicit context: Context) : Boolean = context.evaluate(_allowComments).toBoolean
    def allowUnquotedFieldNames(implicit context: Context) : Boolean = context.evaluate(_allowUnquotedFieldNames).toBoolean
    def allowSingleQuotes(implicit context: Context) : Boolean = context.evaluate(_allowSingleQuotes).toBoolean
    def allowNumericLeadingZeros(implicit context: Context) : Boolean = context.evaluate(_allowNumericLeadingZeros).toBoolean
    def allowNonNumericNumbers(implicit context: Context) : Boolean = context.evaluate(_allowNonNumericNumbers).toBoolean
    def allowBackslashEscapingAnyCharacter(implicit context: Context) : Boolean = context.evaluate(_allowBackslashEscapingAnyCharacter).toBoolean
    def allowUnquotedControlChars(implicit context: Context) : Boolean = context.evaluate(_allowUnquotedControlChars).toBoolean

    /**
      * Returns the dependencies (i.e. names of tables in the Dataflow model)
      *
      * @param context
      * @return
      */
    override def dependencies(implicit context: Context): Array[MappingIdentifier] = {
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
        implicit val context = executor.context
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
            .json(table.select(table(column)).as[String](Encoders.STRING))
    }
}
