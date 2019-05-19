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
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types.StringType
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spec.schema.Schema
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.StructType


object UnpackJsonMapping {
    case class ColumnMapping(name:String, alias:String, schema:Schema)
}


case class UnpackJsonMapping(
    instanceProperties:Mapping.Properties,
    input:MappingIdentifier,
    columns: Seq[UnpackJsonMapping.ColumnMapping],
    corruptedColumn: String,
    allowComments: Boolean,
    allowUnquotedFieldNames: Boolean,
    allowSingleQuotes: Boolean,
    allowNumericLeadingZeros: Boolean,
    allowNonNumericNumbers: Boolean,
    allowBackslashEscapingAnyCharacter: Boolean,
    allowUnquotedControlChars: Boolean
) extends BaseMapping {
    private val logger = LoggerFactory.getLogger(classOf[UnpackJsonMapping])

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
      * @param tables
      * @return
      */
    override def execute(executor: Executor, tables: Map[MappingIdentifier, DataFrame]): DataFrame = {
        logger.info(s"Unpacking JSON columns $columns from mapping '$input'")

        val table = tables(input)
        val options = Map(
            "columnNameOfCorruptRecord" -> corruptedColumn,
            "allowComments" -> allowComments.toString,
            "allowUnquotedFieldNames" -> allowUnquotedFieldNames.toString,
            "allowSingleQuotes" -> allowSingleQuotes.toString,
            "allowNumericLeadingZeros" -> allowNumericLeadingZeros.toString,
            "allowNonNumericNumbers" -> allowNonNumericNumbers.toString,
            "allowBackslashEscapingAnyCharacter" -> allowBackslashEscapingAnyCharacter.toString,
            "allowUnquotedControlChars" -> allowUnquotedControlChars.toString
        )

        columns.foldLeft(table) { (t, c) =>
            val sparkSchema = c.schema.sparkSchema
            t.withColumn(Option(c.alias).getOrElse(c.name), from_json(table(c.name)
                    .cast(StringType), sparkSchema, options))
        }
    }

    /**
      * Returns the schema as produced by this mapping, relative to the given input schema
      *
      * @param input
      * @return
      */
    override def describe(input: Map[MappingIdentifier, StructType]): StructType = {
        require(input != null)

        val schema = input(this.input)
        val fields = schema.fields ++ columns.map(c => Field(Option(c.alias).getOrElse(c.name), StructType(c.schema.fields)))
        StructType(fields)
    }
}



object UnpackJsonMappingSpec {
    class ColumnMapping {
        @JsonProperty(value="name", required=true) private var name:String = _
        @JsonProperty(value="alias", required=true) private var alias:String = _
        @JsonProperty(value="schema", required=true) private var schema: Schema = _

        def instantiate(context:Context) : UnpackJsonMapping.ColumnMapping = {
            UnpackJsonMapping.ColumnMapping(
                context.evaluate(name),
                context.evaluate(alias),
                schema
            )
        }
    }
}



class UnpackJsonMappingSpec extends MappingSpec {
    @JsonProperty(value = "input", required = true) private var input: String = _
    @JsonProperty(value = "columns", required = true) private var columns: Seq[UnpackJsonMappingSpec.ColumnMapping] = Seq()
    @JsonProperty(value = "corruptedColumn", required = false) private var corruptedColumn: String = "_corrupt_record"
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
    override def instantiate(context: Context): UnpackJsonMapping = {
        UnpackJsonMapping(
            instanceProperties(context),
            MappingIdentifier(context.evaluate(input)),
            columns.map(_.instantiate(context)),
            context.evaluate(corruptedColumn),
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
