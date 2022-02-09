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

package com.dimajix.flowman.spec.mapping

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types.StringType

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.model.BaseMapping
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.spec.schema.SchemaSpec
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.StructType


object UnpackJsonMapping {
    case class ColumnMapping(name:String, alias:String, schema:Schema)
}


case class UnpackJsonMapping(
    instanceProperties:Mapping.Properties,
    input:MappingOutputIdentifier,
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
    /**
      * Returns the dependencies (i.e. names of tables in the Dataflow model)
      *
      * @return
      */
    override def inputs : Seq[MappingOutputIdentifier] = {
        Seq(input)
    }

    /**
      * Executes this MappingType and returns a corresponding DataFrame
      *
      * @param execution
      * @param tables
      * @return
      */
    override def execute(execution: Execution, tables: Map[MappingOutputIdentifier, DataFrame]): Map[String,DataFrame] = {
        require(execution != null)
        require(tables != null)

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

        val result = columns.foldLeft(table) { (t, c) =>
            val sparkSchema = c.schema.sparkSchema
            t.withColumn(Option(c.alias).getOrElse(c.name), from_json(table(c.name)
                    .cast(StringType), sparkSchema, options))
        }

        Map("main" -> result)
    }

    /**
      * Returns the schema as produced by this mapping, relative to the given input schema
      *
      * @param input
      * @return
      */
    override def describe(execution:Execution, input: Map[MappingOutputIdentifier, StructType]): Map[String,StructType] = {
        require(execution != null)
        require(input != null)

        val schema = input(this.input)
        val fields = schema.fields ++ columns.map(c => Field(Option(c.alias).getOrElse(c.name), StructType(c.schema.fields)))
        val result = StructType(fields)

        // Apply documentation
        val schemas = Map("main" -> result)
        applyDocumentation(schemas)
    }
}



object UnpackJsonMappingSpec {
    class ColumnMapping {
        @JsonProperty(value="name", required=true) private var name:String = _
        @JsonProperty(value="alias", required=true) private var alias:String = _
        @JsonProperty(value="schema", required=true) private var schema: SchemaSpec = _

        def instantiate(context:Context) : UnpackJsonMapping.ColumnMapping = {
            UnpackJsonMapping.ColumnMapping(
                context.evaluate(name),
                context.evaluate(alias),
                schema.instantiate(context)
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
            MappingOutputIdentifier(context.evaluate(input)),
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
