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

import java.util.Locale

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.{types => stypes}
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.types.ArrayType
import com.dimajix.flowman.types.DecimalType
import com.dimajix.flowman.types.FieldType
import com.dimajix.flowman.types.MapType
import com.dimajix.flowman.types.StructType
import com.dimajix.flowman.types.VarcharType


object SanitizeMapping {
    private val typeAliases = Map(
        "text" -> "string",
        "long" -> "bigint",
        "short" -> "tinyint"
    )
    def apply(input:String, types:Map[String,String]) : SanitizeMapping = {
        val result = new SanitizeMapping
        result._input = input
        result._types = types
        result
    }
}


class SanitizeMapping extends BaseMapping {
    import SanitizeMapping.typeAliases
    private val logger = LoggerFactory.getLogger(classOf[ProjectMapping])

    @JsonProperty(value = "input", required = true) private[spec] var _input:String = _
    @JsonProperty(value = "types") private[spec] var _types:Map[String,String] = Map()

    def input(implicit context: Context) : MappingIdentifier = MappingIdentifier.parse(context.evaluate(_input))
    def types(implicit context: Context) : Map[String,FieldType] = _types.map(kv =>
        typeAliases.getOrElse(kv._1.toLowerCase(Locale.ROOT), kv._1) -> FieldType.of(context.evaluate(kv._2))
    )

    /**
      * Executes this MappingType and returns a corresponding DataFrame
      *
      * @param executor
      * @param input
      * @return
      */
    override def execute(executor: Executor, input: Map[MappingIdentifier, DataFrame]): DataFrame = {
        require(executor != null)
        require(input != null)

        implicit val icontext = executor.context
        val types = this.types.map(kv => kv._1.toLowerCase(Locale.ROOT) -> kv._2)
        val mappingId = this.input
        val df = input(mappingId)
        val schema = df.schema


        def fqName(prefix:String, name:String) : String = {
            if (prefix.isEmpty)
                name
            else
                prefix + "." + name
        }
        def processMap(fqn:String, mt:stypes.MapType) : Option[Column] = {
            // Ensure that no entry in the map requires a remapping
            if (processField(fqn, stypes.StructField("key", mt.keyType)).
                orElse(processField(fqn, stypes.StructField("value", mt.valueType))).nonEmpty)
                throw new UnsupportedOperationException("Transforming map types is not supported")
            None
        }
        def processArray(fqn:String, at:stypes.ArrayType) : Option[Column] = {
            // Ensure that no entry in the array requires a remapping
            if (processField(fqn, stypes.StructField(fqn, at.elementType)).nonEmpty)
                throw new UnsupportedOperationException("Transforming array types is not supported")
            None
        }
        def processStruct(fqn:String, st:stypes.StructType, nullable:Boolean) : Option[Column] = {
            // Map each field to an unprocessed field and to a cast field
            val fields = st.fields.map(f => (col(fqName(fqn, f.name)).as(f.name), processField(fqn, f)))

            // If nothing changed, return None
            if (fields.forall(_._2.isEmpty)) {
                None
            }
            else {
                val columns = fields.map(f => f._2.getOrElse(f._1))
                if (nullable) {
                    Some(when(columns.map(_.isNotNull).reduce(_ || _), struct(columns:_*)))
                }
                else {
                    Some(struct(columns: _*))
                }
            }
        }
        def processField(prefix:String, field:stypes.StructField) : Option[Column] = {
            val fqn = fqName(prefix, field.name)
            val column = field.dataType match {
                case _:stypes.VarcharType =>
                    types.get("string").map(d => col(fqn).cast(d.sparkType))
                case _:stypes.DecimalType =>
                    types.get("decimal").map(d => col(fqn).cast(d.sparkType))
                case at:stypes.ArrayType => processArray(fqn, at)
                case mt:stypes.MapType => processMap(fqn, mt)
                case st:stypes.StructType => processStruct(fqn, st, field.nullable)
                case dt:stypes.DataType =>
                    types.get(dt.sql.toLowerCase(Locale.ROOT)).map(t => col(fqn).cast(t.sparkType))
            }
            column.map(_.as(field.name))
        }

        val fields = schema.fields.map(f => (col(f.name), processField("", f)))
        val columns = fields.map(p => p._2.getOrElse(p._1))
        df.select(columns:_*)
    }

    /**
      * Returns the dependencies (i.e. names of tables in the Dataflow model)
      *
      * @param context
      * @return
      */
    override def dependencies(implicit context: Context): Array[MappingIdentifier] = Array(input)

    /**
      * Returns the schema as produced by this mapping, relative to the given input schema
      * @param context
      * @param input
      * @return
      */
    override def describe(context:Context, input:Map[MappingIdentifier,StructType]) : StructType = {
        require(context != null)
        require(input != null)

        implicit val icontext = context
        val types = this.types.map(kv => kv._1.toLowerCase(Locale.ROOT) -> kv._2)
        val mappingId = this.input
        val schema = input(mappingId)

        def processType(fieldType: FieldType) : FieldType = {
            fieldType match {
                case dt:DecimalType => types.getOrElse("decimal", dt)
                case dt:VarcharType => types.getOrElse("string", dt)
                case at:ArrayType => processArray(at)
                case mt:MapType => processMap(mt)
                case st:StructType => processStruct(st)
                case ft:FieldType => types.getOrElse(ft.sqlType.toLowerCase(Locale.ROOT), ft)
            }
        }
        def processArray(at:ArrayType) : ArrayType = {
            val elemType = processType(at.elementType)
            if (elemType != at.elementType)
                at.copy(elementType=elemType)
            else
                at
        }
        def processMap(mt:MapType) : MapType = {
            val keyType = processType(mt.keyType)
            val valueType = processType(mt.valueType)
            if (keyType != mt.keyType || valueType != mt.valueType)
                mt.copy(keyType=keyType, valueType=valueType)
            else
                mt
        }
        def processStruct(st:StructType) : StructType = {
            val fields = st.fields.map { field =>
                val ftype = processType(field.ftype)

                // Only returns a new field if type really has changed
                if (ftype ne field.ftype)
                    field.copy(ftype = ftype)
                else
                    field
            }

            // Only return a new struct if something has changed
            if (st.fields.zip(fields).exists { case (left,right) => left != right} )
                StructType(fields)
            else
                st
        }

        processStruct(schema)
    }
}
