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

import java.util.Locale

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.NullType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.util.SchemaUtils


case class UnionMapping(
    instanceProperties:Mapping.Properties,
    inputs:Seq[MappingIdentifier],
    columns:Map[String,String],
    distinct:Boolean
) extends BaseMapping {
    private val logger = LoggerFactory.getLogger(classOf[UnionMapping])

    /**
      * Executes this MappingType and returns a corresponding DataFrame
      *
      * @param executor
      * @param tables
      * @return
      */
    override def execute(executor:Executor, tables:Map[MappingIdentifier,DataFrame]) : DataFrame = {
        val dfs = inputs.map(tables(_))

        // Create a common schema from collected columns
        val schema = if (columns != null) SchemaUtils.createSchema(columns.toSeq) else getCommonSchema(dfs)
        logger.info(s"Creating union from mappings ${inputs.mkString(",")} using columns ${schema.fields.map(_.name).mkString(",")}}")

        // Project all tables onto common schema
        val projectedTables = dfs.map(table =>
            projectTable(table, schema)
        )

        // Now create a union of all tables
        val union = projectedTables.reduce((l,r) => l.union(r))

        // Optionally perform distinct operation
        if (distinct)
            union.distinct()
        else
            union
    }

    /**
      * Creates the list of required dependencies
      *
      * @return
      */
    override def dependencies : Array[MappingIdentifier] = {
        inputs.toArray
    }

    private def getCommonSchema(tables:Seq[DataFrame]) = {
        def commonField(newField:StructField, fields:Map[String,StructField]) = {
            val existingField = fields.getOrElse(newField.name.toLowerCase(Locale.ROOT), newField)
            val nullable = existingField.nullable || newField.nullable
            val dataType = if (existingField.dataType == NullType) newField.dataType else existingField.dataType
            StructField(newField.name, dataType, nullable)
        }
        val allColumns = tables.foldLeft(Map[String,StructField]())((columns, table) => {
            val tableColumns = table
                .schema
                .map(field => field.name.toLowerCase(Locale.ROOT) -> commonField(field, columns)).toMap
            columns ++ tableColumns
        })

        // Create a common schema from collected columns
        StructType(allColumns.values.toSeq.sortBy(_.name.toLowerCase(Locale.ROOT)))
    }

    private def projectTable(table:DataFrame, schema:StructType) = {
        val tableColumnNames = table.schema.map(_.name).toSet
        table.select(schema.fields.map(column =>
            if (tableColumnNames.contains(column.name))
                table(column.name).cast(column.dataType)
            else
                lit(null).cast(column.dataType).as(column.name)
        ):_*)
    }
}



class UnionMappingSpec extends MappingSpec {
    @JsonProperty(value="inputs", required=true) private[spec] var inputs:Seq[String] = Seq()
    @JsonProperty(value="columns", required=false) private[spec] var columns:Map[String,String] = _
    @JsonProperty(value="distinct", required=false) var distinct:String = "false"

    /**
      * Creates the instance of the specified Mapping with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context): UnionMapping = {
        UnionMapping(
            instanceProperties(context),
            inputs.map(i => MappingIdentifier.parse(context.evaluate(i))),
            if (columns != null) columns.mapValues(context.evaluate) else null,
            context.evaluate(distinct).toBoolean
        )
    }
}
